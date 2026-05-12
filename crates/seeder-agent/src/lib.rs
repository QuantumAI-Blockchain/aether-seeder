//! A single seeder worker.
//!
//! A worker runs an infinite (or N-batch) loop:
//! 1. `KnowledgeSource::fetch_batch(batch_size)` for unique seed nodes.
//! 2. POST the batch to `{base_url}/aether/knowledge/sync` with
//!    `X-Admin-Key: {admin_key}` to bypass per-wallet rate limits.
//! 3. On `429 Too Many Requests`, parse `Retry-After`, sleep, retry.
//! 4. On 5xx or transport errors, exponential backoff.
//! 5. Update local stats; emit `tracing::info` per N batches.
//!
//! Workers share an `Arc<dyn KnowledgeSource>` — the source is responsible
//! for cross-worker uniqueness (content-hash dedup, partitioning).

use std::sync::Arc;
use std::time::Duration;

use seeder_common::{KnowledgeSource, SeedNode, SeederError, SeederResult};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub agent_id: usize,
    pub base_url: String,
    pub admin_key: String,
    pub batch_size: usize,
    pub max_batches: Option<usize>,
    pub request_timeout: Duration,
    pub inter_batch_pause: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            agent_id: 0,
            base_url: "http://127.0.0.1:5003".to_string(),
            admin_key: String::new(),
            batch_size: 50,
            max_batches: None,
            request_timeout: Duration::from_secs(30),
            inter_batch_pause: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct WorkerStats {
    pub agent_id: usize,
    pub batches_attempted: u64,
    pub nodes_submitted: u64,
    pub nodes_accepted: u64,
    pub rate_limit_hits: u64,
    pub transport_errors: u64,
}

/// Response shape from `/aether/knowledge/sync`. Field names are best-effort
/// — adjust to match `KnowledgeSyncRequest`'s sibling response once the
/// actual handler shape is confirmed.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct SyncResponse {
    #[serde(default)]
    nodes_accepted: Option<u64>,
    #[serde(default)]
    total_knowledge_nodes: Option<u64>,
}

#[derive(Serialize)]
struct SyncRequest<'a> {
    nodes: &'a [SeedNode],
}

pub async fn run_worker(
    config: WorkerConfig,
    source: Arc<dyn KnowledgeSource>,
) -> SeederResult<WorkerStats> {
    let client = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .build()
        .map_err(|e| SeederError::Transport(e.to_string()))?;

    let url = format!("{}/aether/knowledge/sync", config.base_url.trim_end_matches('/'));
    let mut stats = WorkerStats { agent_id: config.agent_id, ..Default::default() };
    let mut backoff = Duration::from_millis(250);

    loop {
        if let Some(max) = config.max_batches {
            if stats.batches_attempted >= max as u64 {
                break;
            }
        }

        let batch = match source.fetch_batch(config.batch_size).await {
            Ok(b) if b.is_empty() => {
                info!(agent_id = config.agent_id, "source exhausted, stopping");
                break;
            }
            Ok(b) => b,
            Err(SeederError::Exhausted) => break,
            Err(e) => {
                warn!(agent_id = config.agent_id, err = %e, "fetch_batch failed; backing off");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };

        stats.batches_attempted += 1;
        stats.nodes_submitted += batch.len() as u64;

        let resp = client
            .post(&url)
            .header("X-Admin-Key", &config.admin_key)
            .json(&SyncRequest { nodes: &batch })
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                let body: SyncResponse = r.json().await.unwrap_or(SyncResponse {
                    nodes_accepted: None,
                    total_knowledge_nodes: None,
                });
                let accepted = body.nodes_accepted.unwrap_or(batch.len() as u64);
                stats.nodes_accepted += accepted;
                backoff = Duration::from_millis(250);
                debug!(agent_id = config.agent_id, batch = stats.batches_attempted, accepted, "ok");
            }
            Ok(r) if r.status().as_u16() == 429 => {
                stats.rate_limit_hits += 1;
                let retry_after = r
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(5);
                warn!(agent_id = config.agent_id, retry_after, "429 rate limited");
                tokio::time::sleep(Duration::from_secs(retry_after)).await;
            }
            Ok(r) => {
                stats.transport_errors += 1;
                warn!(agent_id = config.agent_id, status = %r.status(), "non-success");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
            Err(e) => {
                stats.transport_errors += 1;
                warn!(agent_id = config.agent_id, err = %e, "transport error");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }

        tokio::time::sleep(config.inter_batch_pause).await;
    }

    Ok(stats)
}
