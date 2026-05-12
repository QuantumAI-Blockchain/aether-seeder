//! A single seeder worker.
//!
//! Pipeline per batch (verified 2026-05-12 against
//! `aether-core/bin/aether-mind/src/main.rs`):
//!
//! 1. `KnowledgeSource::fetch_batch(batch_size)` for unique text chunks.
//! 2. `POST {base_url}/aether/embed` with `{ texts: [...] }` to get
//!    896-dim L2-normalized embeddings (handler at main.rs registers
//!    `/aether/embed` and returns `{ embed_dim, embeddings: [...] }`).
//! 3. `POST {base_url}/aether/gradients` with empty gradient fields and a
//!    populated `embeddings: Vec<EmbeddingSubmission>` (the
//!    `submit_gradients` handler ingests these into the right Sephirot
//!    shard iff `embedding.len() == embed_dim && domain < 10`).
//! 4. On `429 Too Many Requests`, parse `Retry-After`, sleep, retry.
//! 5. On 5xx or transport errors, exponential backoff.
//!
//! Workers share an `Arc<dyn KnowledgeSource>` — the source is responsible
//! for cross-worker uniqueness (content-hash dedup, partitioning).
//!
//! Auth: pass `X-Admin-Key: $ADMIN_API_KEY`. The aether-mind server does
//! not currently enforce auth on these endpoints, but sending the header
//! is forward-compatible and matches the pattern other tools (frontend,
//! api-gateway) use.

use std::sync::Arc;
use std::time::Duration;

use seeder_common::{KnowledgeSource, SeedNode, SeederError, SeederResult, SephirotDomain};
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
            request_timeout: Duration::from_secs(60),
            inter_batch_pause: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct WorkerStats {
    pub agent_id: usize,
    pub batches_attempted: u64,
    pub texts_submitted: u64,
    pub embeddings_received: u64,
    pub embeddings_ingested: u64,
    pub rate_limit_hits: u64,
    pub transport_errors: u64,
}

#[derive(Serialize)]
struct EmbedRequest<'a> {
    texts: Vec<&'a str>,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    embed_dim: usize,
    embeddings: Vec<Vec<f32>>,
}

/// Mirror of `aether-core/bin/aether-mind/src/main.rs::EmbeddingSubmission`.
#[derive(Serialize)]
struct EmbeddingSubmission {
    embedding: Vec<f32>,
    content: String,
    domain: u8,
    confidence: f32,
}

/// Mirror of `aether-core/bin/aether-mind/src/main.rs::GradientSubmission`.
/// When seeding (not actually training), the gradient fields are zeroed —
/// the handler runs FedAvg over a single empty entry (no-op) and ingests
/// the embeddings array.
#[derive(Serialize)]
struct GradientSubmission {
    indices: Vec<u32>,
    values: Vec<f32>,
    total_params: u64,
    sparsity: f32,
    full_norm: f32,
    residual_norm: f32,
    embeddings: Vec<EmbeddingSubmission>,
    miner_id: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GradientResponse {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    embeddings_ingested: Option<u64>,
    #[serde(default)]
    total_knowledge_vectors: Option<u64>,
}

fn sephirot_index(d: SephirotDomain) -> u8 {
    d as u8
}

pub async fn run_worker(
    config: WorkerConfig,
    source: Arc<dyn KnowledgeSource>,
) -> SeederResult<WorkerStats> {
    let client = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .build()
        .map_err(|e| SeederError::Transport(e.to_string()))?;

    let embed_url = format!("{}/aether/embed", config.base_url.trim_end_matches('/'));
    let ingest_url = format!("{}/aether/gradients", config.base_url.trim_end_matches('/'));
    let miner_id = format!("seeder_agent_{}", config.agent_id);

    let mut stats = WorkerStats { agent_id: config.agent_id, ..Default::default() };
    let mut backoff = Duration::from_millis(250);

    loop {
        if let Some(max) = config.max_batches {
            if stats.batches_attempted >= max as u64 {
                break;
            }
        }

        // ── Step 1: fetch text chunks from source ──
        let batch: Vec<SeedNode> = match source.fetch_batch(config.batch_size).await {
            Ok(b) if b.is_empty() => {
                info!(agent_id = config.agent_id, "source exhausted; stopping");
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
        stats.texts_submitted += batch.len() as u64;

        // ── Step 2: POST /aether/embed ──
        let embed_req = EmbedRequest {
            texts: batch.iter().map(|n| n.text.as_str()).collect(),
        };
        let embed_resp = match client
            .post(&embed_url)
            .header("X-Admin-Key", &config.admin_key)
            .json(&embed_req)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => match r.json::<EmbedResponse>().await {
                Ok(b) => b,
                Err(e) => {
                    stats.transport_errors += 1;
                    warn!(agent_id = config.agent_id, err = %e, "/aether/embed: bad JSON");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }
            },
            Ok(r) if r.status().as_u16() == 429 => {
                stats.rate_limit_hits += 1;
                let retry_after = parse_retry_after(&r).unwrap_or(5);
                warn!(agent_id = config.agent_id, retry_after, "429 on /aether/embed");
                tokio::time::sleep(Duration::from_secs(retry_after)).await;
                continue;
            }
            Ok(r) => {
                stats.transport_errors += 1;
                warn!(agent_id = config.agent_id, status = %r.status(), "non-success on /aether/embed");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
            Err(e) => {
                stats.transport_errors += 1;
                warn!(agent_id = config.agent_id, err = %e, "/aether/embed transport error");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };

        if embed_resp.embeddings.len() != batch.len() {
            stats.transport_errors += 1;
            warn!(
                agent_id = config.agent_id,
                expected = batch.len(),
                got = embed_resp.embeddings.len(),
                "/aether/embed returned wrong count"
            );
            continue;
        }
        stats.embeddings_received += embed_resp.embeddings.len() as u64;

        // ── Step 3: build EmbeddingSubmissions + POST /aether/gradients ──
        let mut submissions = Vec::with_capacity(batch.len());
        for (node, embedding) in batch.into_iter().zip(embed_resp.embeddings.into_iter()) {
            if embedding.len() != embed_resp.embed_dim {
                debug!(
                    agent_id = config.agent_id,
                    "skipping embedding with wrong dim ({} vs {})",
                    embedding.len(),
                    embed_resp.embed_dim
                );
                continue;
            }
            submissions.push(EmbeddingSubmission {
                embedding,
                content: node.text,
                domain: sephirot_index(node.domain),
                confidence: node.confidence,
            });
        }

        let grad_req = GradientSubmission {
            indices: vec![],
            values: vec![],
            total_params: 0,
            sparsity: 0.0,
            full_norm: 0.0,
            residual_norm: 0.0,
            embeddings: submissions,
            miner_id: miner_id.clone(),
        };

        match client
            .post(&ingest_url)
            .header("X-Admin-Key", &config.admin_key)
            .json(&grad_req)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                let body: GradientResponse = r.json().await.unwrap_or(GradientResponse {
                    status: None,
                    embeddings_ingested: None,
                    total_knowledge_vectors: None,
                });
                let ingested = body.embeddings_ingested.unwrap_or(0);
                stats.embeddings_ingested += ingested;
                backoff = Duration::from_millis(250);
                debug!(
                    agent_id = config.agent_id,
                    batch = stats.batches_attempted,
                    ingested,
                    total = body.total_knowledge_vectors.unwrap_or(0),
                    "ok"
                );
            }
            Ok(r) if r.status().as_u16() == 429 => {
                stats.rate_limit_hits += 1;
                let retry_after = parse_retry_after(&r).unwrap_or(5);
                warn!(agent_id = config.agent_id, retry_after, "429 on /aether/gradients");
                tokio::time::sleep(Duration::from_secs(retry_after)).await;
            }
            Ok(r) => {
                stats.transport_errors += 1;
                warn!(agent_id = config.agent_id, status = %r.status(), "non-success on /aether/gradients");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
            Err(e) => {
                stats.transport_errors += 1;
                warn!(agent_id = config.agent_id, err = %e, "/aether/gradients transport error");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }

        tokio::time::sleep(config.inter_batch_pause).await;
    }

    Ok(stats)
}

fn parse_retry_after(r: &reqwest::Response) -> Option<u64> {
    r.headers()
        .get("Retry-After")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
}
