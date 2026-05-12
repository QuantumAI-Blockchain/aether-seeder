//! `aether-seeder spawn N` — spawn N concurrent workers feeding the
//! Aether Mind Knowledge Fabric.
//!
//! Workers share a single `Arc<dyn KnowledgeSource>`. The source is
//! responsible for cross-worker uniqueness; see docs/DESIGN.md.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::future::join_all;
use seeder_agent::{run_worker, WorkerConfig};
use seeder_common::{KnowledgeSource, SeedNode, SeederResult, SephirotDomain};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "aether-seeder", version, about = "Distributed Aether Mind knowledge seeder")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Spawn N concurrent worker agents.
    Spawn {
        /// Number of workers to spawn (e.g. 250).
        #[arg(short = 'n', long, default_value_t = 1)]
        count: usize,

        /// Aether Mind base URL.
        #[arg(long, default_value = "http://127.0.0.1:5003")]
        base_url: String,

        /// Admin API key for X-Admin-Key header (bypasses per-wallet rate limit).
        #[arg(long, env = "ADMIN_API_KEY")]
        admin_key: String,

        /// Knowledge source identifier. Currently only `placeholder` is wired;
        /// implement `GrokipediaSource` etc. per docs/DESIGN.md.
        #[arg(long, default_value = "placeholder")]
        source: String,

        /// Per-batch size (nodes per HTTP request).
        #[arg(long, default_value_t = 50)]
        batch_size: usize,

        /// Max batches per worker (None = run forever).
        #[arg(long)]
        max_batches: Option<usize>,
    },

    /// Print expected request shape for /aether/knowledge/sync (debugging aid).
    DescribeProtocol,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();

    match cli.cmd {
        Cmd::DescribeProtocol => {
            describe_protocol();
            Ok(())
        }
        Cmd::Spawn { count, base_url, admin_key, source, batch_size, max_batches } => {
            let src = build_source(&source).context("build source")?;
            run_swarm(count, base_url, admin_key, src, batch_size, max_batches).await
        }
    }
}

fn describe_protocol() {
    println!(
        r#"POST {{base_url}}/aether/knowledge/sync
X-Admin-Key: {{admin_key}}
Content-Type: application/json

{{
  "nodes": [
    {{
      "text": "...",
      "domain": "Chochmah",
      "source": "seeder_agent_47:grokipedia:Quantum_computing",
      "confidence": 0.85
    }}
  ]
}}

NB: confirm the actual KnowledgeSyncRequest shape against
aether-core/bin/aether-mind/src/main.rs and adjust seeder-agent::SyncRequest
before going live."#
    );
}

fn build_source(name: &str) -> Result<Arc<dyn KnowledgeSource>> {
    match name {
        "placeholder" => Ok(Arc::new(PlaceholderSource::new(2000))),
        other => anyhow::bail!(
            "unknown source `{}` — implement and register it in main.rs (see docs/DESIGN.md)",
            other
        ),
    }
}

async fn run_swarm(
    count: usize,
    base_url: String,
    admin_key: String,
    source: Arc<dyn KnowledgeSource>,
    batch_size: usize,
    max_batches: Option<usize>,
) -> Result<()> {
    info!(count, source = source.name(), "spawning workers");
    let mut handles = Vec::with_capacity(count);
    for i in 0..count {
        let config = WorkerConfig {
            agent_id: i,
            base_url: base_url.clone(),
            admin_key: admin_key.clone(),
            batch_size,
            max_batches,
            request_timeout: Duration::from_secs(30),
            inter_batch_pause: Duration::from_millis(500),
        };
        let src = Arc::clone(&source);
        handles.push(tokio::spawn(async move { run_worker(config, src).await }));
    }
    let results = join_all(handles).await;
    let mut total_submitted = 0u64;
    let mut total_accepted = 0u64;
    let mut total_429 = 0u64;
    let mut total_errors = 0u64;
    for r in results {
        match r {
            Ok(Ok(stats)) => {
                total_submitted += stats.nodes_submitted;
                total_accepted += stats.nodes_accepted;
                total_429 += stats.rate_limit_hits;
                total_errors += stats.transport_errors;
            }
            Ok(Err(e)) => tracing::error!(err = %e, "worker returned error"),
            Err(e) => tracing::error!(err = %e, "worker task panicked"),
        }
    }
    info!(total_submitted, total_accepted, total_429, total_errors, "swarm done");
    Ok(())
}

// ── PlaceholderSource — replace with GrokipediaSource etc. in a follow-up PR ──

struct PlaceholderSource {
    cap: usize,
    counter: tokio::sync::Mutex<usize>,
}

impl PlaceholderSource {
    fn new(cap: usize) -> Self {
        Self { cap, counter: tokio::sync::Mutex::new(0) }
    }
}

#[async_trait::async_trait]
impl KnowledgeSource for PlaceholderSource {
    fn name(&self) -> &'static str { "placeholder" }

    async fn fetch_batch(&self, n: usize) -> SeederResult<Vec<SeedNode>> {
        let mut c = self.counter.lock().await;
        let mut out = Vec::with_capacity(n);
        let domains = [
            SephirotDomain::Keter, SephirotDomain::Chochmah, SephirotDomain::Binah,
            SephirotDomain::Chesed, SephirotDomain::Gevurah, SephirotDomain::Tiferet,
            SephirotDomain::Netzach, SephirotDomain::Hod, SephirotDomain::Yesod,
            SephirotDomain::Malkuth,
        ];
        while out.len() < n && *c < self.cap {
            out.push(SeedNode {
                text: format!(
                    "[placeholder #{i}] real seeder must replace this with curated knowledge from a content source (Grokipedia, ArXiv, Wikipedia dump).",
                    i = *c
                ),
                domain: domains[*c % domains.len()],
                source: format!("placeholder:#{}", *c),
                confidence: 0.5,
            });
            *c += 1;
        }
        Ok(out)
    }
}
