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

        /// Knowledge source identifier. Built-in:
        /// `placeholder` (stub text for smoke tests),
        /// `grokipedia` (curated articles from grokipedia.com, ~100 topics),
        /// `wikipedia` (MediaWiki extract API, ~200 topics across all 10
        /// Sephirot domains),
        /// `arxiv` (paper abstracts from the arXiv Atom API, ~65 papers
        /// across foundational ML / quantum / RL / alignment).
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
            let src = build_source(&source).await.context("build source")?;
            run_swarm(count, base_url, admin_key, src, batch_size, max_batches).await
        }
    }
}

fn describe_protocol() {
    println!(
        r#"Aether Mind ingestion path (verified 2026-05-12 against
aether-core/bin/aether-mind/src/main.rs):

  POST {{base_url}}/aether/gradients
  X-Admin-Key: {{admin_key}}
  Content-Type: application/json

  {{
    "indices": [],
    "values": [],
    "total_params": 0,
    "sparsity": 0.0,
    "full_norm": 0.0,
    "residual_norm": 0.0,
    "embeddings": [
      {{
        "embedding": [/* 896 floats */],
        "content": "...",
        "domain": 1,
        "confidence": 0.9
      }}
    ],
    "miner_id": "seeder_agent_47"
  }}

NB1: POST /aether/knowledge/sync is a peer-to-peer SHARD sync that takes a
     pre-serialized bincode blob — it is NOT the ingestion endpoint.
NB2: each `embedding` MUST be Vec<f32> of length 896 (config.embed_dim) or
     the handler silently skips it.
NB3: the seeder does not yet compute embeddings. aether-mind needs to
     expose its TextEmbedder via a new POST /aether/embed handler before
     this path is end-to-end. See docs/DESIGN.md."#
    );
}

async fn build_source(name: &str) -> Result<Arc<dyn KnowledgeSource>> {
    // `name` may be plain (e.g. "wikipedia") or parameterised
    // (e.g. "wikipedia-random:500" → 500 random titles).
    let (base, param) = match name.split_once(':') {
        Some((b, p)) => (b, Some(p)),
        None => (name, None),
    };
    match base {
        "placeholder" => Ok(Arc::new(PlaceholderSource::new(2000))),
        "grokipedia" => {
            let src = seeder_source_grokipedia::GrokipediaSource::new()
                .context("build GrokipediaSource")?;
            Ok(Arc::new(src))
        }
        "wikipedia" => {
            let src = seeder_source_wikipedia::WikipediaSource::new()
                .context("build WikipediaSource")?;
            Ok(Arc::new(src))
        }
        "wikipedia-random" => {
            // Default pool size: 500 random titles per rotation. Tunable
            // via `--source wikipedia-random:N`.
            let pool: usize = param
                .map(|p| p.parse().unwrap_or(500))
                .unwrap_or(500);
            let src = seeder_source_wikipedia::WikipediaSource::with_random_pool(pool)
                .await
                .context("build WikipediaSource random pool")?;
            Ok(Arc::new(src))
        }
        "arxiv" => {
            let src = seeder_source_arxiv::ArxivSource::new()
                .context("build ArxivSource")?;
            Ok(Arc::new(src))
        }
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
    let mut total_texts = 0u64;
    let mut total_embedded = 0u64;
    let mut total_ingested = 0u64;
    let mut total_429 = 0u64;
    let mut total_errors = 0u64;
    for r in results {
        match r {
            Ok(Ok(stats)) => {
                total_texts += stats.texts_submitted;
                total_embedded += stats.embeddings_received;
                total_ingested += stats.embeddings_ingested;
                total_429 += stats.rate_limit_hits;
                total_errors += stats.transport_errors;
            }
            Ok(Err(e)) => tracing::error!(err = %e, "worker returned error"),
            Err(e) => tracing::error!(err = %e, "worker task panicked"),
        }
    }
    info!(total_texts, total_embedded, total_ingested, total_429, total_errors, "swarm done");
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
