//! `FineWebSource` — pulls curated web text rows from HuggingFace's
//! FineWeb-edu dataset via the `datasets-server` rows API:
//!   `https://datasets-server.huggingface.co/rows?dataset=HuggingFaceFW/fineweb-edu
//!    &config=sample-10BT&split=train&offset={off}&length=100`
//!
//! Each row has a `row.text` field of clean educational web text. We
//! pace requests at 1 req/sec.
//!
//! If `HF_TOKEN` is set (or `~/.cache/huggingface/token` exists) we send
//! an `Authorization: Bearer` header for a higher rate limit; the public
//! dataset is accessible unauthenticated too.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use seeder_common::{
    shared_dedup, KnowledgeSource, SeedNode, SeederError, SeederResult, SephirotDomain,
    SharedDedup,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

const DEFAULT_API_BASE_URL: &str = "https://datasets-server.huggingface.co/rows";
const DEFAULT_DATASET: &str = "HuggingFaceFW/fineweb-edu";
const DEFAULT_CONFIG: &str = "sample-10BT";
const DEFAULT_SPLIT: &str = "train";
const DEFAULT_USER_AGENT: &str =
    "aether-seeder/0.1 (+https://qbc.network; info@qbc.network)";
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const REQUEST_DELAY: Duration = Duration::from_secs(1);

const CHUNK_MAX_CHARS: usize = 800;
const CHUNK_MIN_CHARS: usize = 40;
const ROW_MAX_CHARS: usize = 8_000;
const SEED_CONFIDENCE: f32 = 0.85;

/// Approximate upper bound for the sample-10BT split (in rows). FineWeb-edu
/// is ~1.3T tokens / ~9B+ rows in the full set; the 10BT sample is several
/// million rows. Starting offsets are picked in [0, MAX_OFFSET).
const DEFAULT_MAX_OFFSET: u64 = 9_000_000_000;

/// Default page size per request (also the API max for /rows).
pub const DEFAULT_COUNT_PER_CALL: u64 = 100;
/// Default number of rows pulled per construction (50_000 rows ≈ 500
/// API calls @ 100 rows/call).
pub const DEFAULT_TOTAL_ROWS: u64 = 50_000;

pub struct FineWebSource {
    client: reqwest::Client,
    api_base_url: String,
    dataset: String,
    config: String,
    split: String,
    auth_header: Option<String>,
    pending_calls: Mutex<VecDeque<u64>>, // offsets queued for fetching
    pending_chunks: Mutex<VecDeque<SeedNode>>,
    dedup: SharedDedup,
}

impl FineWebSource {
    pub async fn new_default() -> SeederResult<Self> {
        let start = {
            let mut rng = rand::rng();
            rng.random_range(0..DEFAULT_MAX_OFFSET)
        };
        Self::with_offset_range(start, DEFAULT_COUNT_PER_CALL, DEFAULT_TOTAL_ROWS).await
    }

    /// Build a source that will issue `total_rows / count_per_call` API
    /// calls starting at offset `start`, advancing by `count_per_call`
    /// each call.
    pub async fn with_offset_range(
        start: u64,
        count_per_call: u64,
        total_rows: u64,
    ) -> SeederResult<Self> {
        if count_per_call == 0 {
            return Err(SeederError::Other(anyhow::anyhow!(
                "count_per_call must be > 0"
            )));
        }
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_FETCH_TIMEOUT)
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .map_err(|e| SeederError::Transport(e.to_string()))?;

        let auth_header = load_hf_token().map(|t| format!("Bearer {}", t.trim()));
        if auth_header.is_some() {
            debug!("huggingface-fineweb: HF token loaded, sending Authorization");
        } else {
            debug!("huggingface-fineweb: no HF token; relying on unauth quota");
        }

        let mut calls: VecDeque<u64> = VecDeque::new();
        let mut offset = start;
        let mut remaining = total_rows;
        while remaining > 0 {
            calls.push_back(offset);
            offset = offset.saturating_add(count_per_call);
            remaining = remaining.saturating_sub(count_per_call);
        }
        info!(
            start,
            count_per_call,
            total_rows,
            api_calls = calls.len(),
            "huggingface-fineweb worklist seeded"
        );

        Ok(Self {
            client,
            api_base_url: DEFAULT_API_BASE_URL.to_string(),
            dataset: DEFAULT_DATASET.to_string(),
            config: DEFAULT_CONFIG.to_string(),
            split: DEFAULT_SPLIT.to_string(),
            auth_header,
            pending_calls: Mutex::new(calls),
            pending_chunks: Mutex::new(VecDeque::new()),
            dedup: shared_dedup(),
        })
    }

    pub fn set_api_base_url(&mut self, base: impl Into<String>) {
        self.api_base_url = base.into();
    }

    async fn fetch_offset(&self, offset: u64) -> SeederResult<Vec<HfRow>> {
        let length = DEFAULT_COUNT_PER_CALL.to_string();
        let offset_str = offset.to_string();
        let mut req = self.client.get(&self.api_base_url).query(&[
            ("dataset", self.dataset.as_str()),
            ("config", self.config.as_str()),
            ("split", self.split.as_str()),
            ("offset", offset_str.as_str()),
            ("length", length.as_str()),
        ]);
        if let Some(h) = &self.auth_header {
            req = req.header("Authorization", h);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SeederError::Transport(format!("hf offset {offset}: {e}")))?;
        if !resp.status().is_success() {
            warn!(offset, status = %resp.status(), "huggingface-fineweb non-success");
            return Ok(Vec::new());
        }
        let env: HfRowsResponse = resp
            .json()
            .await
            .map_err(|e| SeederError::Transport(format!("hf parse offset {offset}: {e}")))?;
        Ok(env.rows)
    }

    async fn refill_from_next_topic(&self) -> SeederResult<()> {
        {
            let pending = self.pending_chunks.lock().await;
            if !pending.is_empty() {
                return Ok(());
            }
        }
        let next = {
            let mut wl = self.pending_calls.lock().await;
            wl.pop_front()
        };
        let Some(offset) = next else {
            return Ok(());
        };

        // Pace at 1 req/sec.
        tokio::time::sleep(REQUEST_DELAY).await;
        let rows = self.fetch_offset(offset).await?;

        let mut new_chunks = Vec::new();
        let mut dedup = self.dedup.lock().await;
        for row in rows {
            let row_idx = row.row_idx;
            let text = row.row.text.unwrap_or_default();
            if text.trim().len() < 80 {
                continue;
            }
            let key = row
                .row
                .url
                .clone()
                .unwrap_or_else(|| format!("fineweb:{row_idx}"));
            let domain = SephirotDomain::from_hash(&key);
            let text = safe_truncate(&text, ROW_MAX_CHARS).to_string();
            for chunk in chunk_text(&text) {
                let node = SeedNode {
                    text: chunk,
                    domain,
                    source: format!("huggingface-fineweb:{row_idx}"),
                    confidence: SEED_CONFIDENCE,
                };
                if dedup.insert_new(node.content_hash()) {
                    new_chunks.push(node);
                }
            }
        }
        drop(dedup);

        let mut pending = self.pending_chunks.lock().await;
        pending.extend(new_chunks);
        Ok(())
    }
}

#[async_trait]
impl KnowledgeSource for FineWebSource {
    fn name(&self) -> &'static str {
        "huggingface-fineweb"
    }

    async fn fetch_batch(&self, n: usize) -> SeederResult<Vec<SeedNode>> {
        let mut out = Vec::with_capacity(n);
        loop {
            {
                let mut pending = self.pending_chunks.lock().await;
                while let Some(node) = pending.pop_front() {
                    out.push(node);
                    if out.len() >= n {
                        return Ok(out);
                    }
                }
            }
            let calls_empty = self.pending_calls.lock().await.is_empty();
            let pending_empty = self.pending_chunks.lock().await.is_empty();
            if calls_empty && pending_empty {
                return Ok(out);
            }
            self.refill_from_next_topic().await?;
        }
    }
}

// ── HF datasets-server JSON envelope ─────────────────────────────────

#[derive(Debug, Deserialize)]
struct HfRowsResponse {
    #[serde(default)]
    rows: Vec<HfRow>,
}

#[derive(Debug, Deserialize)]
struct HfRow {
    #[serde(default)]
    row_idx: u64,
    row: HfRowContent,
}

#[derive(Debug, Deserialize)]
struct HfRowContent {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    url: Option<String>,
}

/// Load an HF token from the `HF_TOKEN` env var first, then from
/// `~/.cache/huggingface/token` if present.
fn load_hf_token() -> Option<String> {
    if let Ok(t) = std::env::var("HF_TOKEN") {
        if !t.trim().is_empty() {
            return Some(t);
        }
    }
    let path = std::env::var("HOME")
        .ok()
        .map(PathBuf::from)
        .map(|p| p.join(".cache/huggingface/token"));
    if let Some(p) = path {
        if let Ok(t) = std::fs::read_to_string(&p) {
            let trimmed = t.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn safe_truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut idx = max_bytes;
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    &s[..idx]
}

fn split_sentences(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if (b == b'.' || b == b'!' || b == b'?') && i + 1 < bytes.len() {
            let next = bytes[i + 1];
            if next == b' ' || next == b'\t' || next == b'\n' || next == b'\r' {
                let end = i + 1;
                if start < end {
                    out.push(&text[start..end]);
                }
                let mut j = end;
                while j < bytes.len() {
                    let c = bytes[j];
                    if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
                        j += 1;
                    } else {
                        break;
                    }
                }
                start = j;
                i = j;
                continue;
            }
        }
        i += 1;
    }
    if start < bytes.len() {
        out.push(&text[start..]);
    }
    out
}

fn chunk_text(text: &str) -> Vec<String> {
    let sentences = split_sentences(text);
    let mut out = Vec::new();
    let mut current = String::new();
    for sentence in sentences {
        if !current.is_empty() && current.len() + sentence.len() > CHUNK_MAX_CHARS {
            let s = current.trim().to_string();
            if s.len() >= CHUNK_MIN_CHARS {
                out.push(s);
            }
            current = sentence.to_string();
        } else if current.is_empty() {
            current = sentence.to_string();
        } else {
            current.push(' ');
            current.push_str(sentence);
        }
    }
    let tail = current.trim().to_string();
    if tail.len() >= CHUNK_MIN_CHARS {
        out.push(tail);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_source() -> FineWebSource {
        let client = reqwest::Client::builder()
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .unwrap();
        FineWebSource {
            client,
            api_base_url: DEFAULT_API_BASE_URL.to_string(),
            dataset: DEFAULT_DATASET.to_string(),
            config: DEFAULT_CONFIG.to_string(),
            split: DEFAULT_SPLIT.to_string(),
            auth_header: None,
            pending_calls: Mutex::new(VecDeque::new()),
            pending_chunks: Mutex::new(VecDeque::new()),
            dedup: shared_dedup(),
        }
    }

    #[tokio::test]
    async fn fetch_batch_empty_on_exhausted_worklist() {
        let src = empty_source();
        assert!(src.fetch_batch(0).await.unwrap().is_empty());
        assert!(src.fetch_batch(10).await.unwrap().is_empty());
    }

    #[test]
    fn chunk_text_respects_bounds() {
        let long = "Sentence one. Sentence two. Sentence three. ".repeat(40);
        let chunks = chunk_text(&long);
        assert!(chunks.len() > 1);
        for c in &chunks {
            assert!(c.len() <= CHUNK_MAX_CHARS + 50);
        }
    }
}
