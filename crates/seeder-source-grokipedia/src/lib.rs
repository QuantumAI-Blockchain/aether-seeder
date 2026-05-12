//! `GrokipediaSource` — a [`KnowledgeSource`] implementation that pulls
//! articles from `grokipedia.com`, strips boilerplate HTML, chunks the body
//! at sentence boundaries, dedups by content hash, and emits `SeedNode`s
//! tagged with the appropriate Sephirot cognitive domain.
//!
//! ## Concurrency
//!
//! A single `Arc<GrokipediaSource>` is shared across all workers. Internally
//! it holds:
//!
//! - `worklist`: a `Mutex<VecDeque<(slug, domain)>>` from which workers
//!   pop topics one at a time. This guarantees no two workers ever fetch
//!   the same article.
//! - `pending_chunks`: a `Mutex<VecDeque<SeedNode>>` of chunks left over
//!   from a previous fetch (e.g. a 50KB article yields ~60 chunks that
//!   don't all fit into one batch).
//! - `dedup`: a `Mutex<DedupSet>` of content hashes already emitted,
//!   preventing duplicate vectors from crossing the network even if two
//!   different articles share a passage.
//!
//! The HTTP fetch itself is not held under any lock, so workers can fetch
//! in parallel as long as they hold distinct slugs.
//!
//! ## What "ingest" means in this codebase
//!
//! The seeded `SeedNode`s carry text + Sephirot domain + source attribution
//! + confidence. They do **not** carry the 896-dimensional embedding the
//! Aether Mind ultimately stores. The intended pipeline is:
//!
//! 1. This source emits text-only `SeedNode`s.
//! 2. The worker (in `seeder-agent`) calls an embedding endpoint
//!    (`POST /aether/embed`, not yet implemented in aether-mind) to convert
//!    text → `Vec<f32>` of length 896.
//! 3. The worker POSTs the resulting `EmbeddingSubmission`s through
//!    `POST /aether/gradients` (which accepts an `embeddings: Vec<…>` field
//!    via `#[serde(default)]`).
//!
//! Until step 2 is wired up in qubitcoin-aether, this source is useful for
//! the dry-run / wire-shape smoke tests but cannot itself grow the
//! Knowledge Fabric. That gap is tracked in `docs/DESIGN.md`.

pub mod topics;

use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use regex::Regex;
use seeder_common::{
    shared_dedup, KnowledgeSource, SeedNode, SeederError, SeederResult, SephirotDomain,
    SharedDedup,
};
use tokio::sync::Mutex;
use tracing::{debug, warn};

const DEFAULT_BASE_URL: &str = "https://grokipedia.com";
const DEFAULT_USER_AGENT: &str = "aether-seeder/0.1 (+https://qbc.network)";
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Article chunk size — matches the Python reference (800 chars per chunk).
const CHUNK_MAX_CHARS: usize = 800;
/// Drop chunks shorter than this — short chunks are usually navigation
/// scraps that escaped the HTML stripping.
const CHUNK_MIN_CHARS: usize = 40;
/// Cap article body length to keep memory bounded on pathological pages.
const ARTICLE_MAX_CHARS: usize = 50_000;
/// Confidence assigned to seeded knowledge — calibrated rather than 1.0,
/// because curated articles are quality but not ground truth.
const SEED_CONFIDENCE: f32 = 0.90;

pub struct GrokipediaSource {
    client: reqwest::Client,
    base_url: String,
    worklist: Mutex<VecDeque<(&'static str, SephirotDomain)>>,
    pending_chunks: Mutex<VecDeque<SeedNode>>,
    dedup: SharedDedup,
}

impl GrokipediaSource {
    /// New source seeded with the built-in curated topic list.
    pub fn new() -> SeederResult<Self> {
        Self::with_topics(topics::SEED_TOPICS.iter().copied())
    }

    /// New source with a custom topic list. Useful for tests and for
    /// partitioning a corpus across multiple source instances.
    pub fn with_topics<I>(topics: I) -> SeederResult<Self>
    where
        I: IntoIterator<Item = (&'static str, SephirotDomain)>,
    {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_FETCH_TIMEOUT)
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .map_err(|e| SeederError::Transport(e.to_string()))?;
        Ok(Self {
            client,
            base_url: DEFAULT_BASE_URL.to_string(),
            worklist: Mutex::new(topics.into_iter().collect()),
            pending_chunks: Mutex::new(VecDeque::new()),
            dedup: shared_dedup(),
        })
    }

    /// Override the HTTP base URL — used by the unit test against a local
    /// mock server.
    pub fn set_base_url(&mut self, base_url: impl Into<String>) {
        self.base_url = base_url.into();
    }

    /// Reset the dedup state. Mostly useful in tests; in production the
    /// dedup set should accumulate across the lifetime of the source.
    pub async fn reset_dedup(&self) {
        let mut d = self.dedup.lock().await;
        *d = Default::default();
    }

    async fn fetch_article(
        &self,
        slug: &str,
    ) -> SeederResult<Option<String>> {
        let url = format!("{}/page/{}", self.base_url.trim_end_matches('/'), slug);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| SeederError::Transport(format!("fetch {slug}: {e}")))?;

        if !resp.status().is_success() {
            warn!(slug, status = %resp.status(), "non-success");
            return Ok(None);
        }
        let html = resp
            .text()
            .await
            .map_err(|e| SeederError::Transport(format!("read body {slug}: {e}")))?;
        Ok(Some(extract_clean_text(&html, slug)))
    }

    async fn refill_from_next_topic(&self) -> SeederResult<()> {
        // Drain pending chunks before fetching new articles.
        {
            let pending = self.pending_chunks.lock().await;
            if !pending.is_empty() {
                return Ok(());
            }
        }

        let next = {
            let mut wl = self.worklist.lock().await;
            wl.pop_front()
        };
        let Some((slug, domain)) = next else {
            return Ok(());
        };

        let body = match self.fetch_article(slug).await? {
            Some(b) if b.len() >= 100 => b,
            _ => {
                debug!(slug, "article empty or too short");
                return Ok(());
            }
        };

        // Cap body length defensively. Slice on a UTF-8 char boundary —
        // naive byte slicing panics when the cut falls inside a multi-byte
        // codepoint (e.g. an em-dash at byte 49999..50002).
        let body = safe_truncate(&body, ARTICLE_MAX_CHARS);

        let mut new_chunks = Vec::new();
        let mut dedup = self.dedup.lock().await;
        for chunk in chunk_text(body) {
            let node = SeedNode {
                text: chunk,
                domain,
                source: format!("grokipedia:{slug}"),
                confidence: SEED_CONFIDENCE,
            };
            if dedup.insert_new(node.content_hash()) {
                new_chunks.push(node);
            }
        }
        drop(dedup);

        let mut pending = self.pending_chunks.lock().await;
        pending.extend(new_chunks);
        Ok(())
    }
}

#[async_trait]
impl KnowledgeSource for GrokipediaSource {
    fn name(&self) -> &'static str {
        "grokipedia"
    }

    async fn fetch_batch(&self, n: usize) -> SeederResult<Vec<SeedNode>> {
        let mut out = Vec::with_capacity(n);
        // Keep refilling from articles until we have n chunks, or the
        // worklist is empty and pending is empty.
        loop {
            // Drain pending into out.
            {
                let mut pending = self.pending_chunks.lock().await;
                while let Some(node) = pending.pop_front() {
                    out.push(node);
                    if out.len() >= n {
                        return Ok(out);
                    }
                }
            }
            // Need more chunks: fetch next article.
            let worklist_empty = {
                let wl = self.worklist.lock().await;
                wl.is_empty()
            };
            let pending_empty = {
                let p = self.pending_chunks.lock().await;
                p.is_empty()
            };
            if worklist_empty && pending_empty {
                return Ok(out);
            }
            self.refill_from_next_topic().await?;
        }
    }
}

/// Truncate `s` to at most `max_bytes` while preserving UTF-8 char
/// boundaries — a naive `&s[..n]` panics if `n` falls inside a
/// multi-byte codepoint, which Grokipedia articles routinely contain
/// (em-dashes, smart quotes, mathematical symbols).
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

// ── HTML cleaning + chunking ────────────────────────────────────────────

static RE_SCRIPT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<script[^>]*>.*?</script>").unwrap());
static RE_STYLE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<style[^>]*>.*?</style>").unwrap());
static RE_NAV: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?is)<nav[^>]*>.*?</nav>").unwrap());
static RE_HEADER: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<header[^>]*>.*?</header>").unwrap());
static RE_FOOTER: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<footer[^>]*>.*?</footer>").unwrap());
static RE_ARTICLE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<article[^>]*>(.*?)</article>").unwrap());
static RE_TAG: Lazy<Regex> = Lazy::new(|| Regex::new(r"<[^>]+>").unwrap());
static RE_WHITESPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static RE_REF: Lazy<Regex> = Lazy::new(|| Regex::new(r"\[\d+\]").unwrap());
/// Split `text` into sentence-shaped substrings. A sentence ends at
/// `.`, `!`, or `?` followed by whitespace. (Rust's `regex` crate does
/// not support look-around, so we walk the bytes by hand.)
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
                // End of sentence; emit substring up to & including the terminator.
                let end = i + 1; // exclusive bound
                if start < end {
                    out.push(&text[start..end]);
                }
                // Skip the whitespace run.
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

fn extract_clean_text(html: &str, slug: &str) -> String {
    let cleaned: String = {
        let s = RE_SCRIPT.replace_all(html, "");
        let s = RE_STYLE.replace_all(&s, "");
        let s = RE_NAV.replace_all(&s, "");
        let s = RE_HEADER.replace_all(&s, "");
        let s = RE_FOOTER.replace_all(&s, "");
        s.into_owned()
    };

    // Prefer the <article> body if present.
    let body = if let Some(cap) = RE_ARTICLE.captures(&cleaned) {
        cap.get(1).map(|m| m.as_str().to_string()).unwrap_or(cleaned)
    } else {
        cleaned
    };

    let stripped = RE_TAG.replace_all(&body, " ");
    let collapsed = RE_WHITESPACE.replace_all(&stripped, " ");
    let no_refs = RE_REF.replace_all(&collapsed, "");
    let trimmed = no_refs.trim();

    // Prepend a human-readable headline for context.
    let headline = slug.replace('_', " ");
    format!("{headline}\n\n{trimmed}")
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

    #[test]
    fn extract_clean_text_strips_html_and_boilerplate() {
        let html = r#"
            <html><head><title>x</title>
            <style>body{color:red}</style>
            <script>alert(1)</script>
            </head><body>
            <nav>Home Search</nav>
            <header>Banner</header>
            <article>
              <h1>Quantum Computing</h1>
              <p>Quantum computers exploit superposition.</p>
              <p>They use entanglement [1] as a resource.</p>
            </article>
            <footer>(C) 2026</footer>
            </body></html>
        "#;
        let out = extract_clean_text(html, "Quantum_computing");
        assert!(out.starts_with("Quantum computing"), "missing headline: {}", out);
        assert!(!out.contains("alert(1)"), "script body leaked");
        assert!(!out.contains("Home Search"), "nav leaked");
        assert!(!out.contains("Banner"), "header leaked");
        assert!(!out.contains("(C) 2026"), "footer leaked");
        assert!(!out.contains("[1]"), "reference number leaked");
        assert!(out.contains("Quantum computers exploit superposition."));
    }

    #[test]
    fn chunk_text_respects_max_chars_and_splits_on_sentence() {
        let long = "Sentence one. Sentence two! Sentence three? "
            .repeat(50); // ~2200 chars worth of sentences
        let chunks = chunk_text(&long);
        assert!(chunks.len() > 1, "expected multi-chunk output");
        for c in &chunks {
            assert!(c.len() <= CHUNK_MAX_CHARS + 50, "chunk too big: {} chars", c.len());
            assert!(c.len() >= CHUNK_MIN_CHARS, "chunk too small: {} chars", c.len());
        }
    }

    #[test]
    fn chunk_text_drops_tiny_fragments() {
        let chunks = chunk_text("Hi.");
        assert!(chunks.is_empty(), "expected empty result for sub-min content");
    }

    #[tokio::test]
    async fn fetch_batch_returns_empty_when_worklist_exhausted() {
        let src = GrokipediaSource::with_topics(std::iter::empty()).unwrap();
        let batch = src.fetch_batch(10).await.unwrap();
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn deduplication_prevents_double_emission() {
        // Two slugs with the same content → dedup yields one set of chunks.
        // Use the source directly with a hand-rolled mock client would be
        // heavier than necessary; here we verify the dedup helper integrates
        // by directly calling refill twice with the same article via pending.
        let src = GrokipediaSource::with_topics(std::iter::empty()).unwrap();
        let chunk = "This is a test passage repeated. ".repeat(20);
        let chunks = chunk_text(&chunk);
        assert!(!chunks.is_empty());

        // Manually populate pending twice — second time should not duplicate.
        {
            let mut pending = src.pending_chunks.lock().await;
            let mut dedup = src.dedup.lock().await;
            for c in &chunks {
                let node = SeedNode {
                    text: c.clone(),
                    domain: SephirotDomain::Chochmah,
                    source: "test:passage".to_string(),
                    confidence: 0.9,
                };
                if dedup.insert_new(node.content_hash()) {
                    pending.push_back(node);
                }
            }
        }
        let first = src.fetch_batch(100).await.unwrap();

        // Try to add same content again.
        {
            let mut pending = src.pending_chunks.lock().await;
            let mut dedup = src.dedup.lock().await;
            for c in &chunks {
                let node = SeedNode {
                    text: c.clone(),
                    domain: SephirotDomain::Chochmah,
                    source: "test:passage".to_string(),
                    confidence: 0.9,
                };
                if dedup.insert_new(node.content_hash()) {
                    pending.push_back(node);
                }
            }
        }
        let second = src.fetch_batch(100).await.unwrap();

        assert!(!first.is_empty());
        assert!(second.is_empty(), "dedup failed: second pass should be empty");
    }
}
