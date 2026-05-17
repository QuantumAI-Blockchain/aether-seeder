//! `StackExchangeSource` — pulls top-voted questions across a small set
//! of Stack Exchange sites (StackOverflow, Math, CS, Physics, CSTheory,
//! Data Science) via the public REST API at
//!   `https://api.stackexchange.com/2.3/questions`
//! with `filter=withbody` so question HTML bodies are included.
//!
//! HTML stripping is regex-based (the SE bodies are reasonably clean
//! prose with `<p>`, `<pre>`, `<code>`, `<a>` tags). The Stack Exchange
//! API enforces a daily 300-request unauthenticated quota per IP, so the
//! default fan-out (6 sites × 5 pages = 30 requests per construction)
//! is well inside it. A 200ms inter-request delay further protects the
//! quota and any per-second throttle.
//!
//! The SE API ALWAYS returns gzip-encoded responses regardless of the
//! Accept-Encoding header sent by the client; reqwest is built with the
//! `gzip` feature so the body is transparently decompressed.

use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use regex::Regex;
use seeder_common::{
    shared_dedup, KnowledgeSource, SeedNode, SeederError, SeederResult, SephirotDomain,
    SharedDedup,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

const DEFAULT_API_BASE_URL: &str = "https://api.stackexchange.com/2.3/questions";
const DEFAULT_USER_AGENT: &str =
    "aether-seeder/0.1 (+https://qbc.network; info@qbc.network)";
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(45);
const REQUEST_DELAY: Duration = Duration::from_millis(200);

const CHUNK_MAX_CHARS: usize = 800;
const CHUNK_MIN_CHARS: usize = 40;
const QUESTION_MAX_CHARS: usize = 12_000;
const SEED_CONFIDENCE: f32 = 0.88;

pub const DEFAULT_SITES: &[&str] = &[
    "stackoverflow",
    "math",
    "cs",
    "physics",
    "cstheory",
    "datascience",
];

pub const DEFAULT_PAGES_PER_SITE: usize = 5;

#[derive(Debug, Clone)]
struct QuestionEntry {
    question_id: u64,
    site: String,
    text: String,
    domain: SephirotDomain,
}

pub struct StackExchangeSource {
    worklist: Mutex<VecDeque<QuestionEntry>>,
    pending_chunks: Mutex<VecDeque<SeedNode>>,
    dedup: SharedDedup,
}

impl StackExchangeSource {
    pub async fn new_default() -> SeederResult<Self> {
        Self::with_sites(DEFAULT_SITES, DEFAULT_PAGES_PER_SITE).await
    }

    pub async fn with_sites(sites: &[&str], pages_per_site: usize) -> SeederResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_FETCH_TIMEOUT)
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .map_err(|e| SeederError::Transport(e.to_string()))?;

        let mut worklist: VecDeque<QuestionEntry> = VecDeque::new();
        let mut first = true;
        for site in sites {
            for page in 1..=pages_per_site {
                if !first {
                    tokio::time::sleep(REQUEST_DELAY).await;
                }
                first = false;
                match fetch_questions_page(&client, DEFAULT_API_BASE_URL, site, page).await {
                    Ok(entries) => {
                        debug!(site, page, count = entries.len(), "stackexchange page");
                        for e in entries {
                            worklist.push_back(e);
                        }
                    }
                    Err(e) => {
                        warn!(site, page, err = %e, "stackexchange page fetch failed");
                    }
                }
            }
        }
        info!(total_questions = worklist.len(), "stackexchange worklist seeded");

        Ok(Self {
            worklist: Mutex::new(worklist),
            pending_chunks: Mutex::new(VecDeque::new()),
            dedup: shared_dedup(),
        })
    }

    async fn refill_from_next_topic(&self) -> SeederResult<()> {
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
        let Some(entry) = next else {
            return Ok(());
        };

        let mut new_chunks = Vec::new();
        let mut dedup = self.dedup.lock().await;
        for chunk in chunk_text(&entry.text) {
            let node = SeedNode {
                text: chunk,
                domain: entry.domain,
                source: format!("stackexchange:{}:{}", entry.site, entry.question_id),
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
impl KnowledgeSource for StackExchangeSource {
    fn name(&self) -> &'static str {
        "stackexchange"
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
            let worklist_empty = self.worklist.lock().await.is_empty();
            let pending_empty = self.pending_chunks.lock().await.is_empty();
            if worklist_empty && pending_empty {
                return Ok(out);
            }
            self.refill_from_next_topic().await?;
        }
    }
}

// ── SE API envelope ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct SeResponse {
    items: Vec<SeQuestion>,
}

#[derive(Debug, Deserialize)]
struct SeQuestion {
    question_id: u64,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

async fn fetch_questions_page(
    client: &reqwest::Client,
    api_base: &str,
    site: &str,
    page: usize,
) -> SeederResult<Vec<QuestionEntry>> {
    let page_str = page.to_string();
    let resp = client
        .get(api_base)
        .query(&[
            ("site", site),
            ("order", "desc"),
            ("sort", "votes"),
            ("pagesize", "100"),
            ("page", page_str.as_str()),
            ("filter", "withbody"),
        ])
        .send()
        .await
        .map_err(|e| SeederError::Transport(format!("se {site} p{page}: {e}")))?;
    if !resp.status().is_success() {
        warn!(site, page, status = %resp.status(), "stackexchange non-success");
        return Ok(Vec::new());
    }
    let env: SeResponse = resp
        .json()
        .await
        .map_err(|e| SeederError::Transport(format!("se parse {site} p{page}: {e}")))?;

    let mut out = Vec::with_capacity(env.items.len());
    for q in env.items {
        let title = q.title.unwrap_or_default();
        let body_html = q.body.unwrap_or_default();
        let body_text = strip_html(&body_html);
        let tag_line = if q.tags.is_empty() {
            String::new()
        } else {
            format!("Tags: {}\n\n", q.tags.join(", "))
        };
        let combined = format!("{title}\n\n{tag_line}{body_text}");
        let combined = safe_truncate(&combined, QUESTION_MAX_CHARS).to_string();
        if combined.trim().len() < 80 {
            continue;
        }
        let key = format!("{}:{}", site, q.question_id);
        let domain = SephirotDomain::from_hash(&key);
        out.push(QuestionEntry {
            question_id: q.question_id,
            site: site.to_string(),
            text: combined,
            domain,
        });
    }
    Ok(out)
}

// ── Lightweight HTML → plaintext (regex). Good enough for SE bodies. ─

static RE_TAG: Lazy<Regex> = Lazy::new(|| Regex::new(r"<[^>]+>").unwrap());
static RE_WHITESPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());

fn strip_html(html: &str) -> String {
    let no_tags = RE_TAG.replace_all(html, " ");
    // Decode a minimal set of named entities.
    let decoded = no_tags
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");
    RE_WHITESPACE.replace_all(&decoded, " ").trim().to_string()
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

    fn empty_source() -> StackExchangeSource {
        StackExchangeSource {
            worklist: Mutex::new(VecDeque::new()),
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
    fn strip_html_handles_tags_and_entities() {
        let html = "<p>Hello &amp; <code>world</code>.</p>\n<pre>x &lt; y</pre>";
        let out = strip_html(html);
        assert_eq!(out, "Hello & world . x < y");
    }
}
