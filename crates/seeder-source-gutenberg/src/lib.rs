//! `GutenbergSource` — pulls public-domain books from Project Gutenberg.
//!
//! Discovery via the gutendex.com listing API:
//!   `https://gutendex.com/books?languages=en&topic={topic}&sort=popular`
//! Plaintext via the canonical mirror:
//!   `https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt`
//!
//! Each book is capped at the first 200K chars to keep one book's chunk
//! count bounded; the PG header/footer (delimited by `*** START OF THE
//! PROJECT GUTENBERG` and `*** END OF THE PROJECT GUTENBERG`) is stripped
//! before chunking.

use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use seeder_common::{
    shared_dedup, KnowledgeSource, SeedNode, SeederError, SeederResult, SephirotDomain,
    SharedDedup,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

const DEFAULT_GUTENDEX_BASE: &str = "https://gutendex.com/books";
const DEFAULT_PG_TEXT_BASE: &str = "https://www.gutenberg.org/cache/epub";
const DEFAULT_USER_AGENT: &str =
    "aether-seeder/0.1 (+https://qbc.network; info@qbc.network)";
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(90);

const CHUNK_MAX_CHARS: usize = 800;
const CHUNK_MIN_CHARS: usize = 40;
const BOOK_MAX_CHARS: usize = 200_000;
const SEED_CONFIDENCE: f32 = 0.90;
const LISTING_REQUEST_DELAY: Duration = Duration::from_millis(750);

/// Default topic pool — broad coverage across humanities, hard science,
/// philosophy, narrative fiction and verse.
pub const DEFAULT_TOPICS: &[&str] = &[
    "science",
    "history",
    "philosophy",
    "fiction",
    "mathematics",
    "biography",
    "poetry",
];

pub const DEFAULT_BOOKS_PER_TOPIC: usize = 30;

#[derive(Debug, Clone)]
struct BookEntry {
    id: u64,
    title: String,
    domain: SephirotDomain,
    text_url: String,
}

pub struct GutenbergSource {
    client: reqwest::Client,
    pg_text_base: String,
    worklist: Mutex<VecDeque<BookEntry>>,
    pending_chunks: Mutex<VecDeque<SeedNode>>,
    dedup: SharedDedup,
}

impl GutenbergSource {
    pub async fn new_default() -> SeederResult<Self> {
        Self::with_topic_pool(DEFAULT_TOPICS, DEFAULT_BOOKS_PER_TOPIC).await
    }

    pub async fn with_topic_pool(topics: &[&str], books_per_topic: usize) -> SeederResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_FETCH_TIMEOUT)
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .map_err(|e| SeederError::Transport(e.to_string()))?;

        let mut worklist: VecDeque<BookEntry> = VecDeque::new();
        for (i, topic) in topics.iter().enumerate() {
            if i > 0 {
                tokio::time::sleep(LISTING_REQUEST_DELAY).await;
            }
            match fetch_books_for_topic(&client, DEFAULT_GUTENDEX_BASE, topic, books_per_topic)
                .await
            {
                Ok(entries) => {
                    debug!(topic, count = entries.len(), "gutenberg topic listing");
                    for e in entries {
                        worklist.push_back(e);
                    }
                }
                Err(e) => {
                    warn!(topic, err = %e, "gutenberg topic listing failed");
                }
            }
        }
        info!(total_books = worklist.len(), "gutenberg worklist seeded");

        Ok(Self {
            client,
            pg_text_base: DEFAULT_PG_TEXT_BASE.to_string(),
            worklist: Mutex::new(worklist),
            pending_chunks: Mutex::new(VecDeque::new()),
            dedup: shared_dedup(),
        })
    }

    pub fn set_pg_text_base(&mut self, base: impl Into<String>) {
        self.pg_text_base = base.into();
    }

    async fn fetch_book_text(&self, entry: &BookEntry) -> SeederResult<Option<String>> {
        // Prefer the API-provided text URL when present; fall back to the
        // canonical PG mirror path.
        let url = if !entry.text_url.is_empty() {
            entry.text_url.clone()
        } else {
            format!("{}/{}/pg{}.txt", self.pg_text_base, entry.id, entry.id)
        };
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| SeederError::Transport(format!("fetch {url}: {e}")))?;
        if !resp.status().is_success() {
            warn!(book_id = entry.id, status = %resp.status(), "gutenberg text non-success");
            return Ok(None);
        }
        let raw = resp
            .text()
            .await
            .map_err(|e| SeederError::Transport(format!("read body book {}: {e}", entry.id)))?;
        let cleaned = strip_pg_boilerplate(&raw);
        let trimmed = safe_truncate(cleaned, BOOK_MAX_CHARS).to_string();
        if trimmed.len() < 500 {
            debug!(book_id = entry.id, "gutenberg text too short after stripping");
            return Ok(None);
        }
        let normalized = trimmed
            .replace('\r', " ")
            .replace('\t', " ")
            .replace("\n\n", " \n");
        Ok(Some(format!("{}\n\n{}", entry.title, normalized)))
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

        let body = match self.fetch_book_text(&entry).await? {
            Some(b) => b,
            None => return Ok(()),
        };

        let mut new_chunks = Vec::new();
        let mut dedup = self.dedup.lock().await;
        for chunk in chunk_text(&body) {
            let node = SeedNode {
                text: chunk,
                domain: entry.domain,
                source: format!("gutenberg:{}", entry.id),
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
impl KnowledgeSource for GutenbergSource {
    fn name(&self) -> &'static str {
        "gutenberg"
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

// ── Gutendex JSON envelope ───────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct GutendexResponse {
    results: Vec<GutendexBook>,
}

#[derive(Debug, Deserialize)]
struct GutendexBook {
    id: u64,
    title: Option<String>,
    #[serde(default)]
    formats: std::collections::HashMap<String, String>,
}

async fn fetch_books_for_topic(
    client: &reqwest::Client,
    listing_base: &str,
    topic: &str,
    take_n: usize,
) -> SeederResult<Vec<BookEntry>> {
    let resp = client
        .get(listing_base)
        .query(&[
            ("languages", "en"),
            ("topic", topic),
            ("sort", "popular"),
        ])
        .send()
        .await
        .map_err(|e| SeederError::Transport(format!("gutendex {topic}: {e}")))?;
    if !resp.status().is_success() {
        warn!(topic, status = %resp.status(), "gutendex non-success");
        return Ok(Vec::new());
    }
    let env: GutendexResponse = resp
        .json()
        .await
        .map_err(|e| SeederError::Transport(format!("gutendex parse {topic}: {e}")))?;

    let mut out = Vec::with_capacity(take_n);
    for book in env.results.into_iter().take(take_n) {
        let text_url = pick_text_url(&book.formats).unwrap_or_default();
        let title = book.title.unwrap_or_else(|| format!("PG#{}", book.id));
        let key = format!("gutenberg:{}", book.id);
        let domain = SephirotDomain::from_hash(&key);
        out.push(BookEntry {
            id: book.id,
            title,
            domain,
            text_url,
        });
    }
    Ok(out)
}

/// Pick a usable plaintext URL from a gutendex `formats` map. The map keys
/// have shapes like `"text/plain; charset=utf-8"`, `"text/plain"`,
/// `"text/plain; charset=us-ascii"`, etc. Prefer UTF-8, fall back to any
/// `text/plain*`.
fn pick_text_url(formats: &std::collections::HashMap<String, String>) -> Option<String> {
    for (k, v) in formats {
        if k.starts_with("text/plain") && k.contains("utf-8") {
            return Some(v.clone());
        }
    }
    for (k, v) in formats {
        if k.starts_with("text/plain") {
            return Some(v.clone());
        }
    }
    None
}

/// Strip the Project Gutenberg header/footer. Lines bracketing the actual
/// content look like:
///   `*** START OF THE PROJECT GUTENBERG EBOOK ... ***`
///   `*** END OF THE PROJECT GUTENBERG EBOOK ... ***`
/// (older books use "THIS PROJECT GUTENBERG"; we tolerate both).
fn strip_pg_boilerplate(raw: &str) -> &str {
    let start_markers = ["*** START OF THE PROJECT GUTENBERG", "*** START OF THIS PROJECT GUTENBERG"];
    let end_markers = ["*** END OF THE PROJECT GUTENBERG", "*** END OF THIS PROJECT GUTENBERG"];

    let mut body_start = 0usize;
    for m in start_markers {
        if let Some(idx) = raw.find(m) {
            // Advance to end of that marker line.
            if let Some(nl) = raw[idx..].find('\n') {
                body_start = idx + nl + 1;
                break;
            }
        }
    }

    let mut body_end = raw.len();
    for m in end_markers {
        if let Some(idx) = raw.find(m) {
            body_end = idx;
            break;
        }
    }
    if body_end < body_start {
        return raw;
    }
    &raw[body_start..body_end]
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

    fn empty_source() -> GutenbergSource {
        let client = reqwest::Client::builder()
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .unwrap();
        GutenbergSource {
            client,
            pg_text_base: DEFAULT_PG_TEXT_BASE.to_string(),
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
    fn pg_header_and_footer_are_stripped() {
        let raw = "PG metadata blah\n\
                   *** START OF THE PROJECT GUTENBERG EBOOK FOO ***\n\
                   The actual book begins here.\n\
                   Chapter one.\n\
                   *** END OF THE PROJECT GUTENBERG EBOOK FOO ***\n\
                   PG license blurb...";
        let stripped = strip_pg_boilerplate(raw);
        assert!(stripped.contains("The actual book begins here."));
        assert!(!stripped.contains("PG metadata blah"));
        assert!(!stripped.contains("PG license blurb"));
    }

    #[test]
    fn pick_text_url_prefers_utf8() {
        let mut m = std::collections::HashMap::new();
        m.insert("text/plain".to_string(), "ascii".to_string());
        m.insert("text/plain; charset=utf-8".to_string(), "utf8".to_string());
        assert_eq!(pick_text_url(&m).as_deref(), Some("utf8"));
    }
}
