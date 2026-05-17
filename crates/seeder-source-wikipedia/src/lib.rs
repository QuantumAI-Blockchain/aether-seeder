//! `WikipediaSource` — pulls plain-text article extracts from the
//! English Wikipedia action API and emits Sephirot-tagged `SeedNode`s.
//!
//! Endpoint: `https://en.wikipedia.org/w/api.php?action=query&format=json
//! &prop=extracts&explaintext=&exsectionformat=plain&exlimit=1&titles={title}`
//!
//! Wikipedia explicitly approves this kind of bulk read traffic provided
//! the User-Agent identifies the project and you don't run requests at
//! a brutal rate. We set a contact-able UA and rely on the worklist
//! semantics of the seeder (one fetch per worker per ~500ms inter-batch
//! pause) to keep within their guidance.
//!
//! Topic list is intentionally complementary to `GrokipediaSource` — more
//! advanced/specialized articles, expanding into domains that Grokipedia
//! is sparse on.

pub mod topics;

use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use seeder_common::{
    shared_dedup, KnowledgeSource, SeedNode, SeederError, SeederResult, SephirotDomain,
    SharedDedup,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, warn};

const DEFAULT_BASE_URL: &str = "https://en.wikipedia.org/w/api.php";
const DEFAULT_USER_AGENT: &str =
    "aether-seeder/0.1 (+https://qbc.network; info@qbc.network)";
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(45);

/// Sentence-shaped chunk target (matches Grokipedia source).
const CHUNK_MAX_CHARS: usize = 800;
const CHUNK_MIN_CHARS: usize = 40;
const ARTICLE_MAX_CHARS: usize = 50_000;
const SEED_CONFIDENCE: f32 = 0.92;

pub struct WikipediaSource {
    client: reqwest::Client,
    base_url: String,
    worklist: Mutex<VecDeque<(String, SephirotDomain)>>,
    pending_chunks: Mutex<VecDeque<SeedNode>>,
    dedup: SharedDedup,
}

impl WikipediaSource {
    pub fn new() -> SeederResult<Self> {
        Self::with_topics(topics::SEED_TOPICS.iter().copied())
    }

    /// Build a source whose worklist is `SEED_TOPICS` rotated left by
    /// `offset` — different `offset`s cause re-runs to explore different
    /// slices first instead of always re-fetching the head of the list.
    pub fn with_rotation(offset: usize) -> SeederResult<Self> {
        Self::with_topics(topics::rotated_topics(offset).into_iter())
    }

    pub fn with_topics<I>(topics: I) -> SeederResult<Self>
    where
        I: IntoIterator<Item = (&'static str, SephirotDomain)>,
    {
        Self::with_owned_topics(topics.into_iter().map(|(t, d)| (t.to_string(), d)))
    }

    /// Like `with_topics` but accepts owned `String` titles — used for
    /// dynamic sources like the random-article pool.
    pub fn with_owned_topics<I>(topics: I) -> SeederResult<Self>
    where
        I: IntoIterator<Item = (String, SephirotDomain)>,
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

    /// Build a WikipediaSource seeded with `n` random article titles
    /// fetched live from `action=query&list=random`. Each title is
    /// assigned to a Sephirot domain via stable string hashing so the
    /// fabric stays roughly balanced across domains over time.
    ///
    /// Wikipedia caps `rnlimit` at 500 per call, so we batch.
    pub async fn with_random_pool(n: usize) -> SeederResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_FETCH_TIMEOUT)
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .map_err(|e| SeederError::Transport(e.to_string()))?;
        let titles = fetch_random_titles(&client, DEFAULT_BASE_URL, n).await?;
        let topics: Vec<(String, SephirotDomain)> = titles
            .into_iter()
            .map(|t| {
                let d = SephirotDomain::from_hash(&t);
                (t, d)
            })
            .collect();
        debug!(count = topics.len(), "wikipedia-random pool seeded");
        Ok(Self {
            client,
            base_url: DEFAULT_BASE_URL.to_string(),
            worklist: Mutex::new(topics.into_iter().collect()),
            pending_chunks: Mutex::new(VecDeque::new()),
            dedup: shared_dedup(),
        })
    }

    pub fn set_base_url(&mut self, base_url: impl Into<String>) {
        self.base_url = base_url.into();
    }

    async fn fetch_article(&self, title: &str) -> SeederResult<Option<String>> {
        let resp = self
            .client
            .get(&self.base_url)
            .query(&[
                ("action", "query"),
                ("format", "json"),
                ("prop", "extracts"),
                ("explaintext", "1"),
                ("exsectionformat", "plain"),
                ("exlimit", "1"),
                ("titles", title),
                ("redirects", "1"),
            ])
            .send()
            .await
            .map_err(|e| SeederError::Transport(format!("fetch {title}: {e}")))?;

        if !resp.status().is_success() {
            warn!(title, status = %resp.status(), "wikipedia non-success");
            return Ok(None);
        }
        let body: WikipediaResponse = resp
            .json()
            .await
            .map_err(|e| SeederError::Transport(format!("parse {title}: {e}")))?;

        let pages = body.query.pages;
        // Single-title query returns a one-entry map keyed by page_id.
        let Some((_, page)) = pages.into_iter().next() else {
            return Ok(None);
        };
        let extract = page.extract.unwrap_or_default();
        if extract.len() < 100 {
            debug!(title, "extract too short");
            return Ok(None);
        }

        // Slice on a UTF-8 char boundary to avoid panicking on multi-byte
        // codepoints (em-dashes, mathematical symbols, non-ASCII content).
        let extract = safe_truncate(&extract, ARTICLE_MAX_CHARS);

        // Wikipedia extracts already drop wiki markup but can contain "=="
        // headings and stray whitespace; normalize lightly.
        let cleaned = extract
            .replace("==", " ")
            .replace("\n\n", " ")
            .replace('\t', " ");
        let title_display = page
            .title
            .unwrap_or_else(|| title.replace('_', " "));

        Ok(Some(format!("{title_display}\n\n{cleaned}")))
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
        let Some((title, domain)) = next else {
            return Ok(());
        };

        let body = match self.fetch_article(&title).await? {
            Some(b) => b,
            None => return Ok(()),
        };

        let mut new_chunks = Vec::new();
        let mut dedup = self.dedup.lock().await;
        for chunk in chunk_text(&body) {
            let node = SeedNode {
                text: chunk,
                domain,
                source: format!("wikipedia:{title}"),
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
impl KnowledgeSource for WikipediaSource {
    fn name(&self) -> &'static str {
        "wikipedia"
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

/// Truncate `s` to at most `max_bytes` while preserving UTF-8 char
/// boundaries.
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

// ── JSON envelope ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct WikipediaResponse {
    query: WikipediaQuery,
}

#[derive(Debug, Deserialize)]
struct WikipediaQuery {
    pages: std::collections::HashMap<String, WikipediaPage>,
}

#[derive(Debug, Deserialize)]
struct WikipediaPage {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    extract: Option<String>,
}

// ── Random-article pool ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RandomEnvelope {
    query: RandomQuery,
}

#[derive(Debug, Deserialize)]
struct RandomQuery {
    random: Vec<RandomEntry>,
}

#[derive(Debug, Deserialize)]
struct RandomEntry {
    title: String,
}

/// Fetch up to `n` random article titles. Wikipedia caps `rnlimit` at
/// 500 per request; we batch when n > 500 with multiple sequential calls.
/// Article namespace only (ns=0), so no User_talk / Category / etc.
async fn fetch_random_titles(
    client: &reqwest::Client,
    base_url: &str,
    n: usize,
) -> SeederResult<Vec<String>> {
    let mut out = Vec::with_capacity(n);
    let mut remaining = n;
    while remaining > 0 {
        let batch = remaining.min(500);
        let resp = client
            .get(base_url)
            .query(&[
                ("action", "query"),
                ("format", "json"),
                ("list", "random"),
                ("rnnamespace", "0"),
                ("rnlimit", &batch.to_string()),
            ])
            .send()
            .await
            .map_err(|e| SeederError::Transport(format!("random list: {e}")))?;
        if !resp.status().is_success() {
            warn!(status = %resp.status(), "wikipedia random list non-success");
            break;
        }
        let envelope: RandomEnvelope = resp
            .json()
            .await
            .map_err(|e| SeederError::Transport(format!("random parse: {e}")))?;
        let returned = envelope.query.random.len();
        for entry in envelope.query.random {
            // The MediaWiki action API returns titles with spaces; the
            // article fetch path accepts either underscores or spaces
            // when `redirects=1` is set, so leave as-is.
            out.push(entry.title);
        }
        if returned == 0 {
            break;
        }
        remaining = remaining.saturating_sub(returned);
    }
    Ok(out)
}

// ── Sentence-aware chunker (Rust regex has no look-around) ───────────

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

    #[test]
    fn chunk_text_respects_bounds() {
        let long = "Sentence one. Sentence two! Sentence three? ".repeat(50);
        let chunks = chunk_text(&long);
        assert!(chunks.len() > 1);
        for c in &chunks {
            assert!(c.len() <= CHUNK_MAX_CHARS + 50);
            assert!(c.len() >= CHUNK_MIN_CHARS);
        }
    }

    #[tokio::test]
    async fn fetch_batch_empty_on_exhausted_worklist() {
        let src = WikipediaSource::with_topics(std::iter::empty()).unwrap();
        assert!(src.fetch_batch(10).await.unwrap().is_empty());
    }

    #[test]
    fn topics_module_compiles_and_is_non_trivial() {
        assert!(topics::SEED_TOPICS.len() >= 50);
    }
}
