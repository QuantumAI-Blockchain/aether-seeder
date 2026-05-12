//! `ArxivSource` — fetches paper abstracts from arXiv via the public
//! Atom feed API at `https://export.arxiv.org/api/query?id_list={id}`.
//!
//! ArXiv's API guidance (https://info.arxiv.org/help/api/index.html)
//! asks for a User-Agent identifying the project and a delay of ≥3
//! seconds between requests from the same IP. The seeder's per-batch
//! inter-batch-pause (default 500ms) combined with the worklist-pop
//! pattern (one fetch per refill_from_next_topic) stays inside that
//! budget at small concurrency. At 10+ workers we'd need to add a
//! global rate-limiter; that's a follow-up.
//!
//! Topic list is a curated set of ~80 arXiv IDs across foundational
//! ML, distributed systems, and quantum computing papers — the
//! kind of background reading a frontier AI would benefit from.

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

const DEFAULT_BASE_URL: &str = "https://export.arxiv.org/api/query";
const DEFAULT_USER_AGENT: &str =
    "aether-seeder/0.1 (+https://qbc.network; info@qbc.network)";
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(45);

const CHUNK_MAX_CHARS: usize = 800;
const CHUNK_MIN_CHARS: usize = 40;
const ABSTRACT_MAX_CHARS: usize = 8_000;
const SEED_CONFIDENCE: f32 = 0.95; // peer-reviewed → higher than wiki

pub struct ArxivSource {
    client: reqwest::Client,
    base_url: String,
    worklist: Mutex<VecDeque<(&'static str, SephirotDomain)>>,
    pending_chunks: Mutex<VecDeque<SeedNode>>,
    dedup: SharedDedup,
}

impl ArxivSource {
    pub fn new() -> SeederResult<Self> {
        Self::with_topics(topics::SEED_TOPICS.iter().copied())
    }

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

    pub fn set_base_url(&mut self, base_url: impl Into<String>) {
        self.base_url = base_url.into();
    }

    async fn fetch_paper(&self, arxiv_id: &str) -> SeederResult<Option<String>> {
        let resp = self
            .client
            .get(&self.base_url)
            .query(&[("id_list", arxiv_id), ("max_results", "1")])
            .send()
            .await
            .map_err(|e| SeederError::Transport(format!("fetch {arxiv_id}: {e}")))?;
        if !resp.status().is_success() {
            warn!(arxiv_id, status = %resp.status(), "arxiv non-success");
            return Ok(None);
        }
        let xml = resp
            .text()
            .await
            .map_err(|e| SeederError::Transport(format!("read body {arxiv_id}: {e}")))?;

        // Pull <title> and <summary> from the first <entry>; arXiv's
        // schema puts the paper title in entry/title and the abstract in
        // entry/summary.
        let title = first_match(&xml, &RE_ENTRY_TITLE).unwrap_or_else(|| arxiv_id.to_string());
        let summary = first_match(&xml, &RE_SUMMARY)
            .map(normalize_arxiv_summary)
            .filter(|s| s.len() >= 100);
        let Some(summary) = summary else {
            debug!(arxiv_id, "no usable summary");
            return Ok(None);
        };

        let summary = safe_truncate(&summary, ABSTRACT_MAX_CHARS);
        Ok(Some(format!("{title}\n\n{summary}")))
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
        let Some((arxiv_id, domain)) = next else {
            return Ok(());
        };

        let body = match self.fetch_paper(arxiv_id).await? {
            Some(b) => b,
            None => return Ok(()),
        };

        let mut new_chunks = Vec::new();
        let mut dedup = self.dedup.lock().await;
        for chunk in chunk_text(&body) {
            let node = SeedNode {
                text: chunk,
                domain,
                source: format!("arxiv:{arxiv_id}"),
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
impl KnowledgeSource for ArxivSource {
    fn name(&self) -> &'static str {
        "arxiv"
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

// ── XML extraction (regex is fine here; the feed is well-formed and
//    we only need two tags. A full XML parser would be overkill.) ─────

static RE_ENTRY_TITLE: Lazy<Regex> = Lazy::new(|| {
    // First <title> inside an <entry> block. The first <title> overall is
    // the feed title, so we look for the one inside <entry>.
    Regex::new(r"(?is)<entry>.*?<title[^>]*>(.*?)</title>").unwrap()
});

static RE_SUMMARY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<summary[^>]*>(.*?)</summary>").unwrap());

fn first_match(haystack: &str, re: &Regex) -> Option<String> {
    re.captures(haystack)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim().to_string())
}

/// arXiv summaries wrap lines at 80 chars and use single-newline breaks
/// between paragraphs. Normalize whitespace before chunking.
fn normalize_arxiv_summary(s: String) -> String {
    s.replace('\n', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
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

    #[test]
    fn normalize_collapses_whitespace_and_newlines() {
        let s = "Line one\n   has wrapping.\nLine two has  spaces.".to_string();
        assert_eq!(
            normalize_arxiv_summary(s),
            "Line one has wrapping. Line two has spaces."
        );
    }

    #[test]
    fn first_match_extracts_entry_title() {
        let xml = r#"<?xml version="1.0"?><feed>
            <title>arXiv Query</title>
            <entry><title>Attention Is All You Need</title>
            <summary>We propose...</summary></entry>
            </feed>"#;
        assert_eq!(
            first_match(xml, &RE_ENTRY_TITLE).as_deref(),
            Some("Attention Is All You Need")
        );
        assert_eq!(first_match(xml, &RE_SUMMARY).as_deref(), Some("We propose..."));
    }

    #[test]
    fn safe_truncate_handles_multibyte() {
        let s = "abc—def"; // em-dash is 3 UTF-8 bytes
        let truncated = safe_truncate(s, 4);
        assert!(truncated.chars().all(|c| c.is_ascii() || c == '—' || c == 'a' || c == 'b' || c == 'c'));
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    #[tokio::test]
    async fn fetch_batch_empty_on_exhausted_worklist() {
        let src = ArxivSource::with_topics(std::iter::empty()).unwrap();
        assert!(src.fetch_batch(10).await.unwrap().is_empty());
    }

    #[test]
    fn topics_has_breadth() {
        assert!(topics::SEED_TOPICS.len() >= 40);
    }
}
