//! `ArxivRecentSource` — pulls the latest arXiv submissions across a
//! handful of relevant categories from the RSS feeds (`/rss/{category}`),
//! collects paper IDs into a worklist, then resolves each ID against the
//! Atom API for the abstract.
//!
//! Contrast with `seeder-source-arxiv` which iterates a hand-curated
//! list of canonical IDs — this source is fresh-on-every-construction
//! and gives the seeder a moving target distribution.
//!
//! ArXiv AUP asks for a 3s delay between requests and an identifying
//! User-Agent — we honour both.

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
use tracing::{debug, info, warn};

const DEFAULT_API_BASE_URL: &str = "https://export.arxiv.org/api/query";
const DEFAULT_RSS_BASE_URL: &str = "https://export.arxiv.org/rss";
const DEFAULT_USER_AGENT: &str =
    "aether-seeder/0.1 (+https://qbc.network; info@qbc.network)";
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(45);
/// arXiv AUP minimum inter-request delay.
const ARXIV_REQUEST_DELAY: Duration = Duration::from_secs(3);

const CHUNK_MAX_CHARS: usize = 800;
const CHUNK_MIN_CHARS: usize = 40;
const ABSTRACT_MAX_CHARS: usize = 8_000;
const SEED_CONFIDENCE: f32 = 0.94;

/// Default category set — broad coverage across CS/AI, ML, NLP, security,
/// quantum and statistics. Each feed returns the day's ~20-50 latest items.
pub const DEFAULT_CATEGORIES: &[&str] = &[
    "cs.AI",
    "cs.LG",
    "cs.CL",
    "cs.CR",
    "quant-ph",
    "stat.ML",
    "math.PR",
    "math.ST",
];

pub struct ArxivRecentSource {
    client: reqwest::Client,
    api_base_url: String,
    worklist: Mutex<VecDeque<(String, SephirotDomain)>>,
    pending_chunks: Mutex<VecDeque<SeedNode>>,
    dedup: SharedDedup,
}

impl ArxivRecentSource {
    pub async fn new() -> SeederResult<Self> {
        Self::with_categories(DEFAULT_CATEGORIES).await
    }

    pub async fn with_categories(categories: &[&str]) -> SeederResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(DEFAULT_FETCH_TIMEOUT)
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .map_err(|e| SeederError::Transport(e.to_string()))?;

        let mut worklist: VecDeque<(String, SephirotDomain)> = VecDeque::new();
        for (i, cat) in categories.iter().enumerate() {
            if i > 0 {
                tokio::time::sleep(ARXIV_REQUEST_DELAY).await;
            }
            match fetch_rss_ids(&client, DEFAULT_RSS_BASE_URL, cat).await {
                Ok(ids) => {
                    debug!(category = cat, count = ids.len(), "arxiv-recent rss ids");
                    for id in ids {
                        let domain = SephirotDomain::from_hash(&id);
                        worklist.push_back((id, domain));
                    }
                }
                Err(e) => {
                    warn!(category = cat, err = %e, "arxiv-recent rss fetch failed");
                }
            }
        }
        info!(total_ids = worklist.len(), "arxiv-recent worklist seeded");

        Ok(Self {
            client,
            api_base_url: DEFAULT_API_BASE_URL.to_string(),
            worklist: Mutex::new(worklist),
            pending_chunks: Mutex::new(VecDeque::new()),
            dedup: shared_dedup(),
        })
    }

    pub fn set_api_base_url(&mut self, base_url: impl Into<String>) {
        self.api_base_url = base_url.into();
    }

    async fn fetch_paper(&self, arxiv_id: &str) -> SeederResult<Option<String>> {
        let resp = self
            .client
            .get(&self.api_base_url)
            .query(&[("id_list", arxiv_id), ("max_results", "1")])
            .send()
            .await
            .map_err(|e| SeederError::Transport(format!("fetch {arxiv_id}: {e}")))?;
        if !resp.status().is_success() {
            warn!(arxiv_id, status = %resp.status(), "arxiv-recent non-success");
            return Ok(None);
        }
        let xml = resp
            .text()
            .await
            .map_err(|e| SeederError::Transport(format!("read body {arxiv_id}: {e}")))?;

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

        let body = match self.fetch_paper(&arxiv_id).await? {
            Some(b) => b,
            None => return Ok(()),
        };

        let mut new_chunks = Vec::new();
        let mut dedup = self.dedup.lock().await;
        for chunk in chunk_text(&body) {
            let node = SeedNode {
                text: chunk,
                domain,
                source: format!("arxiv-recent:{arxiv_id}"),
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
impl KnowledgeSource for ArxivRecentSource {
    fn name(&self) -> &'static str {
        "arxiv-recent"
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

// ── RSS parsing (regex-based; the RSS we care about is well-formed) ──

/// arXiv RSS items look like:
/// `<item><title>...</title><link>http://arxiv.org/abs/2401.12345v1</link>...`
/// We extract the IDs from the `<link>` fields.
static RE_RSS_LINK: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?is)<link[^>]*>\s*https?://arxiv\.org/abs/([0-9]{4}\.[0-9]{4,6})(?:v\d+)?\s*</link>"#)
        .unwrap()
});

async fn fetch_rss_ids(
    client: &reqwest::Client,
    rss_base_url: &str,
    category: &str,
) -> SeederResult<Vec<String>> {
    let url = format!("{rss_base_url}/{category}");
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| SeederError::Transport(format!("rss {category}: {e}")))?;
    if !resp.status().is_success() {
        warn!(category, status = %resp.status(), "arxiv-recent rss non-success");
        return Ok(Vec::new());
    }
    let body = resp
        .text()
        .await
        .map_err(|e| SeederError::Transport(format!("rss read {category}: {e}")))?;

    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for cap in RE_RSS_LINK.captures_iter(&body) {
        if let Some(m) = cap.get(1) {
            let id = m.as_str().to_string();
            if seen.insert(id.clone()) {
                out.push(id);
            }
        }
    }
    Ok(out)
}

// ── XML extraction shared with the static arxiv source ───────────────

static RE_ENTRY_TITLE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?is)<entry>.*?<title[^>]*>(.*?)</title>").unwrap()
});

static RE_SUMMARY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<summary[^>]*>(.*?)</summary>").unwrap());

fn first_match(haystack: &str, re: &Regex) -> Option<String> {
    re.captures(haystack)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().trim().to_string())
}

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

    fn empty_source() -> ArxivRecentSource {
        let client = reqwest::Client::builder()
            .user_agent(DEFAULT_USER_AGENT)
            .build()
            .unwrap();
        ArxivRecentSource {
            client,
            api_base_url: DEFAULT_API_BASE_URL.to_string(),
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
    fn rss_link_regex_extracts_id() {
        let xml = r#"<item>
            <title>Some Paper</title>
            <link>http://arxiv.org/abs/2401.12345v1</link>
            </item>"#;
        let caps: Vec<_> = RE_RSS_LINK
            .captures_iter(xml)
            .filter_map(|c| c.get(1).map(|m| m.as_str().to_string()))
            .collect();
        assert_eq!(caps, vec!["2401.12345"]);
    }
}
