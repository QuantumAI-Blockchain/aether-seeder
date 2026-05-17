//! `ArxivSource` — fetches paper abstracts from arXiv via the public
//! Atom feed API at `https://export.arxiv.org/api/query?id_list={id}`.
//!
//! ArXiv's API guidance (https://info.arxiv.org/help/api/index.html)
//! asks for a User-Agent identifying the project and a delay of ≥3
//! seconds between requests from the same IP. This crate enforces
//! that delay via a per-source rate limiter (`RateLimitState`) shared
//! across all `fetch_batch` callers on the same `ArxivSource`. On HTTP
//! 429 the limiter consumes the `Retry-After` header when present and
//! falls back to exponential backoff (5s × 2^consecutive_throttles,
//! capped at 5 min) otherwise. Throttled topics are pushed back to the
//! front of the worklist so no work is lost.
//!
//! Topic list is a curated set of ~80 arXiv IDs across foundational
//! ML, distributed systems, and quantum computing papers — the
//! kind of background reading a frontier AI would benefit from.

pub mod topics;

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use regex::Regex;
use seeder_common::{
    shared_dedup, KnowledgeSource, SeedNode, SeederError, SeederResult, SephirotDomain,
    SharedDedup,
};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, info, warn};

const DEFAULT_BASE_URL: &str = "https://export.arxiv.org/api/query";
const DEFAULT_USER_AGENT: &str =
    "aether-seeder/0.1 (+https://qbc.network; info@qbc.network)";
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(45);

const CHUNK_MAX_CHARS: usize = 800;
const CHUNK_MIN_CHARS: usize = 40;
const ABSTRACT_MAX_CHARS: usize = 8_000;
const SEED_CONFIDENCE: f32 = 0.95; // peer-reviewed → higher than wiki

/// arXiv's stated minimum inter-request interval. One worker hitting
/// closer than this will earn 429s eventually.
const MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(3);
/// Base backoff when arXiv returns 429 without a Retry-After header.
const BACKOFF_BASE: Duration = Duration::from_secs(5);
/// Ceiling on the exponential backoff. Past this we stop doubling.
const BACKOFF_MAX: Duration = Duration::from_secs(300);

pub struct ArxivSource {
    client: reqwest::Client,
    base_url: String,
    worklist: Mutex<VecDeque<(&'static str, SephirotDomain)>>,
    pending_chunks: Mutex<VecDeque<SeedNode>>,
    dedup: SharedDedup,
    rate_limit: Mutex<RateLimitState>,
}

/// Per-source rate-limit ledger. Held inside a `Mutex` on the
/// `ArxivSource` so concurrent workers share one pacing token rather
/// than each holding their own.
struct RateLimitState {
    /// Wall-clock instant before which the next request must not be sent.
    next_allowed: Instant,
    /// Count of consecutive 429s observed since the last 2xx.
    /// Used for exponential backoff when `Retry-After` is absent.
    consecutive_throttles: u32,
}

impl RateLimitState {
    fn new() -> Self {
        Self {
            next_allowed: Instant::now(),
            consecutive_throttles: 0,
        }
    }
}

/// What `fetch_paper` saw for one arxiv_id.
enum FetchOutcome {
    /// Usable abstract.
    Body(String),
    /// arXiv said the paper has no usable summary (404, missing
    /// summary tag, summary too short, etc.). Drop the topic permanently.
    Skip,
    /// 429. Topic gets re-queued; rate limiter already updated.
    Throttled,
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
            rate_limit: Mutex::new(RateLimitState::new()),
        })
    }

    pub fn set_base_url(&mut self, base_url: impl Into<String>) {
        self.base_url = base_url.into();
    }

    /// Block until the rate limiter says we may send the next request,
    /// then advance `next_allowed` by `MIN_REQUEST_INTERVAL`. Concurrent
    /// callers serialize through the mutex and line up at successive
    /// 3-second slots rather than burst-firing.
    async fn acquire_rate_limit_slot(&self) {
        let wait = {
            let mut state = self.rate_limit.lock().await;
            let now = Instant::now();
            let wait = state.next_allowed.saturating_duration_since(now);
            let scheduled_at = if wait.is_zero() { now } else { state.next_allowed };
            state.next_allowed = scheduled_at + MIN_REQUEST_INTERVAL;
            wait
        };
        if !wait.is_zero() {
            debug!(wait_ms = wait.as_millis() as u64, "arxiv rate-limit pause");
            sleep(wait).await;
        }
    }

    /// Record a 429. Use `Retry-After` if provided; otherwise apply
    /// exponential backoff (5s × 2^consecutive_throttles, capped).
    async fn note_throttle(&self, retry_after: Option<Duration>) {
        let mut state = self.rate_limit.lock().await;
        state.consecutive_throttles = state.consecutive_throttles.saturating_add(1);
        let wait = retry_after.unwrap_or_else(|| {
            // shift = throttles - 1, capped so we don't overflow.
            let shift = state.consecutive_throttles.saturating_sub(1).min(6);
            BACKOFF_BASE
                .checked_mul(1u32 << shift)
                .unwrap_or(BACKOFF_MAX)
                .min(BACKOFF_MAX)
        });
        let target = Instant::now() + wait;
        if target > state.next_allowed {
            state.next_allowed = target;
        }
        info!(
            consecutive_throttles = state.consecutive_throttles,
            wait_secs = wait.as_secs(),
            retry_after_provided = retry_after.is_some(),
            "arxiv backoff scheduled"
        );
    }

    /// Reset throttle counter on a successful 2xx.
    async fn note_success(&self) {
        let mut state = self.rate_limit.lock().await;
        state.consecutive_throttles = 0;
    }

    async fn fetch_paper(&self, arxiv_id: &str) -> SeederResult<FetchOutcome> {
        self.acquire_rate_limit_slot().await;

        let resp = self
            .client
            .get(&self.base_url)
            .query(&[("id_list", arxiv_id), ("max_results", "1")])
            .send()
            .await
            .map_err(|e| SeederError::Transport(format!("fetch {arxiv_id}: {e}")))?;

        let status = resp.status();
        if status.as_u16() == 429 {
            let retry_after = parse_retry_after_seconds(resp.headers().get("retry-after"))
                .map(Duration::from_secs);
            self.note_throttle(retry_after).await;
            warn!(
                arxiv_id,
                retry_after_secs = retry_after.map(|d| d.as_secs()),
                "arxiv throttled (429); re-queueing topic"
            );
            return Ok(FetchOutcome::Throttled);
        }
        if !status.is_success() {
            warn!(arxiv_id, status = %status, "arxiv non-success");
            return Ok(FetchOutcome::Skip);
        }
        self.note_success().await;

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
            return Ok(FetchOutcome::Skip);
        };

        let summary = safe_truncate(&summary, ABSTRACT_MAX_CHARS);
        Ok(FetchOutcome::Body(format!("{title}\n\n{summary}")))
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
            FetchOutcome::Body(b) => b,
            FetchOutcome::Skip => return Ok(()),
            FetchOutcome::Throttled => {
                // Re-queue to the front so this topic is retried first
                // after the backoff clears.
                let mut wl = self.worklist.lock().await;
                wl.push_front((arxiv_id, domain));
                return Ok(());
            }
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

/// Parse the `Retry-After` header value as an integer number of seconds.
/// Returns `None` if absent, non-ASCII, non-integer, or the HTTP-date
/// variant (which arXiv doesn't use; exponential fallback covers it).
fn parse_retry_after_seconds(header: Option<&reqwest::header::HeaderValue>) -> Option<u64> {
    header
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
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

    #[test]
    fn parse_retry_after_seconds_handles_valid_integers() {
        use reqwest::header::HeaderValue;
        let v = HeaderValue::from_static("120");
        assert_eq!(parse_retry_after_seconds(Some(&v)), Some(120));
        let v = HeaderValue::from_static("  5  ");
        assert_eq!(parse_retry_after_seconds(Some(&v)), Some(5));
    }

    #[test]
    fn parse_retry_after_seconds_none_for_malformed_or_missing() {
        use reqwest::header::HeaderValue;
        assert_eq!(parse_retry_after_seconds(None), None);
        // HTTP-date variant — unsupported, falls back to exp backoff.
        let v = HeaderValue::from_static("Fri, 31 Dec 2026 23:59:59 GMT");
        assert_eq!(parse_retry_after_seconds(Some(&v)), None);
        let v = HeaderValue::from_static("not-a-number");
        assert_eq!(parse_retry_after_seconds(Some(&v)), None);
    }

    #[tokio::test]
    async fn acquire_advances_next_allowed_by_min_interval() {
        let src = ArxivSource::with_topics(std::iter::empty()).unwrap();
        let before = Instant::now();
        src.acquire_rate_limit_slot().await;
        let next_allowed = src.rate_limit.lock().await.next_allowed;
        // First slot returns immediately; next_allowed jumps to
        // ~MIN_REQUEST_INTERVAL beyond "before". Allow 500ms slack for
        // lock acquisition + scheduler noise.
        let gap = next_allowed.saturating_duration_since(before);
        assert!(
            gap >= MIN_REQUEST_INTERVAL.saturating_sub(Duration::from_millis(500)),
            "gap too small: {:?}",
            gap
        );
        assert!(
            gap <= MIN_REQUEST_INTERVAL + Duration::from_millis(500),
            "gap too large: {:?}",
            gap
        );
    }

    #[tokio::test]
    async fn note_throttle_uses_retry_after_when_present() {
        let src = ArxivSource::with_topics(std::iter::empty()).unwrap();
        let t0 = Instant::now();
        src.note_throttle(Some(Duration::from_secs(7))).await;
        let state = src.rate_limit.lock().await;
        assert_eq!(state.consecutive_throttles, 1);
        assert!(state.next_allowed >= t0 + Duration::from_secs(7));
        // Sanity bound: shouldn't be more than a few hundred ms past 7s.
        assert!(state.next_allowed <= t0 + Duration::from_secs(8));
    }

    #[tokio::test]
    async fn note_throttle_exponential_backoff_without_retry_after() {
        let src = ArxivSource::with_topics(std::iter::empty()).unwrap();
        // First 429 → BACKOFF_BASE (5s).
        let t0 = Instant::now();
        src.note_throttle(None).await;
        {
            let state = src.rate_limit.lock().await;
            assert_eq!(state.consecutive_throttles, 1);
            assert!(state.next_allowed >= t0 + BACKOFF_BASE);
            assert!(state.next_allowed <= t0 + BACKOFF_BASE + Duration::from_millis(500));
        }
        // Second 429 → 10s.
        let t1 = Instant::now();
        src.note_throttle(None).await;
        {
            let state = src.rate_limit.lock().await;
            assert_eq!(state.consecutive_throttles, 2);
            assert!(state.next_allowed >= t1 + Duration::from_secs(10));
        }
        // note_success resets the counter.
        src.note_success().await;
        let state = src.rate_limit.lock().await;
        assert_eq!(state.consecutive_throttles, 0);
    }
}
