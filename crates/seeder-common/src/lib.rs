//! Shared types and traits for the Aether knowledge seeder.
//!
//! The seeder fans out into N concurrent worker tasks; each worker pulls
//! batches of `SeedNode`s from a shared `KnowledgeSource` (which is responsible
//! for content-hash deduplication and round-robin partitioning across workers)
//! and POSTs them to the Aether Mind `/aether/knowledge/sync` endpoint.
//!
//! The trait is async and shareable across tasks via `Arc<dyn KnowledgeSource>`.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

/// A single piece of knowledge ready for ingestion.
///
/// The shape mirrors what `POST /aether/knowledge/sync` expects. Concrete
/// payload field names need to be confirmed against
/// `aether-core/bin/aether-mind/src/main.rs` `KnowledgeSyncRequest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedNode {
    /// Raw knowledge text (must be ≤ chunk size; suggested 800-1500 chars).
    pub text: String,
    /// Sephirot cognitive domain — must be one of the 10 (see
    /// `aether-core/crates/aether-sephirot`).
    pub domain: SephirotDomain,
    /// Free-form source attribution, e.g. `seeder_agent_47:grokipedia:Quantum_computing`.
    pub source: String,
    /// Confidence in [0.1, 1.0]; defaults to 0.85 if unspecified by source.
    pub confidence: f32,
}

impl SeedNode {
    /// Stable content hash for dedup. Hashes text + domain only — source and
    /// confidence are metadata and don't change identity.
    pub fn content_hash(&self) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(self.text.as_bytes());
        h.update([self.domain as u8]);
        h.finalize().into()
    }
}

/// The 10 Sephirot cognitive domains used by Aether Mind. Order matches
/// `aether-transformer::config::SephirotDomain` — if that ordering ever
/// changes, `content_hash` stability is broken; bump a version tag here.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SephirotDomain {
    Keter = 0,
    Chochmah = 1,
    Binah = 2,
    Chesed = 3,
    Gevurah = 4,
    Tiferet = 5,
    Netzach = 6,
    Hod = 7,
    Yesod = 8,
    Malkuth = 9,
}

#[derive(Debug, thiserror::Error)]
pub enum SeederError {
    #[error("source exhausted")]
    Exhausted,
    #[error("transport: {0}")]
    Transport(String),
    #[error("rate limited; retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },
    #[error("other: {0}")]
    Other(#[from] anyhow::Error),
}

pub type SeederResult<T> = Result<T, SeederError>;

/// A `KnowledgeSource` produces batches of `SeedNode`s. Implementations MUST
/// guarantee uniqueness across workers — naive implementations that hand out
/// the same item to every caller produce 250x duplicate ingestion.
///
/// The recommended pattern: maintain an internal `seen: Arc<Mutex<HashSet<...>>>`
/// of content hashes and only emit nodes whose hash hasn't been seen.
#[async_trait]
pub trait KnowledgeSource: Send + Sync {
    /// Fetch up to `n` unique `SeedNode`s. Returns fewer than `n` (possibly
    /// zero) if the source is exhausted; never returns duplicates.
    async fn fetch_batch(&self, n: usize) -> SeederResult<Vec<SeedNode>>;

    /// Human-readable identifier for logging.
    fn name(&self) -> &'static str;
}

/// Reusable content-hash dedup state. Wrap in `Arc<Mutex<>>` for sharing
/// across workers or across a single source instance.
#[derive(Default)]
pub struct DedupSet {
    seen: HashSet<[u8; 32]>,
}

impl DedupSet {
    /// Returns `true` if this hash is new (and records it).
    pub fn insert_new(&mut self, hash: [u8; 32]) -> bool {
        self.seen.insert(hash)
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

/// Convenience type alias for sharing dedup state.
pub type SharedDedup = Arc<Mutex<DedupSet>>;

pub fn shared_dedup() -> SharedDedup {
    Arc::new(Mutex::new(DedupSet::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_hash_is_stable_for_text_and_domain() {
        let a = SeedNode {
            text: "Quantum entanglement is a phenomenon".to_string(),
            domain: SephirotDomain::Chochmah,
            source: "grokipedia:quantum_entanglement".to_string(),
            confidence: 0.9,
        };
        let b = SeedNode {
            text: a.text.clone(),
            domain: a.domain,
            source: "different_source".to_string(),
            confidence: 0.5,
        };
        // Same text+domain ⇒ same hash even with different source/confidence.
        assert_eq!(a.content_hash(), b.content_hash());
    }

    #[test]
    fn content_hash_differs_per_domain() {
        let a = SeedNode {
            text: "Same text".to_string(),
            domain: SephirotDomain::Chochmah,
            source: "x".to_string(),
            confidence: 1.0,
        };
        let b = SeedNode { domain: SephirotDomain::Binah, ..a.clone() };
        assert_ne!(a.content_hash(), b.content_hash());
    }

    #[tokio::test]
    async fn dedup_set_rejects_repeats() {
        let mut d = DedupSet::default();
        let h = [0u8; 32];
        assert!(d.insert_new(h));
        assert!(!d.insert_new(h));
        assert_eq!(d.len(), 1);
    }
}
