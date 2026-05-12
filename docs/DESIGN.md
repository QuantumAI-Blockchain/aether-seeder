# Aether Seeder — Design

## Goal

Grow the Aether Mind Knowledge Fabric (`aether-core/crates/aether-fabric`)
with real, high-quality knowledge by running N concurrent worker agents
that pull from external corpora, dedup, embed, and POST to the live
`aether-mind` HTTP endpoint.

Target initial scale: **250 concurrent workers on a single GPU host**
(RTX 3080 Ti 12 GB, 15 GB RAM, 12-core i9-12900F).

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ aether-seeder spawn -n 250                                      │
│                                                                 │
│   Arc<dyn KnowledgeSource>  (single, shared, dedup'd)           │
│         │                                                       │
│   ┌─────┼──────────────────────────────────────────────────┐    │
│   │     │   tokio::spawn × 250                             │    │
│   │  ┌──▼──┐  ┌──────┐  ┌──────┐  ...  ┌──────┐            │    │
│   │  │  W1 │  │  W2  │  │  W3  │       │ W250 │            │    │
│   │  └──┬──┘  └──┬───┘  └──┬───┘       └──┬───┘            │    │
│   └─────┼────────┼─────────┼──────────────┼────────────────┘    │
│         │        │         │              │                     │
│         │        ▼         ▼              ▼                     │
│         │  POST /aether/knowledge/sync  (X-Admin-Key)           │
└─────────┼───────────────────────────────────────────────────────┘
          │
          ▼
     aether-mind (RTX 3080 Ti host, port 5003)
          │
          ▼
     Knowledge Fabric  (HNSW + RocksDB, 10 Sephirot shards)
```

## Why a shared source instead of per-worker sources?

If every worker spawned its own `GrokipediaSource`, they would all crawl
the same 200 articles and produce 250× identical SeedNodes. The Knowledge
Fabric does not dedup on write (see
`aether-core/crates/aether-fabric/src/search.rs:36-41` — dedup is on
retrieval), so we'd land 250× duplicate vectors.

Architecture rule: **the source owns dedup and partitioning.** Workers
just consume `fetch_batch(n)` and trust the source to return only fresh
items, never seen by any peer worker.

## Implementing a real `KnowledgeSource`

The trait surface is in `crates/seeder-common/src/lib.rs`:

```rust
#[async_trait]
pub trait KnowledgeSource: Send + Sync {
    async fn fetch_batch(&self, n: usize) -> SeederResult<Vec<SeedNode>>;
    fn name(&self) -> &'static str;
}
```

A working `GrokipediaSource` would:

1. Maintain a worklist of article slugs sourced from Grokipedia's index.
2. For each fetch: pop next slug, HTTP GET the article, chunk to 800–1500
   chars, classify into a `SephirotDomain` (manual mapping per topic, or
   embed-then-route via cosine similarity to per-Sephirot exemplar
   vectors).
3. For each chunk: compute `SeedNode::content_hash()`, check
   `DedupSet::insert_new(hash)`. If novel, push to batch; otherwise skip.
4. Return as soon as `n` novel chunks accumulated, or the worklist
   empties (return Ok(empty) to signal exhaustion).
5. Wrap state in `Arc<Mutex<...>>` to keep async-safe concurrent access.

Other sources to add over time:

- `WikipediaDumpSource` — process the ~20 GB enwiki dump, partition by
  category. Bigger corpus, slower per-chunk.
- `ArxivSource` — daily ArXiv listings, abstracts + intro paragraphs.
- `IpfsCuratedSource` — read a curated CAR file off IPFS hash, expert-
  curated science/philosophy/law content.
- `SyntheticDebateSource` — *only* once we have a solid base. Two LLM
  instances debate a topic; the consensus answer becomes a SeedNode.

## HTTP wire format

The current `seeder-agent` posts:

```
POST {base_url}/aether/knowledge/sync
X-Admin-Key: {admin_key}
Content-Type: application/json

{
  "nodes": [
    {
      "text": "...",
      "domain": "Chochmah",
      "source": "seeder_agent_47:grokipedia:Quantum_computing",
      "confidence": 0.85
    }
  ]
}
```

**Caveat:** this shape is a best-effort match for what `aether-mind`
expects, derived from inspecting `aether-core/bin/aether-mind/src/main.rs`
strings (`KnowledgeSyncRequest with 5 elements`). Before going live,
read the actual `KnowledgeSyncRequest` struct definition and reconcile.
That's the first concrete TODO before promoting beyond `--n 1`.

## Resource budget (250 workers, this GPU box)

| | Per agent | × 250 | Available | Headroom |
|--|---|---|---|---|
| Heap (HTTP client, JSON buffers) | ~2 MB | ~500 MB | 15 GB RAM | ✅ ample |
| Shared Ollama LLM | (shared) | 9 GB VRAM | 12 GB VRAM | ✅ 3 GB slack |
| Tokio task slot | <1 KB | <250 KB | unlimited | ✅ |
| Outbound HTTP fan-out | 100 KB/s peak | 25 MB/s | 1 Gbit/s | ✅ |

Bottleneck candidates ranked: (1) source's internal Mutex contention,
(2) Ollama queue when embeddings are needed (each batch in this scaffold
DOES NOT embed locally — it ships the raw text and trusts aether-mind to
embed via its `TextEmbedder`), (3) HNSW write lock on
the Knowledge Fabric.

## Validation gates before scaling

1. **n=1, max-batches=5, placeholder source** — smoke test, verify HTTP
   path, retry behavior, stats reporting.
2. **n=1, real GrokipediaSource, max-batches=10** — verify wire format
   matches, dedup state behaves, Sephirot domain routing is sensible.
3. **n=10, real source, 1 hour** — measure: zero 429s with admin key,
   zero duplicate vectors (diff Knowledge Fabric merkle root before/after,
   subtract by node count — should be exact).
4. **n=50, 1 hour** — same expectations + monitor VRAM/RAM/CPU.
5. **n=250, 30 minutes** — full scale; check Phi trajectory monotonic
   increase, knowledge_vectors counter incrementing, no OOM.

Any failure at a lower gate blocks the next one.

## Open questions

- Should embeddings happen client-side (in the agent, via candle) or
  server-side (in aether-mind, via its existing TextEmbedder)? Server-side
  centralizes the embedding model and dedup, but creates a hot spot.
  Initial choice: server-side (simpler scaffold), revisit at n=250 if
  Ollama queue saturates.
- Sephirot domain routing — manual per-topic or learned? Initial choice:
  manual map. Add a fallback `embed_and_route()` for unknown topics.
- Per-batch confidence scoring — should we ship `0.85` (the existing
  default) or compute something? Cohort with content quality if we add
  any source that doesn't have ground-truth (e.g., synthetic).

## Non-goals (this repo)

- Modifying `aether-mind` itself. If the wire format needs changes, that
  PR goes to `qubitcoin-aether`, not here.
- Running on the agent-stack box (100.80.115.96). This is GPU-host
  local.
- Knowledge curation pipelines (selecting which corpora are worth
  ingesting). That's editorial, not infrastructure.
