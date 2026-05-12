# aether-seeder

Distributed knowledge seeder for the Aether Mind. Spawns N concurrent
agents that pull real knowledge from a configurable source, content-hash
deduplicate, and POST batches to `/aether/knowledge/sync` to grow the
Knowledge Fabric.

> **Status: scaffold.** The trait surface, single-worker HTTP path,
> retry/backoff, and CLI swarm runner are in place. A real knowledge source
> implementation (Grokipedia/ArXiv/Wikipedia/curated PDFs) still needs to
> be written — see [`docs/DESIGN.md`](docs/DESIGN.md).

## Workspace layout

```
aether-seeder/
├── crates/
│   ├── seeder-common/                # KnowledgeSource trait, SeedNode, DedupSet, SephirotDomain
│   ├── seeder-agent/                 # Single worker (run_worker fn), HTTP client, retry/backoff
│   └── seeder-source-grokipedia/     # KnowledgeSource impl: grokipedia.com → Sephirot-tagged SeedNodes
└── bin/
    └── aether-seeder/                # CLI: `aether-seeder spawn -n 250 --source grokipedia`
```

## Build

```bash
cargo build --release
./target/release/aether-seeder --help
```

## Quick start (against local aether-mind)

```bash
export ADMIN_API_KEY=<from .env on the aether-mind host>
aether-seeder spawn -n 10 \
  --base-url http://127.0.0.1:5003 \
  --batch-size 50 \
  --max-batches 5
```

Two sources ship today:

- `placeholder` — emits stub text so you can validate the swarm, HTTP path,
  and dedup pipeline against a real Aether Mind without spamming production
  with junk.
- `grokipedia` — pulls ~100 curated articles across the 10 Sephirot
  cognitive domains from `grokipedia.com`, strips HTML, chunks at sentence
  boundaries (800 chars max per chunk, 40 chars min), dedups by content
  hash, and emits Sephirot-tagged `SeedNode`s. See
  `crates/seeder-source-grokipedia/src/topics.rs` for the topic list.

```bash
aether-seeder spawn -n 1 --source grokipedia \
  --batch-size 50 \
  --max-batches 5
```

## Production rollout

The investigation that gated 250 concurrent agents identified four
must-fix items before scaling:

1. **Write-side dedup** — Knowledge Fabric only dedups on retrieval today.
   `seeder-common::DedupSet` is the seed; real sources must compose it
   into their `fetch_batch` so the same content hash never crosses the
   network twice. See `crates/seeder-common/src/lib.rs`.
2. **Rate limiting** — use `ADMIN_API_KEY` to bypass the per-wallet
   tier caps. Per-agent JWT auth would cap us at 500 nodes/agent/day.
3. **Source size** — Grokipedia has ~200 articles; partition round-robin
   across workers and add additional corpora (Wikipedia dump, ArXiv,
   curated PDFs on IPFS) before a 250-agent run.
4. **Sephirot routing** — `SeedNode::domain` must reflect the actual
   cognitive domain. Manual curation per source, or embedding-based
   classification.

Validation gate before promotion to 250 workers: a 10-worker / 1-hour
run with zero duplicate vectors (verifiable via Knowledge Fabric merkle
root diff) and zero 429 errors using `ADMIN_API_KEY`.

## Resource budget on RTX 3080 Ti (12GB) + 15GB RAM + 12 cores

For 250 agents against a local Aether Mind running qwen2.5:7b on Ollama:

- RAM: ~10.6 / 15 GB (HTTP clients + shared Ollama LLM)
- VRAM: ~9 / 12 GB (qwen2.5:7b loaded once, reused)
- CPU: ~10% (I/O-bound)
- Network: <1% of gigabit
- Ollama: 5-10 embeddings/sec; 250 agents × 25K embeddings/day = trivial.

Bottleneck risk: the central source's `fetch_batch` becoming serial under
high contention. Benchmark before scaling.

## License

MIT
