# aether-seeder

Distributed knowledge seeder for the Aether Mind. Spawns N concurrent
agents that pull real knowledge from a configurable source, content-hash
deduplicate, and POST batches to the `/aether/embed → /aether/gradients`
pipeline to grow the Knowledge Fabric.

> **Status: production-grade.** The trait surface, swarm runner, retry/backoff,
> and 8 plug-in source crates are in place. Currently deployed as continuous
> systemd daemons on the Intel GPU box and the production droplet, both feeding
> the same aether-mind via Tailscale + Cloudflare tunnel.

## Workspace layout

```
aether-seeder/
├── crates/
│   ├── seeder-common/                # KnowledgeSource trait, SeedNode, DedupSet,
│   │                                 # SephirotDomain + SephirotDomain::from_hash()
│   ├── seeder-agent/                 # Worker (run_worker), HTTP client, retry/backoff
│   │
│   ├── seeder-source-grokipedia/     # grokipedia.com (~100 topics, curated)
│   ├── seeder-source-wikipedia/      # en.wikipedia.org extracts:
│   │                                 #   `WikipediaSource::new()`            — curated 230 topics
│   │                                 #   `WikipediaSource::with_random_pool` — N random titles (infinite pool)
│   ├── seeder-source-arxiv/          # export.arxiv.org Atom (65 curated papers)
│   ├── seeder-source-arxiv-recent/   # 8 arXiv RSS category feeds — rolling fresh papers
│   ├── seeder-source-gutenberg/      # Project Gutenberg via gutendex (public-domain books)
│   ├── seeder-source-stackexchange/  # SO + Math + CS + Physics + CSTheory + DataSci Q&A
│   └── seeder-source-huggingface/    # FineWeb-edu via datasets-server (curated web text)
│
└── bin/
    └── aether-seeder/                # CLI: `aether-seeder spawn -n 250 --source <id>`
```

Each `seeder-source-*` crate is a **self-contained plug-in**: it depends only
on `seeder-common` (for the trait) plus its own HTTP/parsing deps. Adding a
new source does not touch any existing source.

## Build

```bash
cargo build --release
./target/release/aether-seeder --help
```

## Quick start (against local aether-mind)

```bash
export ADMIN_API_KEY=seeder-local       # from .env on the aether-mind host
aether-seeder spawn -n 10 \
  --base-url http://127.0.0.1:5003 \
  --source wikipedia-random:500 \
  --batch-size 50 \
  --max-batches 5
```

## Source catalogue

All sources implement the same `seeder_common::KnowledgeSource` async trait.
Pick by content-distribution match:

| Source | Content space | Pool | Construction |
|---|---|---|---|
| `placeholder` | stub text for smoke tests | infinite | sync |
| `grokipedia` | curated articles, ~100 topics | finite (mostly dedup after first run) | sync |
| `wikipedia` | curated 230 topics across all 10 Sephirot | finite (mostly dedup after first run) | sync |
| `wikipedia-random[:N]` | **infinite** — random article API, default N=500/construction | refreshed each construction | async |
| `arxiv` | 65 hand-picked foundational ML/quantum/RL/alignment papers | finite | sync |
| `arxiv-recent` | rolling fresh papers via RSS across 8 categories (cs.AI, cs.LG, cs.CL, cs.CR, quant-ph, stat.ML, math.PR, math.ST) | refreshed each construction | async |
| `gutenberg[:N]` | public-domain books — long-form text, very different distribution from encyclopedia | ~210 books × hundreds of chunks/book at default N=30 | async (slow construction; gutendex listing × 7 topics) |
| `stackexchange` | technical Q&A across SO, Math, CS, Physics, CSTheory, DataSci | ~3,000 questions per construction | async |
| `huggingface-fineweb` (alias `fineweb`) | FineWeb-edu — curated educational web text from datasets-server | 50,000 rows per construction | async (slow; 500 paginated API calls at 1s pacing) |

### Smoke tests

```bash
aether-seeder spawn --source wikipedia-random:500 --count 5 --batch-size 25 --base-url http://127.0.0.1:5003 --admin-key seeder-local --max-batches 2
aether-seeder spawn --source arxiv-recent          --count 3 --batch-size 10 --base-url http://127.0.0.1:5003 --admin-key seeder-local --max-batches 2
aether-seeder spawn --source stackexchange         --count 3 --batch-size 10 --base-url http://127.0.0.1:5003 --admin-key seeder-local --max-batches 2
aether-seeder spawn --source gutenberg:5           --count 3 --batch-size 10 --base-url http://127.0.0.1:5003 --admin-key seeder-local --max-batches 2
aether-seeder spawn --source fineweb               --count 3 --batch-size 10 --base-url http://127.0.0.1:5003 --admin-key seeder-local --max-batches 2
```

## How to add a new source (modular plug-in pattern)

1. **Create the crate.** `cargo new --lib crates/seeder-source-yoursource`. In
   the new `Cargo.toml`, depend on `seeder-common` + whatever HTTP/parsing
   you need:

   ```toml
   [package]
   name = "seeder-source-yoursource"
   version.workspace = true
   edition.workspace = true
   license.workspace = true
   authors.workspace = true
   repository.workspace = true

   [dependencies]
   seeder-common = { path = "../seeder-common" }
   tokio = { workspace = true }
   reqwest = { workspace = true }
   async-trait = { workspace = true }
   serde = { workspace = true }
   serde_json = { workspace = true }
   tracing = { workspace = true }
   ```

2. **Implement the trait.** Copy the shape of
   `crates/seeder-source-wikipedia/src/lib.rs` — a `Source` struct holding
   `client`, `worklist`, `pending_chunks`, `dedup`; one `fetch_batch(n)`
   that pulls from `pending_chunks` and refills from the worklist. Use
   `SephirotDomain::from_hash(&id)` to route each chunk to one of the 10
   cognitive domains without needing per-source classification. Always
   set a contact-able UA: `"aether-seeder/0.1 (+https://qbc.network; info@qbc.network)"`.

3. **Register in the workspace.** Add the crate path under `[workspace]
   members = […]` in the root `Cargo.toml`.

4. **Register in the agent.** Add a `path = "../../crates/seeder-source-yoursource"`
   dep in `bin/aether-seeder/Cargo.toml`, then add one `match` arm in
   `bin/aether-seeder/src/main.rs`'s `build_source()`:

   ```rust
   "yoursource" => {
       let src = seeder_source_yoursource::YourSource::new().await?;
       Ok(Arc::new(src))
   }
   ```

5. **Test.** Build the whole workspace (`cargo build --release`). Add at
   least one `#[tokio::test]` confirming the trait shape, e.g.
   `fetch_batch(0)` returns `Ok(vec![])` on an empty worklist.

6. **Document.** Add one row to the source-catalogue table above.

Nothing else changes — the swarm runner, dedup, retry/backoff, and HTTP
ingestion path are agnostic to the source.

## Production deployment

Currently running as continuous `systemd` daemons (no timer, `Type=simple`
with auto-restart) on two boxes:

| Host | Service | Exit IP | Purpose |
|---|---|---|---|
| Intel GPU box (this repo's primary build host) | user systemd `aether-seeder.service` | Intel home IP | Primary seeder; cycle every ~3-4 min |
| Production droplet (152.42.215.182) | system systemd `aether-seeder.service` at `/usr/local/bin/aether-seeder` | droplet IP | Independent rate budget; hits aether-mind via Tailscale |

Each box runs a `aether-seeder-rotate.sh` wrapper that loops:

```
while true; do
  run_source wikipedia-random:1000  10 25
  run_source arxiv-recent            3 10
  run_source stackexchange           3 10
  run_source wikipedia              10 10
  run_source arxiv                   2  5
  run_source grokipedia              5 25
  sleep 120
done
```

Both boxes target the same aether-mind on the Intel box at
`http://100.127.54.82:5003` (Tailscale).

## Production rollout checklist (one-time)

1. **Write-side dedup** — ✅ `seeder-common::DedupSet` is composed into every
   source's `fetch_batch`; no content hash crosses the network twice.
2. **Rate limiting** — ✅ `ADMIN_API_KEY` bypasses per-wallet tier caps for
   the seeder.
3. **Source size** — ✅ 8 sources span finite-curated + rolling-feed +
   infinite-pool patterns.
4. **Sephirot routing** — ✅ `SephirotDomain::from_hash` provides stable
   string-hash bucketing for sources without natural domain labels.

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
