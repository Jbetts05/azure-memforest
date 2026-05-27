# Copilot instructions for MemForest

## Repository reality

- This repository is a Python research artifact, not a package with a dedicated build target.
- Runtime dependencies are pinned in `requirements.txt`.
- There is no checked-in build script, lint configuration, or test suite in this snapshot.
- The code expects external OpenAI-compatible chat and embedding endpoints, configured through `src/config/default.yaml` / `src/config/default_4b.yaml` or through the values consumed by `src/config/config.py`.

## Commands to use

- Install Python dependencies:
  - `python -m pip install -r requirements.txt`
- Run a Python snippet or import module code directly from `src/`.
- `pytest` is available from the pinned dependencies, but there are no checked-in tests in this snapshot. When a test file is added, run it individually with:
  - `pytest path/to/test.py -q`
- No repository-level lint command is configured in this checkout.

## High-level architecture

MemForest is a persistent long-memory system for multi-turn agent memory. The code is organized around four layers:

1. **Extraction and canonicalization**
   - `src/extraction/pipeline.py` uses `ChunkExtractionPipeline` to chunk turns into `MemCell`s and extract `MemoryItem`s.
   - The extractor uses stable item IDs, per-cell item caps, normalized entity/topic/domain/collection keys, and a fallback extraction path when no LLM backend is present.
   - `src/extraction/fact_manager.py` owns canonical fact storage. It builds and maintains a FAISS index, exact-dedup index, and duplicate-merge history. New items are embedded, filtered against exact matches, then checked through similarity plus LLM equivalence judging.

2. **Tree construction and summarization**
   - `src/build/tree_builder.py` builds the MemTree structures. The builder is organized into explicit phases:
     - structure building (no LLM calls),
     - summary generation, and
     - embedding/index creation.
   - The tree layer contains three tree families: session trees, entity trees, and scene trees. The code routes facts through entity and scene routers before constructing trees.
   - `src/build/tree.py` handles tree creation, insertion, dirty-node bookkeeping, and tree balancing. It preserves temporal ordering and produces internal nodes that summarize child content.
   - `src/build/summary_manager.py` drives LLM summary generation in parallel. The summary prompts are produced from the tree prompt manager.

3. **Persistence and indexing**
   - `src/forest/memforest.py` extends the tree builder with content-hash summary caching. This is important: unchanged descendants can reuse cached summaries during rebuilds instead of re-calling the LLM.
   - `src/build/node_index.py` maintains two parallel index structures: a searchable root-only FAISS index for recall and an embedding store for browse.
   - `src/build/tree_store.py` persists trees and tree metadata under per-user storage.

4. **Retrieval and query orchestration**
   - `src/query/retriever.py` implements recall strategies (`td`, `bu`, and `union`). The default is `union`, combining top embeddings from tree roots with back-projection from fact hits.
   - `src/query/planner.py` decomposes questions into per-tree subqueries. When enabled, it performs one batch LLM planning call over all recalled trees.
   - `src/query/browser.py` performs global beam search through tree nodes and always descends to L0 leaves, using node embeddings and optional LLM-guided ranking.
   - `src/query/pipeline.py` composes recall → planning → browsing → reranking into the final `QueryResult` structure.

## Key codebase conventions

- Keep the extraction, structuring, summarization, and indexing phases separate. The builder code expects structural changes first, then summary generation, then embedding/index updates. Avoid mutating summaries directly outside the builder/summary-manager path.
- `MemoryItem` is the canonical extracted unit. When editing or adding extraction logic, preserve stable IDs, participant metadata, semantic roles, and normalized key lists because downstream tree routing and fact dedupe depend on them.
- `FactManager` is the authoritative canonical fact store. It uses exact-match lookup first, then embedding similarity, then LLM equivalence checks. If you change dedupe behavior, update the fact-manager logic instead of adding ad hoc merging in higher layers.
- The project relies on content-hash caching for summary reuse during incremental rebuilds. When changing tree rebuild or deletion behavior, preserve the cache semantics in `CachingTreeBuilder`.
- Query recall is tree-centric. The `ForestRetriever` expects tree IDs, `BrowsePlanner` emits per-tree subqueries, and `TreeBrowser` returns leaf fact or cell IDs. Do not bypass those layers when adding retrieval behavior.
- Per-user state is isolated under the runtime snapshot directory. `UserForest` owns extraction, fact storage, tree storage, node indexing, and query pipeline per user.
- The repository includes benchmark CSVs and paper assets under `benchmark/` and `paper/`; those are data artifacts for reproduction, not runtime build outputs.
- Python code uses explicit typed dataclasses and dataclass-backed configs in `src/config/`. Prefer working through the existing config objects instead of introducing ad hoc environment lookups.

## Enterprise Azure migration guidance

- Preserve the paper semantics first: canonical facts, session/entity/scene MemTrees, summary reuse, tree recall, tree browsing, reranking, and benchmark parity are the behavior contract.
- Do not treat Azure AI Search as a complete replacement for `NodeIndex`. `NodeIndex` currently provides both root recall and browse-time node embeddings; cloud work should split those into a managed recall index plus a browse embedding store.
- Do not make process-local state authoritative. `MemForest._users`, `UserForest` locks, `TreeStore._trees`, FAISS indexes, summary caches, and query object graphs are local-runtime conveniences that need durable/cloud-backed equivalents before horizontal scale.
- For enterprise Azure designs, prefer Cosmos DB for mutable source-of-truth state, Azure AI Search for managed recall, Blob/ADLS for snapshots and cold artifacts, Redis for hot cache only, and Azure AI Foundry/Azure OpenAI for governed model deployments.
- BU/union recall needs a durable fact-location index (`fact_id -> tree/node locations`) instead of scanning all trees at query time.
- Online tree storage should support random access and incremental updates. Avoid using monolithic tree JSON blobs as the only active mutable representation for large users; use Blob snapshots for archive/restore.
- Any production design must include tenant identity, authorization, write leases/idempotency, deletion/retention, cache invalidation, model versioning, re-embedding migration, redacted telemetry, and explainability traces.

## Working notes

- External model endpoints are required for LLM extraction, summary generation, browsing, and dedupe. The default config files are the first place to check when changing model routing.
- The `README.md` shows the main public API (`MemForest`, `register_user`, `ingest_session`, `query`, `save`) and the expected runtime layout. Use those patterns when writing examples or debugging integration flows.
