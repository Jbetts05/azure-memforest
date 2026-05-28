# Cloud query composition gate

Cloud query composition is the point where Azure-backed stores, search, model clients, and trace persistence replace local research-runtime conveniences. It must not be enabled until the prerequisites below are satisfied.

## Do not enable cloud query until

- Cosmos source-of-truth adapters exist for facts, sessions, turns, tree nodes, fact locations, manifests, jobs, and deletions.
- Azure AI Search recall index exists with vector dimensions from the configured embedding deployment.
- Browse embedding store exists and is populated for non-leaf tree nodes.
- Model clients are configured for planning, browsing, answering, and embeddings.
- Persistent trace sink is attached.
- At least one committed forest manifest exists for the tenant/user being queried.
- Benchmark parity gate is defined and runnable for cloud-mode outputs.

The runtime helper `verify_cloud_query_prerequisites` must fail closed until these are true. Route code should return a 503-style cloud-query-not-ready response rather than silently falling back to empty results.

## Required sequencing

1. Configure model deployment names, embedding model version, and embedding dimensions.
2. Bootstrap or validate Azure AI Search index with the configured embedding dimension.
3. Materialize facts, tree nodes, browse embeddings, fact locations, and manifests from write workers.
4. Commit manifest version only after all source records and projections for that version are durable.
5. Enable cloud query route behind the prerequisite gate.
6. Run cloud-mode benchmark replay.
7. Compare against `benchmark/baseline_summary.json` with `src.evaluation.benchmark_diff`.

## Manifest commit protocol

Workers must write a manifest as `pending` while materializing source records and projections. Only after facts, tree nodes, fact locations, browse embeddings, summaries, and recall documents are durable may the worker atomically mark the manifest `committed`.

Query readers must:

- filter by tenant and user;
- read only `status=committed` manifests;
- choose the latest generation/version;
- use that manifest version to filter source records and recall documents.

Partial writes before manifest commit must be invisible to query readers.

## Search index and embedding version gate

Azure AI Search vector dimensions are immutable for an index field. `ensure_index` must read dimensions from configuration, not hardcode them. Changing embedding model or dimension requires:

- new index or full index rebuild;
- re-embedding affected records;
- re-materializing recall documents;
- updating manifest/search version metadata;
- passing parity diff again.

## Benchmark gate

The frozen baseline is `benchmark/baseline_summary.json`. Cloud-mode benchmark outputs must be converted to the same CSV shape and compared with:

```powershell
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m src.evaluation.benchmark_diff --baseline-json benchmark\baseline_summary.json --candidate-dir <cloud-output-dir> --k 1 --k 8 --tolerance <approved-tolerance>
```

Tolerance must be approved before local behavior is replaced. Until then, use exact or near-exact tolerance only for regression checks.

## Explicitly blocked

- Do not register or enable a cloud query route that can return empty results because stores/indexes are missing.
- Do not create an AI Search index with a hardcoded or guessed vector dimension.
- Do not commit manifests before all projections for that version are durable.
- Do not bypass tenant filters in Cosmos or Azure AI Search.
- Do not treat cloud query as complete until parity diff passes.
