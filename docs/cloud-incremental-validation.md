# P1 incremental cloud-state validation

Use this local harness to validate P1 incremental cloud-state changes without
running live benchmarks or model calls.

Run:

```powershell
.\.venv\Scripts\python.exe -m src.evaluation.incremental_cloud_validation path\to\fixture.json
```

Fixture shape:

```json
{
  "projection": {
    "baseline_full": {
      "stored_nodes": 12,
      "fact_locations": 8,
      "recall_documents": 4,
      "browse_embeddings": 12,
      "summary_cache_entries": 6
    },
    "candidate_incremental": {
      "stored_nodes": 3,
      "fact_locations": 2,
      "recall_documents": 1,
      "browse_embeddings": 3,
      "summary_cache_entries": 1
    },
    "min_total_reduction_ratio": 0.5
  },
  "query_cases": [
    {
      "case_id": "smoke-1",
      "baseline": {
        "answer": "Alice prefers tea.",
        "retrieved_fact_ids": ["fact-1"],
        "recalled_tree_ids": ["entity:alice"]
      },
      "candidate": {
        "answer": "alice prefers tea.",
        "retrieved_fact_ids": ["fact-1"],
        "recalled_tree_ids": ["entity:alice"]
      }
    }
  ]
}
```

The gate passes only when candidate projection writes are lower than the full
projection by the configured ratio, normalized answers match, and retrieval IDs
match in order.

## Current dev validation posture

As of the P1 cloud-state validation run, the dev worker has been exercised with
`AZURE_MEMFOREST_SNAPSHOT_POLICY=cloud_checkpoint` and
`AZURE_MEMFOREST_CLOUD_CHECKPOINT_HYDRATION_SUPPORTED=True`.

Validated at code/dev-smoke level:

- snapshotless cloud checkpoint writes commit successfully;
- a second generation can hydrate from the prior snapshotless cloud state;
- tree-node memberships, fact locations, recall documents, browse embeddings,
  and summary cache entries use committed baselines for dirty-path writes;
- delete-session after cloud hydration removes orphaned derived facts in the
  validation path;
- snapshot-always and cloud-checkpoint A/B smoke returned matching retrieved
  fact text for identical two-generation inputs;
- audited GC dry-run planning can identify deletable tree bodies and closed
  MVCC projection records without live deletion.

This is not a full benchmark or production-readiness claim. Full LongMemEval
parity, load testing, stable production auth, DLQ hygiene, and live GC
procedures remain separate gates.

## Cloud checkpoint rollback

To return the dev worker to the conservative snapshot path:

```powershell
az containerapp update `
  -g azure-memforest `
  -n memforest-example-worker `
  --set-env-vars `
    AZURE_MEMFOREST_SNAPSHOT_POLICY=always `
    AZURE_MEMFOREST_CLOUD_CHECKPOINT_HYDRATION_SUPPORTED=False
```

Do not toggle API dev auth repeatedly during validation. Prefer Entra/bearer
auth or a stable smoke harness; if dev header auth is used, enable it once,
wait for readiness, run the whole smoke, and revert it once.

## Live GC gate

GC is still safe-by-default. Dry-run planning is validated, but live deletion
must remain disabled unless all of these are true:

- the operator supplies an enabled policy with a positive retention count;
- the plan contains at least one retained forest version;
- retained membership body references were read for every retained manifest;
- both body and MVCC record deletions have audit callbacks;
- a live-delete flag is explicitly set at execution time;
- the resulting report is reviewed before enabling recurring execution.
