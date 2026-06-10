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
