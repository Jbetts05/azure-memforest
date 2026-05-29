# UI seed dataset

The UI should be developed against a realistic committed memory forest, not mocked tree shapes.

## Existing dev smoke seed

The dev environment currently includes a smoke tenant/user created through the live worker:

- tenant: `smoke-tenant`
- user: `smoke-user`
- sessions:
  - `smoke-session-1`: “Alice moved to Seattle in 2024.”
  - `smoke-session-2`: “Alice adopted a cat named Pixel in 2025.”

The latest committed forest contains two facts, committed manifests for generation 1 and 2, tree nodes, fact locations, browse embeddings, Search recall documents, and query traces.

## Expected UI checks

A UI seed verification should prove:

1. `/forest/latest` returns a committed manifest.
2. `/sessions` returns at least the two smoke sessions.
3. `/sessions/{session_id}/turns` returns committed source turns.
4. `/forest/{forest_version}/trees` returns roots across tree families.
5. `/query` for “Where did Alice move?” returns a grounded answer and fact.
6. `/queries/{query_id}/trace` returns a trace with recall, facts, rerank, answer, and completion events.

## Creating a new seed

Use the normal API/worker path:

1. Submit an ingest job with `Idempotency-Key`.
2. Poll `/jobs/{job_id}` until `succeeded`.
3. Repeat for at least a second session for the same user.
4. Query and read back the trace.

Do not seed data by writing Cosmos documents directly. The UI seed must exercise the same materialization path that production users use.

