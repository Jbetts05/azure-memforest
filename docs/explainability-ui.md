# Explainability UI and trace architecture

Azure-MemForest must make retrieval explainable without turning telemetry into a data leak.

## Goals

- Show real recall, planning, browsing, reranking, cache, and Azure service-call behavior.
- Explain why trees/facts were selected or rejected.
- Correlate UI traces with service logs and Azure Monitor telemetry.
- Avoid raw prompt, response, or memory payload logging by default.

## Trace storage model

Trace persistence should not write one Cosmos document per node-level event. Browse and recall can emit hundreds or thousands of events per query, so the persistent shape should be bounded:

- one trace metadata document per query;
- phase-level event buckets for high-cardinality phases;
- explicit per-query event budget;
- explicit `trace_truncated` marker when the budget is exceeded.

Persisted trace records must include:

- `schema_version`;
- `tenant_id`;
- `user_id`;
- `query_id`;
- `forest_version`;
- event type and timestamp;
- safe typed attributes only.

`tenant_id` and `user_id` are required for persisted traces, even though in-process `TraceEvent` can remain optional while local code paths are still being wired.

## Pinned trace vocabulary

Trace events use `src.observability.tracing.TraceEvent`. `schema_version` describes the event shape; `TRACE_EVENT_CATALOG_VERSION` describes the event vocabulary. Adding a new event type must be treated as a catalog change and documented here before producers emit it.

Worker/materialization events:

| Event | Required safe attributes |
| --- | --- |
| `worker_job_received` | `job_id`, `job_type`, `session_id`, `delivery_count` |
| `worker_job_claimed` | `job_id`, `owner_id`, `fencing_token`, `attempt_count` |
| `worker_job_retried` | `job_id`, `attempt_count`, `reason_code` |
| `worker_job_completed` | `job_id`, `forest_version`, `manifest_id`, `generation`, `duration_ms` |
| `worker_job_failed` | `job_id`, `error_code`, `error_class`, `retryable` |
| `worker_lease_acquired` | `job_id`, `lease_id`, `fencing_token`, `ttl_seconds` |
| `worker_lease_renewed` | `job_id`, `lease_id`, `ttl_seconds` |
| `worker_lease_lost` | `job_id`, `lease_id`, `reason_code` |
| `materialization_manifest_pending` | `job_id`, `manifest_id`, `forest_version`, `generation` |
| `materialization_source_written` | `job_id`, `session_id`, `turn_count`, `cell_count` |
| `materialization_facts_written` | `job_id`, `fact_count`, `occurrence_count`, `duplicate_count` |
| `materialization_trees_written` | `job_id`, `forest_version`, `tree_count`, `node_count`, `tree_types` |
| `materialization_search_projected` | `job_id`, `forest_version`, `document_count`, `embedding_model_version` |
| `materialization_snapshot_written` | `job_id`, `snapshot_id`, `forest_version`, `byte_count` |
| `materialization_manifest_committed` | `job_id`, `manifest_id`, `forest_version`, `generation` |
| `materialization_step_failed` | `job_id`, `step_name`, `error_code`, `error_class`, `retryable` |
| `materialization_aborted` | `job_id`, `manifest_id`, `forest_version`, `reason_code` |
| `summary_cache_checked` | `cache_key_hash`, `cache_hit`, `summary_model_version` |
| `summary_cache_written` | `cache_key_hash`, `summary_model_version`, `byte_count` |

Query events:

| Event family | Required safe attributes |
| --- | --- |
| `query_started`, `query_completed` | `query_id`, `forest_version`, `query_config_hash`, `request_id`, `trace_id` when available |
| `embedding_requested` | `model_deployment`, `embedding_model_version`, `input_count`, `duration_ms` |
| `recall_search_started`, `recall_candidate_returned`, `recall_tree_selected` | `recall_strategy`, `candidate_rank`, `tree_type`, `forest_version`, `embedding_model_version`, `score` |
| `planner_result` | `planner_model_version`, `tree_count`, `subquery_count`, `query_config_hash` |
| `browse_tree_started`, `browse_level_expanded`, `browse_node_scored`, `browse_node_selected`, `browse_node_pruned` | `tree_type`, `browse_depth`, `candidate_rank`, `score`, `browse_model_version` |
| `leaf_ids_returned`, `facts_loaded` | `leaf_count`, `fact_count`, `forest_version` |
| `facts_reranked` | `rerank_model_version`, `candidate_count`, `selected_count` |
| `answer_generated` | `answer_model_version`, `fact_count`, `duration_ms` |
| `service_call`, `service_call_failed` | `service_name`, `operation`, `duration_ms`, `error_code`, `error_class` for failures |

The per-field model/version attributes are convenience fields. Reproducibility depends on `query_config_hash` and, for worker materialization, the committed `forest_version` and manifest metadata that reference the complete configuration snapshot.

`cache_key_hash` must be an HMAC or tenant-salted digest, never a bare hash of query text, prompts, facts, summaries, or turn content. Opaque identifiers such as `fact_id`, `tree_id`, and `node_id` must not be reversible hashes of raw memory content.

Worker traces and query traces share the same safe event type, but worker events must use a namespaced trace identifier such as `job:<job_id>` until a dedicated job trace record shape exists. The storage layer must distinguish query and job traces before worker events are flushed to Cosmos.

Forbidden attributes are enforced by the Cosmos trace recorder and must not be persisted even when short:

- raw user query, prompts, model responses, requests, responses, turn content, fact text, summaries, or embeddings;
- access tokens, API keys, authorization headers, passwords, or secrets;
- exception messages, exception args, stack traces, or raw SDK response bodies.

Allowed failure diagnostics are enum-like `error_code`, `error_class`, `reason_code`, `retryable`, and numeric counts/durations only.

## Authorization

Trace APIs must be tenant and user scoped:

```text
GET /v1/users/{user_id}/queries/{query_id}/trace
```

The route must:

- require `ActorContext`;
- enforce path user access;
- filter by `actor.tenant.tenant_id`;
- verify the trace belongs to the requested user and tenant;
- return not-found semantics for cross-tenant or cross-user misses.

## Privacy and payload rules

Free-form trace attributes are a potential PII escape hatch. Persistent sinks must enforce:

- event-type-specific allowlists or a redaction layer;
- size caps for string attributes;
- no raw prompts/responses by default;
- no raw memory facts unless explicitly approved by tenant policy;
- Blob payload capture only when explicitly enabled, encrypted, and retention-limited.

## Production sink behavior

Remote sinks must not block query serving on every event. The production pattern should be:

- in-process bounded queue;
- background flusher;
- drop-oldest or truncate policy under backpressure;
- explicit trace event for dropped/truncated data;
- retries with bounded backoff;
- safe failure logging.

## OpenTelemetry alignment

Explainability traces are domain traces. They should be correlated with OpenTelemetry/Application Insights rather than replacing them.

Each query should have:

- service request ID;
- W3C trace ID where available;
- query ID;
- optional OpenTelemetry span with `query_id` as an attribute.

## UI shape

The UI should be built after the trace API exists. It should show:

- live or replayed query timeline;
- recall candidates and selected/rejected trees;
- browse plans and per-tree subqueries without raw prompt leakage;
- beam/frontier evolution from bucketed events;
- facts loaded/reranked;
- service-call timings;
- cache hits/misses;
- index and model versions;
- trace truncation indicators.

Live streaming with SSE or WebSockets is useful for long-running queries, but the first version can be post-hoc through the trace API.

## Azure resources

No new Azure resource is required before the trace sink is implemented. Use:

- Cosmos DB for trace metadata and event buckets with TTL;
- Blob Storage only for explicitly enabled large payload artifacts;
- Application Insights/OpenTelemetry for infrastructure/service traces;
- optional Azure AI Search trace summary index later for finding problematic query patterns.
