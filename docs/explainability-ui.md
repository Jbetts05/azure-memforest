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
