# Production readiness plan

This document records the remaining production workstreams after the Azure-MemForest dev environment reached live ingest/query smoke success.

## Operational dashboards and alerts

Minimum dashboard panels:

- API availability: `/health/live`, `/health/ready`, request count, p50/p95/p99 latency, 4xx/5xx by route.
- Worker health: active revision, message processing outcomes, transient/permanent failures, lease conflicts, lock-renewal failures.
- Service Bus: active message count, oldest message age, DLQ count, abandon/retry count.
- Cosmos DB: 429/5xx counts, RU consumption, per-container storage, hot partition indicators.
- Azure AI Search: query failures, indexing failures, latency, document counts by `forest_version`.
- Storage: snapshot writes/read failures, capacity, stale snapshot count.
- Azure OpenAI: request rate, throttling, latency, token usage, deployment capacity.
- Cost: daily spend by Cosmos, Search, Storage, Service Bus, Container Apps, Azure OpenAI.

Minimum alerts:

- API readiness failing for 5 minutes.
- Worker revision not running.
- Service Bus DLQ count greater than zero.
- Service Bus oldest active message age above 10 minutes.
- Repeated worker permanent failures.
- Cosmos 429 rate above agreed threshold.
- Azure OpenAI throttling above agreed threshold.
- Search indexing/query failures above agreed threshold.

## Compaction and erasure workflow

Foreground writes never hard-delete old committed projections. A separate compaction/erasure workflow must:

- retain the latest committed manifest and an approved number of previous generations;
- delete failed/pending orphan projections after audit retention;
- delete superseded tree nodes, fact locations, browse embeddings, recall documents, and snapshots;
- remove tombstoned source sessions/turns/facts according to tenant retention;
- remove related query traces and cache entries on erasure requests;
- verify Search, Cosmos, Blob, and cache state converge before marking erasure complete;
- emit an audit record containing identifiers, counts, timestamps, and operator/job identity, without raw memory payloads.

## Network and key security

Production promotion must decide and document:

- private endpoint or public network posture for Cosmos, Search, Storage, Service Bus, Container Apps, and Azure OpenAI;
- customer-managed key requirements and key rotation ownership;
- separate managed identities for API and worker;
- least-privilege role assignments for API, worker, deployment automation, and operators;
- allowed inbound origins and API authentication policy;
- redacted telemetry and trace retention.

## CI/CD promotion and rollback

Production promotion should follow:

1. Build from a clean, pushed commit.
2. Use the cached ACR base image.
3. Publish immutable image tag and digest.
4. Deploy to dev with worker enabled and Entra auth.
5. Run smoke gates: readiness, worker queue/DLQ, two-ingest hydration, query answer, trace read, bearer-token auth.
6. Promote the same digest to the next environment.
7. Keep previous revision available for rollback.
8. Roll back on failed readiness, DLQ growth, manifest commit failures, or query smoke failure.

## Load, backup, and DR runbooks

Before production claims:

- run load tests covering ingest burst, steady query, mixed ingest/query, and delete traffic;
- define capacity targets for p95/p99 ingest latency, query latency, queue drain time, and cost per user/session;
- verify Blob snapshot restore into a fresh worker scratch directory;
- verify Cosmos point-in-time restore or backup policy;
- define Search index rebuild from committed manifests/projections;
- define re-embedding migration for embedding model changes;
- document regional recovery expectations and manual failover steps.
