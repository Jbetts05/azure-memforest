# Production readiness plan

This document records the remaining production workstreams after the Azure-MemForest dev environment reached live ingest/query smoke success.

## P1 cloud-state status

P1 incremental cloud state has been achieved at code/dev-smoke level. The dev
worker has run with cloud checkpoint enabled and validated:

- cloud-state hydration from snapshotless committed manifests;
- generation-range MVCC for tree memberships, fact locations, and Azure AI
  Search recall documents;
- exact fact-vector replay during cloud hydration;
- durable summary-cache loading with cache misses treated as a rebuild
  optimization rather than a correctness blocker;
- delete-session orphan propagation after cloud hydration;
- targeted A/B smoke between snapshot-always and cloud-checkpoint paths;
- audited dry-run GC planning for tree bodies and closed MVCC records.

This does **not** mean production is ready. Before production promotion, complete
the gates below: live GC runbook/approval, stable Entra auth, DLQ hygiene,
broader benchmark parity, and load/soak validation.

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

Current state: GC planning and audited dry-run execution exist, including
record-level MVCC delete planning and body-delete membership coverage checks.
Live deletion must stay disabled until an operator-approved runbook wires the
delete callbacks and reviews the dry-run report for the target tenant/window.

## Network and key security

Production promotion must decide and document:

- private endpoint or public network posture for Cosmos, Search, Storage, Service Bus, Container Apps, and Azure OpenAI;
- customer-managed key requirements and key rotation ownership;
- separate managed identities for API and worker;
- least-privilege role assignments for API, worker, deployment automation, and operators;
- allowed inbound origins and API authentication policy;
- redacted telemetry and trace retention.

## Stable authentication gate

Dev header auth is not a benchmark or production control. Promotion requires:

- Entra JWT validation configured with non-empty tenant and audience values;
- smoke tests using bearer tokens, not `AZURE_MEMFOREST_DEV_AUTH`;
- no repeated Container App revision flips for auth during validation;
- route tests proving unauthenticated memory access fails closed;
- audit evidence that API dev auth is `False` after smoke completion.

## Live GC approval gate

Live projection GC must remain off until all of the following are true:

- a dry-run report for the exact tenant/window has been reviewed;
- the plan includes retained forest versions and a positive retention count;
- retained tree membership body references are confirmed complete for every retained manifest;
- both body and MVCC record delete callbacks are wired through audited execution;
- the operator explicitly sets the live-delete execution flag;
- rollback/restore expectations are documented for the affected tenant.

Until this gate passes, only dry-run GC planning is approved.

## Monitoring additions for P1 cloud checkpoint

Add panels/alerts for:

- cloud-hydration failures by reason (`missing_fact_ids`, missing node embeddings, SDK/read errors);
- snapshot policy distribution (`always` vs `cloud_checkpoint`) by worker revision;
- MVCC row growth by container and Search document growth over time;
- projection plan counts (`unchanged`, `upsert`, `delete`, `superseded`) by surface;
- retained/dropped generation counts from GC dry-run reports;
- DLQ count and oldest DLQ message age after every validation run.

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

For cloud-checkpoint rollback in dev, set the worker environment back to:

```text
AZURE_MEMFOREST_SNAPSHOT_POLICY=always
AZURE_MEMFOREST_CLOUD_CHECKPOINT_HYDRATION_SUPPORTED=False
```

See `docs/cloud-incremental-validation.md` for the concrete dev rollback
command.

API dev header auth is for short-lived dev smoke only. Repeated revision
toggling caused readiness instability during validation; production promotion
must use stable Entra auth rather than dev-auth revision flips.

## Load, backup, and DR runbooks

Before production claims:

- run load tests covering ingest burst, steady query, mixed ingest/query, and delete traffic;
- define capacity targets for p95/p99 ingest latency, query latency, queue drain time, and cost per user/session;
- verify Blob snapshot restore into a fresh worker scratch directory;
- verify Cosmos point-in-time restore or backup policy;
- define Search index rebuild from committed manifests/projections;
- define re-embedding migration for embedding model changes;
- document regional recovery expectations and manual failover steps.
