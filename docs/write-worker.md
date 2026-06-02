# Write worker design

The write worker consumes ordered memory mutation jobs and materializes them into durable Azure stores. It must preserve MemForest paper behavior while making writes safe under horizontal scale.

For the current migration from Service Bus worker-owned execution to Durable Functions orchestration, see [Service Bus to Durable Functions migration](durable-orchestration-migration.md).

## Current state

The API records accepted `WriteJobEnvelope` metadata in the Cosmos `jobs` container. Service Bus remains the safe default write dispatch path and carries `delete_session` / `clear_memory` fallback traffic. Durable Functions is now validated for `ingest_session` in guarded windows; see the migration document linked above.

A worker Container App is deployed in dev for Service Bus-backed jobs, but Durable ingest no longer depends on Service Bus message locks for long-running memory materialization. Do not deploy stub workers against the real queue; a stub would consume sessions, burn delivery counts, pollute the DLQ, and mark real jobs failed before the real materialization path exists.

## Queue and ordering

Service Bus `session_id` is:

```text
tenant_id:user_id
```

This gives FIFO ordering per tenant/user. Cross-user ordering is not guaranteed and must not be assumed.

Worker instances must:

- accept one Service Bus session at a time per active receiver;
- renew the session lock for long-running jobs;
- bound concurrent sessions per instance;
- abandon/retry transient failures;
- dead-letter schema/validation failures.

The current queue uses:

- `requiresSession = true`
- `lockDuration = PT2M`
- `maxDeliveryCount = 5`
- `deadLetteringOnMessageExpiration = true`

Long ingest jobs will need lock renewal because extraction, summarization, embeddings, and projection writes can exceed the initial lock duration.

## Job envelope contract

Workers consume `WriteJobEnvelope` with:

- `schema_version`
- `job_id`
- `job_type`
- `tenant_id`
- `user_id`
- `session_id`
- `idempotency_key`
- `submitted_at_unix`
- `payload`

Unknown `schema_version` must fail closed and dead-letter or park the message for migration; it must not be processed best-effort.

`job_id` is deterministic from tenant, user, session, job type, and idempotency key. The worker must treat `job_id` as the idempotency boundary and must not double-apply a job that already reached `running`, `succeeded`, or a terminal failure state.

Terminal redeliveries are completed without taking a user write lease or touching memory state. For non-terminal jobs, the worker checks job state first, acquires the per-user lease, then claims the job with an ETag/fencing condition. If lease acquisition fails before claim, the message is abandoned without changing job state.

## Lease and fencing

Before mutating memory state, the worker must acquire a per-tenant/user write lease. The lease must include a fencing token.

Every source-of-truth write must verify the current fencing token or equivalent ETag condition. A distributed lock without fencing is insufficient: a zombie worker whose lease expired must not be able to commit stale writes.

Service Bus sessions and write leases serve different purposes. The Service Bus session preserves FIFO delivery per `tenant_id:user_id`; the fenced lease protects against redelivery, lock loss, zombie workers, and multiple worker hosts mutating the same user concurrently.

The handler pattern is:

1. acquire the tenant/user lease before deriving the next generation;
2. start lease renewal before calling the synchronous MemForest core;
3. treat `LeaseConflict` as retryable and abandon the Service Bus message;
4. verify the current owner/fencing token immediately before manifest commit;
5. release the lease after job success or terminal failure.

Projection writes are tagged with the pending `forest_version` and remain invisible until manifest commit. The final manifest commit must be guarded by `assert_current(lease)` so an expired/zombie worker cannot publish stale projections as the latest committed version.

## Artifact graph and version-scoping model

The worker owns the full cloud materialization graph for each tenant/user:

- source records: sessions, turns, cells, canonical facts, fact occurrences, deletion records;
- derived records: session/entity/scene tree nodes, fact locations, browse embeddings, summary-cache entries, recall documents, snapshots, and manifests;
- explainability records: worker-stage trace events, model/version metadata, projection counts, and safe error codes.

Source records are tenant/user scoped and keyed by deterministic IDs. Reprocessing the same job must converge to the same source record IDs and content, so partial source writes from a failed attempt are safe to overwrite or skip on redelivery. Source records are not directly query-visible without a committed manifest.

Every derived record must carry the `forest_version` that produced it. Cloud query readers must:

1. read the latest committed manifest for the tenant/user;
2. filter Cosmos tree nodes, fact locations, browse embeddings, and Azure AI Search recall documents by that manifest's `forest_version`;
3. ignore all pending, failed, or orphaned versions.

The first worker version uses a full-version projection model: each committed generation re-emits every still-valid derived projection for the active forest version. This is intentionally storage-heavier than range-based validity, but it makes the manifest the only visibility boundary and keeps cloud parity easier to reason about. A future compaction job may remove unreferenced older versions after retention rules allow it.

## Ingest session flow

For `ingest_session`:

1. Validate envelope schema and tenant/user/session identity.
2. Complete terminal redeliveries without mutation.
3. Acquire write lease with fencing token.
4. Mark job `running` with ETag/conditional update.
5. Read the latest committed manifest and mark any stale pending manifests as failed.
6. Create a new `pending` manifest for the next generation and `forest_version`.
7. Upsert source session and turn records with deterministic IDs.
8. Run extraction, dedupe, tree building, summary generation, and embeddings through the synchronous MemForest core in a bounded scratch workspace.
9. Persist canonical facts and occurrence metadata.
10. Persist session/entity/scene tree nodes tagged with the pending `forest_version`.
11. Persist fact-location records tagged with the pending `forest_version`.
12. Persist browse embeddings and summary-cache artifacts tagged with model and forest versions.
13. Upsert Azure AI Search recall documents tagged with the pending `forest_version`.
14. Write Blob snapshots if enabled.
15. Emit non-terminal trace events for materialization counts, model versions, and projection stages.
16. Commit the manifest.
17. Emit terminal success trace/audit event.
18. Mark job `succeeded`.
19. Release lease.

The manifest commit is the observable transaction boundary. Query readers must use the latest committed manifest version and must not read partial writes produced before the commit.

## Delete session flow

For `delete_session`:

1. Validate envelope.
2. Complete terminal redeliveries without mutation.
3. Acquire write lease with fencing token.
4. Mark job `running`.
5. Read the latest committed manifest and create a new `pending` manifest for the next generation.
6. Tombstone session/turn/cell/fact occurrences in Cosmos.
7. Rebuild and write the still-valid derived tree nodes, fact locations, browse embeddings, summaries, and recall documents under the pending `forest_version`.
8. Record the deletion without raw memory payloads.
9. Write a deletion snapshot if enabled.
10. Commit the manifest.
11. Emit terminal deletion trace/audit event.
12. Mark job `succeeded`.

Hard delete/erasure is a separate compaction workflow and must cover source records, projections, snapshots, traces, and caches according to retention policy.

The first worker must not hard-delete recall documents or tree nodes that belong to the previous committed `forest_version` before the new deletion manifest commits. Queries only read the latest committed manifest, and historical manifest pinning is not supported in the first cloud query path. Old-version projections are removed later by compaction, not by the foreground delete handler.

## Failure policy

- Cosmos 429/503, model throttling, Search transient failures: retry with bounded backoff.
- Unknown schema version, malformed payload, tenant mismatch: dead-letter.
- Model content-filter failures: mark job failed with safe error code; do not log raw payload.
- Lease acquisition timeout: abandon/retry.
- Lease fencing failure: abandon/retry or fail safe depending on state.

DLQ runbooks must support inspection and replay without exposing raw memory payloads in logs.

If a worker crashes after creating a pending manifest, the next holder of the user lease ignores that manifest for query purposes. On startup for a new job, the worker scans pending manifests for the tenant/user:

- if a pending manifest belongs to the same `job_id`, the worker may resume by idempotently re-upserting the same versioned artifacts;
- if it belongs to a different stale job, the worker marks it `failed` when possible and continues from the latest committed generation;
- partial Cosmos/Search/Blob writes for pending or failed versions remain invisible because every derived record is filtered by committed `forest_version`;
- a later compaction job deletes orphaned pending/failed derived artifacts after retention and audit windows expire.

Terminal trace/audit events are emitted only after manifest commit. If manifest commit fails after projection writes, the worker emits a failed/pending trace and abandons or fails the job according to retry policy; it must not emit a success event.

## Handler acceptance tests

The first materialization implementation must include fake-adapter tests for:

- crash between Search upsert and manifest commit: cloud query prerequisites do not expose pending results;
- crash after pending manifest creation but before derived writes: the next job marks or ignores the orphan and proceeds from the latest committed manifest;
- redelivery of a job whose manifest already committed: the message completes without creating a second visible version or duplicate Search documents;
- strict tenant/user and `forest_version` filtering for tree nodes, fact locations, browse embeddings, and recall documents;
- delete followed by ingest for the same session or facts: tombstoned source records and stale Search projections do not resurrect;
- partial cross-partition writes: deterministic IDs and idempotent upserts converge on retry.

## Non-goals for first worker

- No cross-tenant batching.
- No streaming ingest.
- No model fine-tuning.
- No production Redis dependency before durable stores work.
- No best-effort writes without manifest commit.

## Implementation prerequisites

Do not implement the production worker until these are ready:

- Cosmos source adapters for jobs, sessions, turns, facts, tree nodes, fact locations, manifests, and deletions.
- Azure OpenAI/Foundry model client configuration and deployments.
- Azure AI Search recall index bootstrap and adapter.
- Blob snapshot adapter.
- LeaseManager implementation with fencing.
- Contract tests for all store/index adapters.

## Hosting plan

The first hosted worker runs as a second Azure Container App in the same managed environment as the API. It uses the same container image with a worker entrypoint, has no external ingress, and is gated by an explicit deployment parameter that defaults off. Do not create a live receiver while the only available handler is a stub; a stub worker would consume messages, increment delivery counts, and pollute the DLQ.

Initial dev shape:

- Container App name: `${appName}-${environmentName}-worker`.
- Ingress: disabled.
- Active revisions: `Single`.
- Replicas: `minReplicas=1`, `maxReplicas=1` for the first end-to-end validation only.
- Identity: the existing user-assigned managed identity may be reused in dev because it already has ACR pull, Cosmos DB data contributor, Search service/index data contributor, Storage Blob Data Contributor, Service Bus sender/receiver, and Azure OpenAI user roles.
- Environment: mirror the API's Azure storage/model/search/cosmos settings and add worker settings:
  - `AZURE_MEMFOREST_WORKER_ENABLED=true`;
  - `AZURE_MEMFOREST_WORKER_CONCURRENCY`;
  - `AZURE_MEMFOREST_WORKER_SESSION_LOCK_RENEWAL_SECONDS`;
  - `AZURE_MEMFOREST_WORKER_MESSAGE_WAIT_SECONDS`;
  - `AZURE_MEMFOREST_WORKER_SCRATCH_ROOT`.

Ordering is not guaranteed by replica count. The receive loop must use Service Bus session receivers and process one in-flight message at a time for each acquired session lock. Multiple sessions may be processed concurrently after correctness is proven because Service Bus allows only one receiver to own a given session lock.

The queue lock duration is currently `PT2M`. The worker lock-renewal interval must be less than half of the queue lock duration, and lock renewal must run while the synchronous materialization core executes. Deploy validation must prove that long extraction, summary, embedding, Search, and Blob stages do not block renewal.

Production identity split:

- API identity: ACR pull, Service Bus sender, and only the read permissions required for readiness checks.
- Worker identity: ACR pull, Service Bus receiver, Cosmos DB data contributor, Search index data contributor, Storage Blob Data Contributor, and Azure OpenAI user.

Create the worker identity and role assignments in IaC before production even if the dev deployment temporarily reuses the API identity.

Scaling plan:

- First validation uses an always-on worker to avoid unverified scaler behavior.
- After correctness, validate Container Apps/KEDA Service Bus scaling with managed identity and `requiresSession=true` queues before enabling scale-to-zero.
- If session-aware managed-identity scaling is not reliable in the target API version, prefer always-on N replicas using session receivers. Container Apps Jobs are a later option only after managed-identity Service Bus trigger behavior is verified.

Observability and rollout requirements:

- Structured logs for every message include safe `session_id`, `message_id`, `delivery_count`, `job_id`, outcome, and duration; raw memory payloads are never logged.
- Log Analytics is attached through the shared Container Apps environment.
- Alerts track DLQ count, abandon/retry count, and repeated transient handler failures.
- Single-revision deployments may terminate an in-flight session lock; idempotent handlers and lock-loss recovery must tolerate the extra delivery attempt.
- `WORKER_SCRATCH_ROOT` is ephemeral. The worker must fail fast when scratch usage exceeds the configured budget and must delete scratch directories on success and failure.
- DLQ inspection and replay tooling must avoid raw memory payload logging.

## Async boundary decision

The first production worker will use an async shell around the existing synchronous MemForest core.

Why:

- Service Bus, Cosmos, Search, and Blob SDK operations are async.
- Existing extraction, fact management, tree build, summary, and indexing code is synchronous and represents the paper behavior we need to preserve.
- A full async rewrite before parity measurement would add large regression risk.

Worker shape:

- receive Service Bus messages and manage session locks on the event loop;
- acquire leases/fencing tokens through async cloud stores;
- run extraction/tree-building/model-call-heavy sync core in a bounded worker thread;
- write source records and projections through async cloud adapters;
- commit manifest as the visibility boundary;
- complete or abandon/dead-letter the Service Bus message.

### Required safeguards

- Start lock renewal before entering the sync core.
- Bound concurrent Service Bus sessions and bound sync worker threads separately.
- Add cancellation checkpoints between extraction, tree build, projection writes, and manifest commit.
- Use fencing tokens or ETags on every source-of-truth mutation.
- Treat local FAISS as transient per-job scratch only:
  - never read FAISS files at worker startup;
  - never share FAISS across jobs as source of truth;
  - delete scratch working directories on success and failure;
  - Azure AI Search and Cosmos are authoritative.
- Construct model clients once and reuse them; do not create model clients per call.
- Plan an async model-client follow-up after parity is achieved.
