# Write worker design

The write worker consumes ordered memory mutation jobs and materializes them into durable Azure stores. It must preserve MemForest paper behavior while making writes safe under horizontal scale.

## Current state

The API publishes `WriteJobEnvelope` messages to the session-enabled Service Bus queue `memforest-writes` and records accepted job metadata in the Cosmos `jobs` container.

No worker is deployed yet. Do not run a stub worker against the real queue. A stub would consume sessions, burn delivery counts, pollute the DLQ, and mark real jobs failed before the real materialization path exists.

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

## Lease and fencing

Before mutating memory state, the worker must acquire a per-tenant/user write lease. The lease must include a fencing token.

Every source-of-truth write must verify the current fencing token or equivalent ETag condition. A distributed lock without fencing is insufficient: a zombie worker whose lease expired must not be able to commit stale writes.

## Ingest session flow

For `ingest_session`:

1. Validate envelope schema and tenant/user/session identity.
2. Acquire write lease with fencing token.
3. Mark job `running` with ETag/conditional update.
4. Extract cells and canonical facts using configured model clients.
5. Write sessions, turns, cells, facts, occurrences, and duplicate decisions to Cosmos.
6. Build or update session/entity/scene tree nodes.
7. Generate summaries and embeddings.
8. Maintain fact-location records.
9. Update Azure AI Search recall documents.
10. Write Blob snapshots if enabled.
11. Commit a new forest manifest version.
12. Mark job `succeeded`.
13. Release lease.

The manifest commit is the observable transaction boundary. Query readers must use the latest committed manifest version and must not read partial writes produced before the commit.

## Delete session flow

For `delete_session`:

1. Validate envelope.
2. Acquire write lease with fencing token.
3. Mark job `running`.
4. Tombstone session/turn/cell/fact occurrences in Cosmos.
5. Exclude tombstoned records from future reads immediately.
6. Remove or tombstone affected tree nodes and fact-location records.
7. Remove Azure AI Search recall docs for affected trees/facts.
8. Invalidate cache entries when Redis is introduced.
9. Emit deletion audit record without raw memory payloads.
10. Commit a new manifest version.
11. Mark job `succeeded`.

Hard delete/erasure is a separate compaction workflow and must cover source records, projections, snapshots, traces, and caches according to retention policy.

## Failure policy

- Cosmos 429/503, model throttling, Search transient failures: retry with bounded backoff.
- Unknown schema version, malformed payload, tenant mismatch: dead-letter.
- Model content-filter failures: mark job failed with safe error code; do not log raw payload.
- Lease acquisition timeout: abandon/retry.
- Lease fencing failure: abandon/retry or fail safe depending on state.

DLQ runbooks must support inspection and replay without exposing raw memory payloads in logs.

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
