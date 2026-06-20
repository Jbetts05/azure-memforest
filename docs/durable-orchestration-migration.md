# Service Bus to Durable Functions migration

This document records the migration path from Service Bus worker-owned write execution to Azure Durable Functions orchestration for Azure-MemForest.

## Summary

Azure-MemForest originally used Service Bus as both the write ingress queue and the long-running execution driver. That worked for short jobs, but live Phase 1 calibration exposed the mismatch: memory ingestion can take longer than broker lock windows, can stall in model calls, and needs progress-aware recovery that is easier to reason about as a durable workflow than as a message-lock loop.

The new target is:

- API records the accepted write job in Cosmos.
- API starts a Durable Functions orchestration for `ingest_session`.
- Durable activity reuses the existing `MemForestMaterializationHandler`.
- Cosmos jobs, manifests, tree nodes, fact locations, Search recall docs, and Blob snapshots remain the source-of-truth contracts.
- Service Bus remains configured as fallback, especially for `delete_session` and `clear_memory` until those paths are migrated.

Service Bus is not being removed. It remains valuable for queueing, fallback, and non-ingest writes. Durable Functions now owns the long-running ingest execution path when explicitly enabled.

## Why migrate

### Benefits

1. **Durable execution instead of message-lock execution**
   Durable Functions manages state, checkpoints, retries, and recovery for long-running workflows. This better matches extraction, dedupe, tree rebuild, model calls, Search projection, and manifest commit.

2. **Clear orchestration identity**
   Durable instances use the deterministic `job_id` as `instance_id`, so retries and duplicate starts converge on the same job boundary.

3. **Better status correlation**
   Cosmos job status and Durable instance status can be correlated by the same ID. The live smoke now verifies both API `/jobs` state and Durable `Completed` state.

4. **Less dependence on Service Bus lock renewal**
   Service Bus PeekLock and session locks remain useful, but using them to hold long model-heavy work creates lock-renewal and DLQ failure modes. Durable shifts the long-running unit to the orchestration runtime.

5. **Preserves existing materialization semantics**
   The Durable activity calls the same materialization handler used by the worker path, preserving per-user leases, fencing tokens, pending/committed manifest visibility, and version-scoped projections.

6. **Flex Consumption hosting**
   The current dev deployment uses Azure Functions Flex Consumption after EP1 quota was unavailable in East US 2. Flex keeps the function app in-region and serverless, with configurable memory and per-function scaling.

## Supporting documentation

- Durable Functions overview: <https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-overview>
- Durable Functions storage providers and task hubs: <https://learn.microsoft.com/azure/durable-task/common/durable-task-storage-providers>
- Durable orchestrator code constraints: <https://learn.microsoft.com/azure/durable-task/common/durable-task-code-constraints>
- Azure Functions Flex Consumption plan: <https://learn.microsoft.com/azure/azure-functions/flex-consumption-plan>
- Azure Functions Flex deployment guidance: <https://learn.microsoft.com/azure/azure-functions/flex-consumption-how-to>
- Service Bus message locks and settlement: <https://learn.microsoft.com/azure/service-bus-messaging/message-transfers-locks-settlement>

## Current implementation

### Dispatch

API dispatch is controlled by environment variables:

- `AZURE_MEMFOREST_DISPATCH_MODE=servicebus|durable`
- `AZURE_MEMFOREST_DURABLE_START_URL`
- `AZURE_MEMFOREST_DURABLE_START_TOKEN`

Defaults prefer Durable orchestration:

- Bicep defaults `apiDispatchMode` to `durable`.
- Durable dispatch settings are emitted when `apiDispatchMode=durable`; if the Durable start URL is not configured, the API falls back to Service Bus.
- The Durable function key is stored as a Container Apps secret and referenced via `secretRef`.

When Durable dispatch is enabled and Service Bus fallback is configured:

- `ingest_session` goes to Durable.
- `delete_session` and `clear_memory` still go to Service Bus.

### Durable Function App

Location:

- `durable_write_orchestrator/function_app.py`
- `durable_write_orchestrator/host.json`
- `durable_write_orchestrator/requirements.txt`

Functions:

- `start_write`: HTTP starter at `/api/writes/start`
- `write_orchestrator`: Durable orchestrator
- `materialize_write_job`: activity that runs the existing materialization path
- `fail_write_job`: compensation activity after exhausted retries

Important implementation details:

- The starter uses `job_id` as Durable `instance_id`.
- Duplicate start conflicts are treated idempotently.
- `runtime_status=None` from `get_status()` is not treated as an existing instance.
- The Function app avoids postponed or parameterized activity annotations because Azure Functions Python worker indexing on Flex failed with `dict[str, Any]` annotations.
- `PYTHON_ENABLE_WORKER_EXTENSIONS=1` is set for Durable Python worker extensions.

### Activity runner

Location:

- `src/worker/durable_activity.py`

Behavior:

- Parses the same `WriteJobEnvelope` contract used by Service Bus.
- Claims the Cosmos job with owner ID `durable:{instance_id}`.
- Calls `MemForestMaterializationHandler`.
- Completes Cosmos job on success.
- Returns structured failures for terminal/permanent errors.
- Re-raises transient errors for Durable retry.
- Preserves terminal replay semantics: an already-succeeded job remains succeeded.

### Infrastructure

Location:

- `infra/main.bicep`

Important parameters:

- `deployDurableOrchestrator=false` by default
- `durableHostingPlan='flex'` by default, with `ep1` as explicit fallback
- `apiDispatchMode='durable'` by default
- `durableStartFunctionKey` is `@secure()`

Hosting notes:

- EP1 was blocked by East US 2 quota (`Current Limit Total VMs: 0`).
- Flex Consumption deployment succeeded and is currently used.
- Flex uses `functionAppConfig` and a deployment blob container.
- The Function app remains deployed, but API dispatch is reverted to Service Bus after smoke windows unless explicitly testing Durable.

## Validated gates

### Direct Durable smoke

Result: passed.

Validated:

- Accepted Cosmos job created.
- Durable starter launched orchestration.
- Durable activity claimed the job.
- Materialization reached manifest commit.
- Cosmos job reached `succeeded`.

### API-through-Durable smoke

Result: passed.

Validated:

- API accepted ingest.
- API recorded accepted Cosmos job.
- API started Durable instance.
- Durable instance reached `Completed`.
- API `/jobs` returned `succeeded`.
- API query recovered the smoke marker.

Image:

- `memforestexampleacr.azurecr.io/azure-memforest-api:f25e65c18ec4`

### Promotion-readiness smoke

Result: passed.

Validated:

- Two sequential same-user API-through-Durable ingests.
- Both Durable instances completed.
- Both API jobs succeeded.
- Queries recovered both unique markers.
- Latest committed projection referenced both facts via latest fact locations.

### Phase 1 Durable calibration

Result: ingestion/reliability passed; the first full aggregate remained `gate_failed` due to answer quality.

Validated:

- 3/3 cases completed.
- 28/28 ingests succeeded.
- 28/28 Durable instances completed.
- 0 DLQ delta.
- 0 content-filter skips.
- Strict unmodified inputs.

Failure:

- Case `4b24c848` knowledge-update answered `3 tops` instead of expected `five`.
- This is a retrieval/temporal update quality issue, not a Durable execution issue.

Follow-up:

- The correct fact, "The user has already purchased five tops from H&M so far," was present in canonical facts and referenced by latest fact locations.
- Cloud query recall/rerank favored an older "three tops" fact.
- A targeted fix added gated lexical fact fallback for update/count questions and recency-aware fact reranking.
- Targeted case `4b24c848` rerun passed with answer `Five tops`, 9/9 Durable instances completed, and both judges correct.

Committed artifacts:

- `docs/parity-runs/2026-05-31-longmemeval-phase1-checkpoint-resume/aggregate.json`
- `docs/parity-runs/2026-05-31-longmemeval-phase1-checkpoint-resume/case_results_durable-20260602135302/`
- `docs/parity-runs/2026-05-31-longmemeval-phase1-checkpoint-resume/case2_retrieval_durable_20260602135302.json`
- `docs/parity-runs/2026-05-31-longmemeval-phase1-checkpoint-resume/aggregate_case2fix_20260602163133.json`
- `docs/parity-runs/2026-05-31-longmemeval-phase1-checkpoint-resume/case_results_case2fix-20260602163133/`

## Limitations and open issues

1. **Durable is the default write control plane**
   The live API should use `apiDispatchMode=durable` for production-like writes. Service Bus remains the direct-worker fallback and the Durable activity offload transport; `enableDevHeaderAuth=false` should still be restored after smoke runs.

2. **Delete and clear still use Service Bus**
   Durable migration currently covers `ingest_session`. `delete_session` and `clear_memory` remain on Service Bus fallback.

3. **API auth is still a smoke-window concern**
   Dev-header auth is only safe in tightly controlled guarded windows. Production use requires Entra or another governed auth mode.

4. **Durable status endpoint requires a valid status key and connection name**
   Harnesses use the Durable status endpoint for evidence. The task hub, connection parameter, and key must match the deployed Function App.

5. **Function indexing is sensitive to Python annotations**
   Flex/Python Durable indexing failed when activity functions used postponed/parameterized annotations. Keep Function entrypoint annotations simple.

6. **Knowledge-update retrieval was improved but needs full-suite revalidation**
   The targeted `4b24c848` rerun passed after adding lexical fallback and recency-aware reranking. The full 3-case Phase 1 Durable harness should be rerun with this fix before Phase 2 promotion.

7. **Service Bus still has unresolved historical blockers**
   Prior Phase 1 blockers around Service Bus lock/DLQ/stale-running jobs are bypassed for Durable ingests, not fully removed for all write types.

## Recommended next steps

1. Rerun full Phase 1 Durable calibration with the knowledge-update fix:
   - confirm 3/3 cases pass;
   - confirm 28/28 Durable instances complete;
   - confirm 0 DLQ delta and no content-filter skips;
   - then decide whether Phase 2 can proceed.

2. Decide delete/clear migration:
   - Keep on Service Bus with explicit docs and tests, or
   - Implement Durable delete/clear orchestration before broader Durable promotion.

3. Add operational runbook:
   - one-command enable Durable dispatch;
   - one-command revert to Service Bus;
   - checks for Durable failure rate, running/stale instances, API job failures, and DLQ growth.

4. Add durable-mode API readiness signal:
   The harness currently asserts mode by behavior. A future API endpoint or readiness field should expose effective dispatch mode to prevent false-green tests.

5. Use Durable API ingest for future benchmark reruns:
   Phase 1 ingestion is now reliable enough to proceed to retrieval-quality investigation without fighting Service Bus lock behavior.
