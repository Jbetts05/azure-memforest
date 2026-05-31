# Azure-MemForest memory pipeline improvement

Run date: 2026-05-31

## Goal

Improve the memory-building pipeline while still benefiting from advanced models:

1. Use a faster model for high-volume memory-building work.
2. Keep the advanced model for final answer paths.
3. Make long memory jobs visible through stages.
4. Add model-call telemetry.
5. Keep memory building asynchronous/background-safe.

## Implementation

Code changes:

- Added model-call telemetry at the `AzureOpenAIModelClient` boundary.
- Added thread-safe telemetry collection that also captures nested worker-thread calls.
- Added `stage`, `stage_started_at`, `stage_history`, and `telemetry` to job status.
- Added `CosmosJobStore.update_stage(...)` with owner/fencing-token and ETag safety.
- Worker ingest now reports stages:
  - `hydrate`
  - `extract_build`
  - `manifest_pending`
  - `project`
  - `write_projection`
  - `commit`
- Existing `/jobs/{job_id}/events` now surfaces stage progress.

Model routing:

- Fast memory model: `gpt-4o-mini-memory`
- Advanced answer model: `gpt-5.4`
- Embeddings: `text-embedding-3-large`
- Recall index: `memforest-recall-large`

Deployed image:

```text
memforestexampleacr.azurecr.io/azure-memforest-api:ba0e9ce3ecf2
```

Focused tests:

```text
python -m pytest tests\test_cloud_config_models.py tests\test_cosmos_jobs.py tests\test_service_app.py tests\test_worker_materialization.py -q
72 passed
```

## Validation notes

Two validation-environment issues were encountered and restored for dev:

- Cosmos public network access had been disabled, blocking Container Apps outbound IP `192.0.2.8`.
- Storage public access/default action had to be restored for the dev Container Apps environment.

These are infrastructure/network posture issues, not pipeline-code failures. Production should use private networking rather than public dev access.

## Synthetic smoke

Artifact: `synthetic_smoke_after_storage_fix.json`

Result:

- Clear succeeded in ~3.3s.
- Ingest succeeded in ~30.2s.
- Stage history and telemetry were returned on job status/events.
- Query for the synthetic phrase returned no facts, so this smoke is useful for pipeline/job validation but not answer-quality validation.

Telemetry from synthetic ingest:

- `chat:default:gpt-4o-mini-memory`: 2 calls, ~2.8s total.
- `embedding:text-embedding-3-large`: 4 calls / 12 items, ~1.19s total.

## Targeted benchmark-session validation

The key validation reran a previously problematic LongMemEval session family after fast-model routing.

### Target 1

Artifact: `target_slow_session_ingest.json`

- Case: `8e91e7d9`
- Source session: `da71721f`
- Turns: 12
- Previous related run: this session family produced very long late ingests (~735s and ~711s) and a DLQ on `lme-batch-8e91e7d9-da71721f`.
- New result: ingest succeeded in ~42.3s.

Stage timing:

- `extract_build`: ~32.8s
- `write_projection`: ~5.7s
- remaining stages: ~3.8s total

Telemetry:

- `chat:default:gpt-4o-mini-memory`: 23 calls, ~35.4s total, max ~4.8s.
- `embedding:text-embedding-3-large`: 4 calls / 76 items, ~2.5s total.

Readback query:

Artifact: `target_query.json`

Question:

```text
Who was the salsa instructor I liked?
```

Answer:

```text
Alex.
```

Supporting fact:

```text
The instructor of the salsa class was named Alex.
```

### Target 2

Artifact: `target_second_slow_session_ingest.json`

- Case: `4b24c848`
- Source session: `answer_2cec623b_2`
- Turns: 12
- Previous observed latency for this session in the earlier batch: ~156.9s.
- New result: ingest succeeded in ~54.0s.

Telemetry:

- `chat:default:gpt-4o-mini-memory`: 16 calls, ~42.2s total, max ~8.6s.
- `embedding:text-embedding-3-large`: 4 calls / 78 items, ~1.5s total.

## Outcome

The memory pipeline is materially improved for the tested sessions:

- One previously dead-lettering/very slow session family completed in ~42s.
- One previously ~157s session completed in ~54s.
- Stage progress is now visible.
- Model-call telemetry now identifies the dominant cost: high-volume memory chat calls, not embeddings.
- Query/readback works on the targeted benchmark session.

## Caveats

- This is not yet a full benchmark rerun. It is two targeted session validations plus a synthetic smoke.
- The worker core still labels many internal calls as `default`, so telemetry confirms deployment-level routing but not perfect logical role attribution.
- `AZURE_OPENAI_SUMMARIZATION_DEPLOYMENT=gpt-5.4` is set in env, but current worker core paths often use the default model name internally; summarization-specific routing needs a follow-up if we want strong summaries while keeping extraction/dedupe fast.
- DLQ count remained non-zero from prior runs and failed validation attempts. Final queue state was `active=0`, `deadletter=12`.

## Recommendation

The solution should keep this mixed model strategy:

- Fast model for memory-building internals.
- GPT-5.4 for final answer/judge paths.
- Embedding-large for vector quality.

Next gate: rerun a small 3-case LongMemEval batch with strict timeouts and official/LLM-judge scoring to verify that latency improved without hurting answer quality.
