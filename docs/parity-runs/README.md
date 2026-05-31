# Parity run experiment tracking

Use these folders for lightweight experiment tracking until the benchmark workflow is large enough to justify Azure ML or Foundry evaluation orchestration.

Each run folder should include:

- `manifest.json` — structured run metadata, inputs, model stack, metrics, artifacts, and caveats.
- `report.md` — human-readable analysis and conclusion.
- Raw artifacts — case files, queue/DLQ snapshots, logs, model env, query outputs, and scoring outputs.

## Manifest workflow

1. Copy `manifest.template.json` into the new run folder as `manifest.json`.
2. Fill it before running where possible:
   - run ID/date/goal
   - git SHA and deployed image
   - model stack
   - dataset/case IDs
   - scoring method
3. Append results after the run:
   - status
   - latency and quality metrics
   - failures/DLQ changes
   - artifact paths
   - caveats and next gate

This keeps experiments reproducible and comparable without requiring Azure ML setup yet. Move to Azure ML or Foundry evals when runs become scheduled, multi-user, or large enough to need managed lineage and dashboards.

## Current experiment gates

Use these gates before scaling LongMemEval or similar long-memory benchmarks:

1. **Synthetic smoke** — clear memory, ingest one small session, query a known fact.
2. **Targeted slow-session validation** — rerun one or two sessions that previously showed long ingest tails or DLQ risk.
3. **Small scored batch** — 3-10 deterministic benchmark cases with strict timeouts and official/LLM-judge scoring.
4. **Full suite** — only after latency, DLQ behavior, model routing, and scoring are stable.

## Required manifest fields for benchmark runs

For benchmark or model-routing comparisons, always fill:

- `code.git_sha`
- `code.image`
- `model_stack.*`
- `dataset.case_ids`
- `scoring.method`
- `scoring.official_benchmark_score`
- `metrics.latency_sec`
- `metrics.model_calls`
- `metrics.queue`
- `caveats`
- `next_gate`

## Interpreting proxy scores

Proxy metrics such as normalized exact match, substring contains-gold, and token F1 are useful for quick debugging, but they are not official LongMemEval scores. They can mark a semantically correct answer as wrong, for example `5 tops` versus gold `five`.

Use official or LLM-judge scoring before making quality claims.

## Current model-routing caveat

Run manifests should distinguish:

- the configured deployment for each role, and
- the deployment actually observed in job telemetry.

Some worker core paths still use the default model name internally, so telemetry may show `chat:default:<deployment>` even when an environment variable exists for a more specific role such as summarization.
