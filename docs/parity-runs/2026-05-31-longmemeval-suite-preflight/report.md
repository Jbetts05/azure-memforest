# LongMemEval full-suite preflight

Run date: 2026-05-31

## Purpose

Lock the experiment decisions required before starting the phased LongMemEval-S suite.

This preflight does **not** run benchmark cases. It captures the scoring decision, stop rules, isolation policy, model stack, environment baseline, and next gate.

## Locked decisions

| Area | Decision |
| --- | --- |
| GPT-4o policy | GPT-4o and GPT-4o-mini are disallowed for every role. |
| Judge | Use the LongMemEval `evaluate_qa.py` task-specific yes/no rubric with Azure `gpt-5.4`, temperature 0; cross-check calibration with `gpt-5.4-mini-memory` and flag disagreements. |
| Memory model | `gpt-5.4-mini-memory` |
| Answer model | `gpt-5.4` |
| Embeddings | `text-embedding-3-large`, 3072 dimensions |
| Dataset | LongMemEval-S cleaned first |
| Isolation | Fresh synthetic user ID per case where possible; cleanup after scoring. |
| Concurrency | Serial through the 10-case pilot; revisit controlled parallelism after the 50-case tier. |
| Timeouts | Start with 20 minutes per case and 5 minutes per session ingest. |
| Stop rules | Halt on any new DLQ, >5% case failures, repeated 429s, timeout over budget, or residue contamination. |
| Cost | No cap, but token/cost estimates still get recorded. |
| Environment | Dev is acceptable for calibration/pilot with public-network caveat documented. |

## Scoring decision

The official LongMemEval repository evaluates QA with `src/evaluation/evaluate_qa.py`. That script uses task-specific prompts and asks an LLM judge to answer yes/no.

The upstream script's built-in `model_zoo` is GPT-4o-era. Because GPT-4o is disallowed for this project, Azure-MemForest will reuse the official task-specific prompt logic but run the judge on `gpt-5.4`.

This is **not directly comparable to published LongMemEval scores** because it uses a GPT-5-family judge instead of the upstream GPT-4o judge, and the answer model is also GPT-5.4. Treat results as a pinned GPT-5-family judge score, not an official published-score reproduction.

The upstream evaluator source is stored as `longmemeval_evaluate_qa.py`; its SHA-256 is recorded in `judge_prompt_source_hash.json`.

Proxy metrics such as exact match, contains-gold, and token F1 remain debug-only.

## Gate sequence

1. **Preflight** — this run.
2. **3-case calibration** — known-hard/representative cases; verifies judge, isolation, timeouts, telemetry, and cleanup.
3. **10-case stratified pilot** — covers all question types and reruns the previously failed scale.
4. **50-case subset** — first meaningful quality/performance signal.
5. **Full 500-case LongMemEval-S** — only after the 50-case gate passes.

## Environment baseline

Artifacts:

- `manifest.json`
- `phase0_decisions.json`
- `api_model_env.json`
- `worker_model_env.json`
- `model_deployments.json`
- `eastus_cognitive_usage.json`
- `queue_baseline.json`
- `dlq_baseline_messages.json`
- `search_index_schema.json`
- `longmemeval_evaluate_qa.py`
- `judge_prompt_source_hash.json`

Current queue baseline:

```json
{
  "active": 0,
  "deadletter": 12,
  "scheduled": 0,
  "transferDeadletter": 0
}
```

The pre-existing DLQ count is non-zero from earlier development runs. Future phases compare against this baseline and should halt on any new DLQ.

DLQ comparisons should use message IDs from `dlq_baseline_messages.json`, not just count deltas.

The `memforest-recall-large` Search index schema is captured in `search_index_schema.json`; its vector field is verified at 3072 dimensions for `text-embedding-3-large`.

The worker currently uses the same shared container image as the API with a worker command override. Seeing `azure-memforest-api:<sha>` in worker environment artifacts is expected.

## Next gate

Implement and run **Phase 1: 3-case calibration**.

Requirements before the first calibration case:

- Pin the judge prompt implementation in the harness.
- Record the judge prompt hash in every calibration manifest.
- Write per-case state atomically.
- Use fresh synthetic user IDs per case.
- Capture raw answer, facts, judge prompt/output, proxy scores, job telemetry, queue/DLQ deltas, and report artifacts.
