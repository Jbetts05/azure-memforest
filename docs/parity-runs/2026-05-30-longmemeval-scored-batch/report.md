# Azure-MemForest LongMemEval proxy-scored batch

Run date: 2026-05-30

## Goal

Measure whether Azure-MemForest can answer benchmark-data questions successfully, beyond the prior one-case smoke. This run used LongMemEval-S cases against the currently deployed Azure-MemForest stack:

- API/worker image: `990944081f7c`
- Chat / extraction / dedupe / summarization / planner / browse / answer deployment: `gpt-5.4`
- Embedding deployment: `text-embedding-3-large`
- Embedding dimension: `3072`
- Recall index: `memforest-recall-large`

## Batch design

Artifacts:

- `batch_manifest.json` records the source URL, source SHA-256, deterministic seed, case IDs, and question-type distribution.
- `batch_cases.json` stores normalized selected sessions and turns for each case.
- `run_batch.py` is the executable harness.

Planned batch:

- 10 deterministic LongMemEval-S cases.
- All six question types represented.
- Selection seed: `20260530`.
- Per case, the harness used a bounded slice: first 8 haystack sessions plus answer sessions, then filled to 9 only when fewer than 9 were selected. Cases can exceed 9 sessions when multiple answer sessions are outside the first 8; case 2 selected 10 sessions.

Scoring is **proxy scoring**, not official LongMemEval LLM-judge scoring:

- normalized exact match
- normalized contains-gold
- token F1
- supporting-fact contains-gold

This is suitable for a first operational/quality signal, but not for reporting official LongMemEval scores.

## What happened

The 10-case run was intentionally stopped after it became operationally infeasible to finish in-session.

Completed:

- Case 1 completed end-to-end.
- Case 2 cleared memory and ingested 7 of 10 selected sessions before the run was stopped.

Observed throughput degradation:

| Metric | Value |
| --- | ---: |
| Completed cases | 1 / 10 |
| Case files present | 2 |
| Observed ingest jobs | 16 |
| Average observed ingest latency | ~187.5s |
| Median observed ingest latency | ~82.3s |
| Max observed ingest latency | ~735.3s |
| Query latency for completed case | ~6.5s |

Case 2 late-session ingests took ~735s and ~711s. Case 2 ran for about 6,380 seconds from clear submission through the seventh ingest completion and still had 3 selected sessions plus query remaining. At that observed pace, finishing the planned 10-case sequential batch would likely take many hours, not an interactive session.

The clear step also degraded from ~7.2s on case 1 to ~98.9s on case 2, suggesting broader service backpressure rather than just one slow ingest.

After stopping the client harness and waiting for drain, the queue eventually reached `active=0`. DLQ grew by 1 during this run, from 10 to 11. Final DLQ provenance shows the new attributable benchmark entry:

```text
job_b014f52c9825127c631c3652ae7ffd8f76511a7f0ace70b21240011bdaf8845d
payload_session_id: lme-batch-8e91e7d9-da71721f
dead_letter_reason: MaxDeliveryCountExceeded
```

## Quality signal from completed case

Case: `4b24c848`

Question:

```text
How many tops have I bought from H&M so far?
```

Gold:

```text
five
```

Azure-MemForest answer:

```text
5 tops.
```

The answer is semantically correct, and the support facts included:

```text
The user said that the user already had five tops from H&M.
```

The simple normalized proxy scorer marked `contains_gold=false` and `token_f1=0.0` because it did not normalize number words (`five`) to digits (`5`). This demonstrates why official/LLM-judge scoring or a better normalization layer is required before treating proxy metrics as benchmark scores.

This is an `n=1` quality signal for a single `knowledge-update` case, manually judged from answer text and support facts. It provides no quality signal for the other five LongMemEval-S question types in the selected batch.

## Stored evidence

- `case_results/01_4b24c848.json`: completed case result.
- `case_results/02_8e91e7d9.json`: partial second case result.
- `aggregate_partial.json`: aggregate after the completed first case.
- `aborted_run_summary.json`: final aborted-run summary.
- `queue_counts_after_partial_stop.json`, `queue_counts_after_drain_wait.json`, `queue_counts_after_long_drain_wait.json`, `queue_counts_final.json`: queue state across abort/drain.
- `deadletter_peek_final.json`: DLQ provenance.
- `worker_logs_after_partial_stop.txt`, `worker_logs_final.txt`: worker log captures.

## Conclusion

This run did **not** complete the planned 10-case benchmark batch. It produced two important findings:

1. **Quality signal:** Azure-MemForest answered the first benchmark-data case correctly with the right supporting fact, but this is only a manual `n=1` signal.
2. **Operational blocker:** The current ingest/materialization pipeline on the GPT-5.4 + text-embedding-3-large stack is too slow for even a 10-case sequential LongMemEval batch as currently configured.

The exact latency cause is not isolated by this run. It may be GPT-5.4 calls, embedding calls, extraction/dedupe/summarization loops, Cosmos/Search operations, queue lock/retry behavior, or a combination. Per-job model-call telemetry is required before attributing causality.

Do not run the full LongMemEval suite yet.

## Recommended next gate

Before retrying the batch:

1. Add official or LLM-judge scoring, or at minimum numeric/date normalization for proxy scoring.
2. Reduce ingest-time model pressure:
   - use a faster/cheaper model for extraction/summarization, or
   - batch less aggressively per case, or
   - add throttle-aware pacing and explicit retry telemetry.
3. Add per-job model-call telemetry: call count, 429 count, retry duration, model role, token usage if available.
4. Re-run a smaller 3-case batch with strict per-case timeout and no client-side indefinite waits.
