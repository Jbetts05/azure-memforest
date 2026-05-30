# LongMemEval-S Azure smoke run

Run date: 2026-05-30

## Goal

Run a bounded LongMemEval-S smoke through Azure-MemForest before any full benchmark. This was intended to validate:

- dataset normalization
- live clear-memory path
- session ingest job completion
- question query
- trace/fact/forest capture

This was **not** intended as an answer-quality benchmark.

## Dataset

Source: Hugging Face `xiaowu0162/longmemeval-cleaned`, file `longmemeval_s_cleaned.json`.

Prepared case:

- `case_id`: `e47becba`
- `question_type`: `single-session-user`
- `question`: `What degree did I graduate with?`
- `gold_answer`: `Business Administration`
- total haystack sessions: `53`
- selected smoke sessions: `9`
- selected turns: `76`
- selection policy: first 8 haystack sessions plus all `answer_session_ids`, capped at 30 sessions

Artifacts:

- `normalized_case_001.json`
- `source_info.json`
- `azure_smoke_case_001_result.json`
- `azure_smoke_case_001_error.json`

## Execution result

The run did **not** complete the full ingest/query cycle.

Observed events:

| Step | Status | Notes |
| --- | --- | --- |
| clear memory | still `running` after timeout | Job was accepted and claimed by worker, but did not reach terminal status within the cap. |
| ingest session 1 | `succeeded` | 2-turn session completed in about 17 seconds. |
| ingest session 2 | still `running` after timeout | 12-turn session was accepted and claimed, but did not reach terminal status within the cap. |

No benchmark query was executed because ingest did not complete.

## Important finding

The smoke revealed a live job lifecycle or harness-timeout issue:

- A `clear_memory` job remained `running`.
- Later ingest jobs were accepted anyway.
- One ingest succeeded, another remained `running`.

Post-timeout polling still showed the clear job and second ingest job as `running`; see `post_timeout_job_status.json`. Worker log tail was captured in `worker_logs_tail.txt` and shows repeated Azure Service Bus AMQP operation timeouts around the same period.

This means the smoke harness is viable, but the current live job/worker path is not reliable enough for a larger LongMemEval run.

## Interpretation

This is a useful failed smoke:

1. Dataset acquisition and normalization work.
2. Case selection can include answer evidence sessions, but this was not exercised fully because the run stopped before the answer session was ingested.
3. Azure API accepts replay jobs.
4. The smoke harness had a bug: it continued after `clear_memory` was non-terminal. Future runs must abort immediately if clear does not finish.
5. The live materialization/clear job path needs reliability and timeout investigation before scaling.

The failure should not be interpreted as LongMemEval answer-quality failure. The system did not reach the query stage.

## Recommended fixes before retry

1. Debug `clear_memory` worker job completion and Service Bus AMQP timeout behavior.
2. Add stronger job timeout/heartbeat/diagnostic events in worker.
3. Fix the smoke harness so non-terminal clear aborts the run before ingesting anything.
4. Ensure write lease behavior prevents concurrent clear + ingest overlap.
5. Add a smaller synthetic smoke before LongMemEval:
   - clear
   - one 2-turn session
   - query
   - verify terminal statuses
6. Retry LongMemEval with:
   - one case
   - answer sessions only plus minimal context
   - longer but bounded job timeout
   - strict abort if clear does not complete

## Current conclusion

Do **not** run the full LongMemEval suite yet. The data path is feasible, but the live job/worker reliability must be fixed first.
