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

## Follow-up rerun after write-reliability fixes

After the worker job-recording race, final-delivery failure handling, and dev Storage network blocker were fixed, the same bounded case was rerun against API image `81e9e956fc1e`.

Additional artifacts:

- `azure_smoke_case_001_token_expired_partial.json`: documents the first rerun harness failure where the bearer token expired during long polling.
- `azure_smoke_case_001_token_refresh.json`: final refreshed-token rerun result.
- `queue_counts_after_token_refresh.json`: Service Bus queue counts after the final rerun.
- `deadletter_peek_after_token_refresh.json`: DLQ provenance after the final rerun.
- `worker_logs_after_token_refresh.txt`: worker log tail after the final rerun.

Final refreshed-token rerun result:

| Step | Status | Notes |
| --- | --- | --- |
| clear memory | `succeeded` | Completed in about 36 seconds. |
| ingest sessions | `9/9 succeeded` | 76 selected turns were ingested. Per-session times ranged from about 12 to 128 seconds. |
| query | `succeeded` | Answer: `Business Administration`; `contains_gold=true`. |

The query used the expected supporting fact: `The user graduated with a degree in Business Administration, which has helped them in their new role.`

## Operational findings from rerun

- The deployed pipeline can now execute one bounded LongMemEval-S case end-to-end through clear, ingest, query, and answer generation.
- The harness must refresh Entra bearer tokens during long-running multi-session evaluations.
- The dev Storage account had to be restored to public network access because the current Container Apps dev environment has no private endpoint/VNet path to Blob. This is a dev-only unblocker, not a production posture.
- Worker logs still show Azure OpenAI `429` retries and Service Bus timeout noise. The one-case run completed despite this, but larger runs need throttle-aware pacing and latency/error telemetry.
- Service Bus had `active=0` after the final run. The `deadletter=8` messages were peeked and recorded; they came from earlier failed runs, including one post-image/pre-storage-fix message with `AuthorizationFailure`.

## Current conclusion

The one-case LongMemEval smoke is now **passed**, but this is still only a pipeline sanity check. Do **not** treat it as benchmark quality evidence or run the full suite yet. The next gate should be a small multi-case batch across question types with explicit pacing, token refresh, DLQ provenance, and 429/timeout telemetry.
