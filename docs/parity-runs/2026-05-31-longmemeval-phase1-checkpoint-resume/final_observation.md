# Final observation for checkpoint-resume Phase 1

The checkpoint/resume and reconciliation harness worked, but Phase 1 still did not pass.

## What succeeded

- Cases `e47becba` and `4b24c848` remained checkpointed as completed and judged correct.
- Case `8e91e7d9` sessions 1-6 were reconciled as completed, including the session 6 DLQ settlement artifact.
- Session 7 (`44fc2b1e`) was confirmed non-answer-bearing and was skipped as a modified-input/pathological long-session workaround.
- Early-settle mode prevented new Service Bus DLQ while session 8 was running.

## What failed

Session 8 (`da71721f`) entered `extract_build` and remained `running` with no telemetry progress after more than 20 minutes:

```text
job_f5dc003c69102a0db46332a66c9417bab8e01676db41e0cf1b7438370eefeb89
status: running
stage: extract_build
telemetry: {}
```

Because early-settle completes the Service Bus message before the long handler runs, the queue shows no active message and no DLQ. That avoids lock-loss DLQ, but it means an interrupted/stalled worker can leave an orphaned Cosmos job with no queue message to resume it.

## Current blocker

Phase 1 is now blocked by missing stale-job reaping/resume for early-settled long jobs.

## Next required product/harness slice

Add a benchmark stale-job reaper:

1. detect benchmark jobs stuck in `running` with no stage/telemetry progress beyond a threshold;
2. mark them as `failed_stale` or release/requeue them safely;
3. let the harness retry only that session with a new attempt;
4. preserve existing checkpointed successes.

Longer-term, product-grade handling should use durable claim/reaper or continuation jobs rather than relying on one in-process handler for very high fan-out sessions.
