# LongMemEval Phase 1 calibration with benchmark users

Run date: 2026-05-31

## Goal

Rerun Phase 1 using benchmark-only synthetic users so each case starts with isolated memory and the harness does not need destructive clear-memory before every case.

## Changes from previous Phase 1 attempts

- Enabled benchmark-only synthetic user auth behind a temporary secret header.
- Used one synthetic user per calibration case.
- Kept content-filter policy:
  - skip non-answer filtered sessions and record modified input;
  - block answer-bearing filtered sessions;
  - no deterministic content-filter DLQ.

## Result

Phase 1 made substantial progress but did not pass the gate.

| Metric | Value |
| --- | ---: |
| Attempted cases | 3 |
| Completed cases | 2 |
| Strict unmodified completed cases | 1 |
| Best-effort completed cases | 2 |
| Filtered non-answer sessions skipped | 1 |
| Blocked answer-bearing sessions | 0 |
| GPT-5.4 judge correct | 2 / 2 completed |
| GPT-5.4 vs GPT-5.4-mini judge agreement | 2 / 2 completed |
| New DLQ | 0 |

The final queue state drained cleanly:

```json
{
  "active": 0,
  "deadletter": 14,
  "scheduled": 0
}
```

## Case outcomes

### `e47becba`

- Type: `single-session-user`
- Gold: `Business Administration`
- Answer: `Business Administration.`
- Primary judge: correct
- Cross-check judge: correct
- One non-answer session triggered content filtering and was skipped.
- This case counts in best-effort metrics, not strict unmodified metrics.

### `4b24c848`

- Type: `knowledge-update`
- Gold: `five`
- Answer: `5 tops`
- Primary judge: correct
- Cross-check judge: correct
- No content-filter skips.
- This case counts in strict metrics.

### `8e91e7d9`

- Type: `multi-session`
- Gold: `4`
- Status: failed ingest / timeout in harness.
- The fourth ingest was still `running` after the 5-minute per-session budget.
- The queue later drained with no new DLQ.
- Direct job readback confirmed the fourth ingest eventually **succeeded** after ~399s total.
- That job made 602 `gpt-5.4-mini-memory` chat calls and embedded 893 items.

## Interpretation

The benchmark-user isolation strategy worked and should replace clear-per-case for the suite.

The content-filter policy also worked for the first filtered non-answer session: it was skipped and did not create a new DLQ.

The remaining Phase 1 blocker is now timeout policy and high fan-out long-session handling, not clear-memory or content-filter DLQ behavior.

## Gate decision

Phase 1 does **not** pass yet because one of three calibration cases exceeded the fixed 5-minute session-ingest timeout, even though the timed-out job later succeeded.

Phase 2 remains blocked until we either:

1. raise or scale the per-session timeout for calibration based on observed stage telemetry; or
2. further optimize the memory pipeline for long multi-session cases; or
3. classify long-but-eventually-successful jobs separately from failed jobs and wait for terminal status with a larger timeout budget.

## Recommended next action

Update the Phase 1 timeout policy from a fixed 5 minutes per session to a stage-aware/progress-aware timeout:

- keep a hard cap to avoid runaway jobs;
- allow longer for sessions with many generated facts/model calls;
- continue polling while the job changes stage or otherwise updates progress;
- fail only when the job is both over budget and not making progress.

Then rerun Phase 1 benchmark-user calibration.
