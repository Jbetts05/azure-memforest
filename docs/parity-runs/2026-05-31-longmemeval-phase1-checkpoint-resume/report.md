# LongMemEval Phase 1 progress-aware timeout rerun

Run date: 2026-05-31

## Goal

Replace the fixed 5-minute per-session timeout with progress-aware job polling:

- hard cap: 30 minutes per session ingest;
- idle cap: 10 minutes without real progress;
- progress signals: status/stage transition, stage-history length, telemetry call count, telemetry latency;
- `updated_at` is not treated as progress.

## Result

The rerun showed the harness can run with progress-aware session polling, but it did not prove a stalled-session timeout path because the observed sessions were making progress or succeeded. The case-level 20-minute budget was too small for the current calibration case shape.

Case `e47becba`:

- sessions 1-4 succeeded;
- session 5 triggered content filter and was skipped as non-answer input;
- sessions 6-8 succeeded;
- the harness hit the 20-minute case cap before session 9 could complete and query/judge could run;
- no new DLQ was produced;
- queue drained to `active=0`, `deadletter=14`.

## Interpretation

The previous fixed per-session timeout was too aggressive. Progress-aware polling is the right direction for long but healthy sessions, but this run mainly exposed a second issue: completed session work is not checkpointed across case-level interruption.

The next bottleneck is the total per-case timeout. A bounded LongMemEval case with 9 selected sessions can exceed 20 minutes even when all sessions are progressing normally.

## Gate decision

Phase 1 still does not pass. Phase 2 remains blocked.

Before rerunning Phase 1 again:

1. add per-session checkpointing/resume so completed ingests are not repeated after a harness timeout or interruption;
2. keep progress-aware session polling;
3. then set a modest larger case wall-clock budget based on observed duration.

The recommended next change is checkpointing first, not simply raising the timeout. A larger budget without checkpointing only makes the next failure more expensive.
