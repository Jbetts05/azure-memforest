# LongMemEval Phase 1 rerun after content-filter policy

Run date: 2026-05-31

## Goal

Rerun Phase 1 after implementing the standard content-filter policy:

- deterministic content-filter failures become terminal failed jobs without Service Bus DLQ;
- the harness skips non-answer filtered sessions and records modified-input metadata;
- answer-bearing filtered sessions block the case.

## Result

The rerun did not reach content-filter handling.

Case `e47becba` failed at the initial clear step:

- clear job: `job_4bea4da4cf6a13bb6dc9552eca2407368437cea375e1c5137b6db3e994dad5b5`
- harness waited 360s;
- job remained `running` from the API perspective;
- Service Bus later dead-lettered the clear message with `MaxDeliveryCountExceeded`;
- DLQ increased from 13 to 14.

Worker logs show repeated Azure Service Bus AMQP operation timeouts around the clear job processing window.

## Interpretation

This is a separate reliability blocker from content filtering. The content-filter no-DLQ policy was implemented and tested locally, but the live rerun was blocked by clear-memory/Service Bus timeout behavior before any session ingest occurred.

Phase 2 remains blocked.

## Next remediation

Before rerunning Phase 1:

1. make clear-memory robust enough for repeated calibration runs, or avoid clear by using a benchmark-isolated user strategy;
2. inspect whether the clear handler is doing unnecessary expensive cleanup for calibration;
3. consider a benchmark namespace/user-per-case path so the harness does not need destructive clear before every case;
4. keep the content-filter policy in place for the next ingest rerun.
