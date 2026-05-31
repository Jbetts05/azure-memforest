# LongMemEval Phase 1 calibration

Run date: 2026-05-31

## Goal

Run a 3-case LongMemEval-S calibration using the locked full-suite decisions:

- no GPT-4o or GPT-4o-mini;
- memory model `gpt-5.4-mini-memory`;
- answer model `gpt-5.4`;
- judge model `gpt-5.4` with `gpt-5.4-mini-memory` cross-check;
- same-user clear-per-case isolation because live Entra auth does not allow arbitrary synthetic benchmark users;
- halt on any new DLQ or operational failure.

## Selected cases

Artifact: `calibration_cases.json`

| Case | Type | Question | Gold |
| --- | --- | --- | --- |
| `e47becba` | `single-session-user` | What degree did I graduate with? | Business Administration |
| `4b24c848` | `knowledge-update` | How many tops have I bought from H&M so far? | five |
| `8e91e7d9` | `multi-session` | What is the total number of siblings I have? | 4 |

## Result

Phase 1 was halted during case 1, as required by the gate rules. It did not produce a query or judge-quality signal.

Case `e47becba`:

- Clear succeeded in ~18.5s.
- Ingest sessions 1-4 succeeded.
- Ingest session 5 failed.
- Queue DLQ increased from 12 to 13.

Failure:

```text
BadRequestError: content_filter / ResponsibleAIPolicyViolation
```

Azure OpenAI filtered the extraction prompt for the LongMemEval session:

```text
payload_session_id: lme-e47becba-sharegpt_DGTCD7D_0-...
stage: extract_build
content_filter_result.sexual.filtered: true
severity: medium
```

This is not an answer-quality failure. It is an ingestion/evaluation-suite compatibility blocker: at least one benchmark source conversation can trigger Azure OpenAI content filtering during memory extraction.

## Evidence

- `case_results/01_e47becba.json`
- `aggregate.json`
- `dlq_after_gate_failure.json`
- `worker_logs_after_gate_failure.txt`
- `queue_after.json`

## Gate decision

Phase 1 **halted before scoring** and Phase 2 must not start yet.

The next remediation slice should first size the content-filter problem before changing benchmark policy:

1. confirm the failed session is deterministic by retrying or probing it once;
2. probe the remaining calibration cases, and then a sample of the full LongMemEval-S set, for content-filter risk;
3. verify which turn(s) in the failed session trigger filtering;
4. only then decide whether filtered sessions are skipped, redacted, retried with a safer extraction prompt, or counted as operational failures.

Any policy that prevents deterministic content-filter failures from reaching DLQ changes the original stop rule. That change must be explicit in the next manifest.

Potential policy options:

- classify `content_filter` failures separately from app bugs;
- skip-and-count filtered sessions if the rate is very low;
- redact or summarize blocked source turns if the rate is material;
- count content-filter cases as operational failures if the benchmark must remain unmodified.

Until that policy exists, a full LongMemEval run would produce misleading operational failures and likely halt early.
