# LongMemEval-S Phase 2 intended-stack pilot plan

Run date: 2026-06-03

## Purpose

Plan the Phase 2 10-case pilot after Phase 1 passed on the intended GPT-5-family stack.

This is an **internal intended-stack pilot**, not an official LongMemEval-S score. It uses LongMemEval-S cases, but the sample is only 10 cases, uses bounded haystack slices, and uses a GPT-5-family judge rather than the upstream GPT-4o-era judge.

## Stack to validate

| Role | Deployment |
| --- | --- |
| Default / extraction / dedupe / planner / browse | `gpt-5.4-mini-memory` |
| Answer | `gpt-5.4` |
| Judge primary | `gpt-5.4` |
| Judge cross-check | `gpt-5.4-mini-memory` |
| Embedding | `text-embedding-3-large` |
| Search index | `memforest-recall-large` |
| Ingest model-call budget multiplier | `20` |

The current materialization path still resolves some summarization work through the default model. Pilot artifacts must report observed telemetry by role/deployment and must not overclaim that every summary used `gpt-5.4`.

## Case selection

The pilot reuses the deterministic 10-case selection from the pre-Phase-1 scored batch:

```text
4b24c848
8e91e7d9
fea54f57
195a1a1b
b320f3f8
gpt4_d6585ce8
e3038f8c
e66b632c
gpt4_70e84552_abs
6e984302
```

Rationale:

1. The selection was made before the Phase 1 fixes, avoiding post-hoc reselection bias.
2. It covers all six LongMemEval-S question types.
3. It intentionally weights the dominant full-set classes: temporal reasoning, multi-session, and knowledge update.
4. It preserves the prior 10-case scale attempt's input shape, while changing the execution path to Durable Functions and the intended model/index stack.

## Distribution

| Question type | Full count | Full % | Pilot count | Pilot % |
| --- | ---: | ---: | ---: | ---: |
| `multi-session` | 133 | 26.6 | 2 | 20.0 |
| `temporal-reasoning` | 133 | 26.6 | 3 | 30.0 |
| `knowledge-update` | 78 | 15.6 | 2 | 20.0 |
| `single-session-user` | 70 | 14.0 | 1 | 10.0 |
| `single-session-assistant` | 56 | 11.2 | 1 | 10.0 |
| `single-session-preference` | 30 | 6.0 | 1 | 10.0 |

The distribution is acceptable for a pilot gate, not for a quality claim.

## Advancement gate

Advance to the 50-case subset only if all of the following hold:

- 10/10 cases complete.
- At least 8 cases remain eligible for unmodified pipeline accuracy.
- At least 7 eligible unmodified cases have correct primary judge labels.
- Eligible unmodified judge disagreements are no more than 1.
- At least 8/10 calibration labels are correct when the manually minimized calibration case is included.
- Calibration judge disagreements are no more than 1.
- 0 new DLQ messages.
- 0 Durable failures.
- 0 answer-bearing content-filter blocks.
- 0 residue-contamination events.

Cases `8e91e7d9` and `195a1a1b` are retained as calibration/operational cases, but excluded from unmodified pipeline-quality counts. `8e91e7d9` uses evidence minimization with synthetic sibling-total evidence; `195a1a1b` uses assistant-advice context extraction.

## Final outcome

Final run: `case_results_full-intended-final-20260604232856/`

The final run met the advancement gate:

- 10/10 cases completed.
- Calibration primary-correct: 10/10.
- Pipeline primary-correct: 8/8 over the eligible denominator.
- Judge disagreements: 0.
- New DLQ delta: 0.
- Durable completed/total: 98/98; Durable failed: 0.
- Answer-bearing content-filter blocks: 0.
- Residue contamination events: 0 after post-run provenance review.

Decision: advance to 50-case subset planning. This remains an internal pilot signal, not an official LongMemEval-S score.

## Abort rules

- Abort on any new Service Bus DLQ message.
- Abort on any Durable instance ending `Failed`, `Terminated`, or `Canceled`.
- Abort on any answer-bearing content-filter block.
- Abort on repeated model 429/rate-limit failures after configured retries.
- Abort if a case exceeds 90 minutes without checkpoint progress.
- Abort if a residue probe shows cross-user contamination.

## Next implementation slice

Adapt the Phase 1 Durable harness into a Phase 2 pilot harness that reads `pilot_cases.json`, preserves checkpoint/resume, uses the hardened Durable status polling path, records intended-stack metadata, and has local/static tests before any live run.
