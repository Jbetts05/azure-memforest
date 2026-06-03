# Phase 2 intended-stack pilot post-run review

Run suffix: `live-intended-20260603032131`

## Outcome

The 10-case pilot ran end to end on the intended stack and reverted to the small safe baseline afterward, but the pilot **did not meet the advancement gate** for the 50-case subset.

Operational positives:

- 10/10 cases reached a terminal case result.
- 0 new Service Bus DLQ messages.
- 0 answer-bearing content-filter blocks.
- 0 residue-contamination events.
- API and Durable Function App were reverted to the safe small-stack baseline.
- Artifact secret scan found no Durable key/query-string leaks.

Gate failures:

1. **Quality gate failed.**
   - Calibration primary-correct: `7/10`, below the `8/10` threshold.
   - Pipeline-eligible primary-correct: `6/9`, below the `7/9` threshold.
2. **Durable/event accounting gate failed.**
   - Durable completed instances: `98`.
   - Successful ingest events: `97`.
   - One non-answer-bearing ingest in case `b320f3f8` failed with an Azure OpenAI content-filter error, while its Durable instance completed.

## Quality misses to diagnose

| Case | Type | Observed answer | Failure mode |
| --- | --- | --- | --- |
| `195a1a1b` | single-session-preference | Evening activity suggestions | Primary judge marked false, cross-check true; likely judge sensitivity or answer completeness issue. |
| `gpt4_d6585ce8` | temporal-reasoning | Listed 3 musical events | Retrieval/answer aggregation missed two expected events and ordering. |
| `gpt4_70e84552_abs` | temporal-reasoning / abstention | `Fixing the fence.` | Abstention failure: answer should state the information is not sufficient because the cow purchase was absent. |

## Durable/event accounting discrepancy

Case `b320f3f8`, source session `sharegpt_S8gExpS_35`, failed during ingest with an Azure OpenAI content-filter error:

```text
content_filter / ResponsibleAIPolicyViolation
```

The skipped session was non-answer-bearing, and the case still answered correctly from remaining evidence. However, the aggregate correctly kept the pilot gate failed because a completed Durable instance paired with a failed ingest event needs an explicit accounting decision or event-status fix before scaling.

## Decision

Do **not** advance to the 50-case subset yet.

Next work should diagnose:

1. the `b320f3f8` Durable/event accounting discrepancy;
2. the `gpt4_d6585ce8` retrieval/completeness miss;
3. the `gpt4_70e84552_abs` abstention failure;
4. the `195a1a1b` primary/cross judge disagreement.

This run should be reported as:

> Phase 2 intended-stack pilot completed end-to-end with no DLQ growth or residue contamination, but it failed the precommitted advancement gate on quality and one Durable/event accounting discrepancy.
