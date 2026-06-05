# Phase 2 intended-stack pilot post-run review

Run suffix: `full-intended-final-20260604232856`

## Outcome

The final 10-case Phase 2 intended-stack pilot **met the advancement gate** for 50-case planning.

This remains an internal pilot, not an official LongMemEval-S score. It uses a 10-case deterministic sample, bounded haystacks, two calibration-only cases, and GPT-5-family judges.

## Aggregate

| Metric | Result |
| --- | ---: |
| Attempted cases | 10/10 |
| Completed cases | 10/10 |
| Calibration primary-correct | 10/10 |
| Judge disagreements | 0 |
| Pipeline-eligible cases | 8 |
| Pipeline primary-correct | 8/8 |
| Pipeline judge disagreements | 0 |
| New DLQ delta | 0 |
| Durable completed / total | 98/98 |
| Durable failed | 0 |
| Accounted non-answer content-filter skips | 1 |
| Answer-bearing content-filter blocks | 0 |
| Residue contamination events | 0 |
| Advance to 50-case planning | true |

## Important caveats

1. `8e91e7d9` and `195a1a1b` are calibration-only and excluded from unmodified pipeline accuracy. The gate denominator is therefore 8 pipeline-eligible cases.
2. Case `b320f3f8` had one non-answer-bearing content-filter skip. It is explicitly accounted for and did not block the answer.
3. The run required checkpoint resumes after a harness stale-polling false negative and after Storage/Cosmos public network posture was restored. The final aggregate is computed from reconciled job/Durable evidence.
4. Post-run adjacent-pair residue re-probing produced one raw non-refusal answer, but provenance review classified it as current-user retrieval, not previous-user contamination. Provenance contamination events: 0.
5. The pilot's quality fixes are case-targeted and must be treated as regression protection, not as proof of broad generalization.

## Runtime cleanup

After preserving artifacts:

- API was reverted to `servicebus`, safe small dev chat/embedding deployments, `memforest-recall`, and benchmark auth disabled.
- Durable Flex memory was reverted to 2048 MB.
- Durable settings were reverted to the small dev model/index posture.
- Cosmos and Storage remain in the documented dev-unblocked public network posture used for these benchmark windows.

## Decision

Advance to **50-case subset planning**, with explicit caveats around the 8-case pipeline denominator, adjacent-pair post-run residue review, case-targeted fixes, and temporary dev-run infrastructure posture.
