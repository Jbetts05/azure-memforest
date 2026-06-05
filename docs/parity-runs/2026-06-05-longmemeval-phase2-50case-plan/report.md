# LongMemEval-S Phase 2 held-out 50-case subset plan

Run date: 2026-06-05

## Purpose

Plan the next validation step after the successful Phase 2 10-case intended-stack pilot.

This is a planning artifact only. It does **not** authorize a live 50-case run.

## Source and selection

- Source: LongMemEval-S cleaned.
- Source SHA-256: `d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442`.
- Selection seed: `20260530`.
- Reproducibility script: `select_cases.py`.
- Case index: `cases_index.json`.

The plan separates two cohorts:

1. **Held-out 50**: fully excludes the successful 10 pilot cases and forms the only headline generalization denominator.
2. **Regression 10**: re-runs the successful pilot cases only to protect the fixes and calibration behavior.

Do not report a combined `60-case` or mixed `50-case` quality number.

## Held-out distribution

The held-out 50 uses largest-remainder allocation against the full 500-case distribution:

| Question type | Full count | Held-out target |
| --- | ---: | ---: |
| `knowledge-update` | 78 | 8 |
| `multi-session` | 133 | 13 |
| `single-session-assistant` | 56 | 6 |
| `single-session-preference` | 30 | 3 |
| `single-session-user` | 70 | 7 |
| `temporal-reasoning` | 133 | 13 |

Per-type counts are for transparency. They are too small for per-type quality claims, especially `single-session-preference` with `n=3`.

## Sharding

The held-out cohort is split into five deterministic shards of 10 cases. Shards are roughly balanced by question type, but shard-level quality is diagnostic only. The pass/fail gate applies to the aggregate held-out 50.

## Gates before a live run

Before any live run:

1. Current code, manifest, and case index must be committed.
2. API and Durable active settings must match the intended GPT-5-family stack.
3. Cosmos and Storage dev-run public network posture must be verified immediately before launch.
4. Durable memory and budget settings must be captured.
5. DLQ baseline must be snapshotted and attributed.
6. Rubber-duck 4.8 must validate the launch plan.
7. A cross-family judge or human-adjudication sample must be defined.

## Held-out pass gate

For the held-out 50:

- 50/50 cases attempted and completed.
- 0 answer-bearing content-filter blocks.
- No case-targeted fixes or overrides may be introduced into the held-out numerator.
- Non-answer content-filter skips are capped at 5 and must be explicitly accounted.
- The denominator is fixed at 50. Non-answer skips count as not-correct for the `>=40/50` gate.
- Primary-correct rate at least 80% (`>=40/50`).
- Judge disagreements at most 10% (`<=5/50`).
- 0 new DLQ messages.
- 0 Durable failed instances.
- Durable accounting gate passes.
- 0 provenance residue contamination events.

A near miss below `40/50` primary-correct, or above `5/50` disagreements, is a no-go for scaling. Preserve the failed artifact and perform root-cause analysis; do not renegotiate caps, thresholds, selection, or denominator after seeing results.

## Regression gate

The 10 pilot cases should remain a separate regression cohort:

- 10/10 cases completed.
- At least 9/10 primary-correct.
- Known fixed cases must pass: `195a1a1b`, `gpt4_d6585ce8`, `gpt4_70e84552_abs`, `e3038f8c`, `e66b632c`.

The regression cohort should run with the pilot fixes and documented overrides enabled. A regression miss blocks advancement even if the held-out cohort passes, because it means the successful pilot behavior is no longer preserved.

## Residue policy

The 10-case pilot's in-run residue probes were unavailable because they queried users before committed manifests existed. The post-run adjacent-pair re-probe provided useful evidence but required provenance review.

For the held-out run:

- Run post-materialization residue probes for every adjacent pair and every shard boundary.
- Capture machine-readable provenance for raw non-refusal responses.
- Manual reclassification is not allowed for gate pass/fail.
- Any raw non-refusal must be cleared by the pre-registered automated provenance rule.

## Caveats

- This remains non-official and bounded by the pilot session-slice caps.
- The judge stack is same-family with the answer model unless an additional control is defined before launch.
- The pilot's case-targeted fixes should be treated as regression protection, not proof of generalization.
- Runtime posture must not change mid-shard. If posture changes are required, abort and restart that shard with a new suffix.
