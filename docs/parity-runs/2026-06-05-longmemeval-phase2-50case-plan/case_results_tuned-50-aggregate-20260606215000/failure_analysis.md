# Tuned held-out 50-case failure analysis

## Scope

This analysis covers the tuned held-out run aggregated in `aggregate_heldout_shards.json`.
The run used the tuned projection-write posture deployed at `7241006b1643`.

This is an internal bounded LongMemEval-S held-out artifact, not an official LongMemEval score.

## Aggregate outcome

- Cases completed: 50/50
- Primary-correct: 37/50
- Judge disagreements: 3
- Filtered skips: 1
- Durable failures: 0
- DLQ delta: 0
- Raw residue non-refusals: 2
- Provenance contamination events: 0

The tuned write path solved the operational timeout blocker: all shard writes completed with zero Durable failures.
The remaining issues are quality, answer strictness, and residue refusal calibration.

## Failure buckets

| Bucket | Cases | Count | Interpretation |
|---|---:|---:|---|
| Assistant-origin answer detail absent | `1de5cff2`, `6ae235be`, `70b3e69b`, `dc439ea3`, `cc539528`, `8464fc84` | 6 | Gold answers live in assistant responses. Current extraction policy usually skips assistant content, so retrieval contains the user's request but not the assistant's answer detail. |
| Multi-fact recall/count/reasoning miss | `2ce6a0f2`, `gpt4_d12ceb0e`, `b46e15ed` | 3 | Relevant facts were partially retrieved, but the answer required collecting all components or computing over multiple sessions. |
| Temporal reference/calculation miss | `gpt4_af6db32f`, `0bc8ad92` | 2 | Relevant dated facts were retrieved, but the answerer used the wrong reference date or rounded/computed the interval incorrectly. |
| Judge/format ambiguity | `3b6f954b`, `80ec1f4f_abs` | 2 | The answer was arguably acceptable or close; one pro-judge sided with correctness (`3b6f954b`), while `80ec1f4f_abs` shows a bare `0` answer was not accepted by the primary/pro judge for an abstention-style gold. |

## Per-case notes

### `2ce6a0f2` — art event count

- Gold: `4`
- Answer: `2`
- Retrieved facts included `Women in Art` and `The Evolution of Street Art`, plus art-event intent facts.
- Likely failure: incomplete event enumeration and attendance filtering. The system retrieved some art-event evidence but did not collect all distinct attended events across sessions.

### `1de5cff2` — Veja wild-rubber brand

- Gold: `Veja`
- Answer: `The information provided is not enough to answer this question.`
- Retrieved facts captured the user's sustainable-fashion interest but not the assistant's specific brand recommendation.
- Likely failure: assistant-origin answer detail was skipped by policy.

### `6ae235be` — CITGO Lake Charles processes

- Gold: atmospheric distillation, FCC, alkylation, hydrotreating
- Answer: not enough information.
- Retrieved facts captured that the user asked about CITGO refinery processes, but not the assistant's actual process list.
- Likely failure: assistant-origin answer detail was skipped by policy.

### `70b3e69b` — Manolo Garcia

- Gold: `Manolo Garcia`
- Answer: not enough information.
- Retrieved facts captured the user's interest in Catalan politics/literature/music, but not the assistant's named example.
- Likely failure: assistant-origin answer detail was skipped by policy.

### `3b6f954b` — University of Melbourne

- Gold: `University of Melbourne in Australia`
- Answer: `The University of Melbourne.`
- Cross-check and pro-judge both labeled the answer correct; the primary judge labeled it incorrect.
- Likely failure: primary judge strictness/format issue, not memory retrieval.

### `80ec1f4f_abs` — December museums/galleries count

- Gold: `0` / no museum visit mentioned in December.
- Answer: `0`
- Cross-check accepted, primary and pro-judge rejected.
- Likely failure: answer-format/abstention-gold ambiguity. A bare `0` may be too terse for the judge prompt even when numerically aligned.

### `gpt4_d12ceb0e` — average age

- Gold: `59.6`
- Answer: not enough information.
- Retrieved facts included the user's age, mother age, and both grandparents' ages, but not enough evidence in top facts for the full requested set.
- Likely failure: multi-fact arithmetic recall/composition. One required component appears missing from the retrieved context, causing abstention.

### `dc439ea3` — powwow Hoop Dance

- Gold: `Hoop Dance`
- Answer: not enough information.
- Retrieved facts captured user questions about traditional games at powwows, but not the assistant's specific answer.
- Likely failure: assistant-origin answer detail was skipped by policy.

### `gpt4_af6db32f` — Super Bowl days ago

- Gold: 17 or 18 days ago.
- Answer: `0 days ago`
- Retrieved facts include Super Bowl-related evidence with a source date, but the answerer appears to have used the wrong reference/event date relationship.
- Likely failure: temporal interval calculation/reference-date handling.

### `cc539528` — back-end languages

- Gold: Ruby, Python, or PHP.
- Answer: not enough information.
- Retrieved facts captured the user's full-stack/backend interest, but not the assistant's recommended language list.
- Likely failure: assistant-origin answer detail was skipped by policy.

### `b46e15ed` — consecutive charity events

- Gold: `2` months.
- Answer: not enough information.
- Retrieved facts included some charity events, but not the needed consecutive-day pair or enough context to compute the interval.
- Likely failure: temporal multi-fact recall and interval reasoning.

### `8464fc84` — Vatican deli

- Gold: `Roscioli`
- Answer: not enough information.
- Retrieved facts captured Vatican/Rome visit planning, but not the assistant's specific deli recommendation.
- Likely failure: assistant-origin answer detail was skipped by policy.

### `0bc8ad92` — months since museum visit with friend

- Gold: `5`
- Answer: about `4` months.
- Retrieved facts included the Science Museum visit with a friend and later museum-related visits.
- Likely failure: temporal reference/date calculation and selection of the correct "last visited with a friend" event.

## Residue events

### `heldout_3` adjacent probe into `545bd2b5`

- Probe question came from previous case `1d4e3b97`: bike performance during Sunday group rides.
- Response answered from current user context about a front tire.
- Returned forest version did not overlap previous forest version.
- Interpretation: raw non-refusal, but not provenance contamination. This is a topical coincidence/current-user retrieval problem.

### `heldout_4` adjacent probe into `ec81a493`

- Probe question came from previous case: recent publications/conferences.
- Response answered from current user context about automation/CI and marketing trends.
- Returned forest version did not overlap previous forest version.
- Interpretation: raw non-refusal, but not provenance contamination. This is current-user retrieval plus insufficient refusal strictness.

## Recommended next work

1. Treat the operational pipeline as substantially improved: the timeout blocker is resolved by tuned projection writes.
2. Do not change the held-out denominator or reclassify the strict gate: the run remains `gate_failed`.
3. Prioritize a product decision on assistant-origin memory:
   - If assistant answers should be recallable, add a bounded, explicit extraction policy for user-requested assistant answers.
   - If not, accept that many single-session-assistant cases are out of scope for this memory policy.
4. Improve multi-fact/temporal reasoning without case-specific tuning:
   - increase diversity or recall width for count/aggregate/temporal questions;
   - make the answerer cite all counted facts before answering;
   - strengthen reference-date handling for elapsed-time questions.
5. Tighten residue refusal behavior:
   - if a probe question is unrelated to the current user's facts, prefer refusal even when topically similar facts exist;
   - keep provenance tracing, since both raw non-refusals here were current-user retrieval rather than cross-user contamination.

