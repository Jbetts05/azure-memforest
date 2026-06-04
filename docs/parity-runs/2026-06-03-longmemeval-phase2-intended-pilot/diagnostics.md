# Phase 2 pilot diagnostics

Diagnostic image: `memforestexampleacr.azurecr.io/azure-memforest-api:11cd808d1249`

Query-only artifact: `query_only_diagnostics_11cd808d1249.json`

## Durable/event accounting

The `b320f3f8` accounting mismatch is explained by one non-answer-bearing source session:

```text
source_session_id: sharegpt_S8gExpS_35
reason: content_filter / ResponsibleAIPolicyViolation
answer_bearing: false
```

The raw ingest event should remain failed. The harness now records this as a soft skipped non-answer ingest for Durable accounting using:

- `skipped_non_answer_ingest_event_count`
- `durable_accounted_event_count`

This preserves the raw failed event while preventing a completed Durable orchestration for an intentional non-answer skip from being misclassified as an unaccounted Durable mismatch.

## Query-only diagnostics

The query-only pass used the already-materialized Phase 2 users and changed only cloud query behavior.

### `gpt4_70e84552_abs`

Original pilot answer:

```text
Fixing the fence.
```

Diagnostic answer:

```text
The information provided is not enough to answer this question.
```

Interpretation: the minimal cloud answer prompt with partial-evidence abstention addresses this failure mode.

### `gpt4_d6585ce8`

Original pilot answer:

```text
1. Music festival in Brooklyn
2. Billie Eilish show in Philly
3. Queen with Adam Lambert at the Prudential Center in Newark, NJ
```

Diagnostic answer:

```text
Free outdoor concert series in the park (March 25, 2023) → music festival in Brooklyn → Billie Eilish show in Philly → Queen concert with my parents.
```

Cosmos fact inspection confirmed all five expected event facts were extracted, including Billie Eilish, outdoor concert, Brooklyn music festival, jazz night, and Queen/Adam Lambert.

The query-only fix improved recall coverage by adding the outdoor concert, but it still missed jazz night and misordered Billie relative to the March 25 and April 1 events. Root cause remains retrieval/rerank/temporal composition, not extraction.

### `195a1a1b`

Diagnostic answer remained meditation-focused and did not mention the negative phone/TV preference:

```text
Yes — good evening activities for you would be calming ones, especially a guided meditation or using a meditation app before bed...
```

Cosmos fact inspection found no extracted facts containing `phone`, `TV`, or `sleep quality` for the synthetic user, while 9:30 pm and meditation facts were present. Root cause is extraction/fact coverage for negative preferences, with possible judge sensitivity secondary.

## Decision

Do not rerun the full Phase 2 pilot yet.

The abstention issue has a plausible query-side fix, but the temporal completeness and negative-preference extraction gaps remain unresolved. Next work should focus on:

1. improving retrieval/composition for ordered event-list questions;
2. improving extraction or fallback coverage for negative preferences;
3. then running targeted query/ingest diagnostics before a full 10-case rerun.

## Negative-preference follow-up

Artifact:

- `case_results_negative-pref-195-20260603172248/04_195a1a1b.json`
- `case_results_negative-pref-marker-195-20260603202112/04_195a1a1b.json`

Findings:

- The harness override successfully retained `answer_6dc4305e:5` as extractable assistant advice context rather than synthetic user text.
- The extracted facts still did not include `phone`, `TV`, `screens`, `melatonin`, or `sleep quality`.
- The answer remained meditation-focused and both judges still disagreed (`primary=false`, `cross=true`).

Interpretation:

- Allowing the assistant turn through extraction is not enough; the extraction prompt needs stronger, scoped instructions for `assistant_advice_context=true`.
- This case remains excluded from unmodified pipeline accuracy because the gold evidence lives in assistant advice.

Follow-up validation on image `093f448d4cc2`:

- The full targeted `195a1a1b` run completed with 9/9 Durable instances completed, 0 DLQ delta, and 0 content-filter skips.
- The marked assistant-advice turn extracted the concrete negative screen fact: `phones, tablets, laptops, or TVs`.
- The answer avoided screens for the last 30 minutes before bed and recommended relaxing evening activities.
- Both judges passed (`primary=true`, `cross=true`).

Interpretation:

- The assistant-advice marker plus extraction prompt refinement fixes this targeted failure mode.
- The case should remain excluded from unmodified pipeline accuracy because the calibration evidence is assistant-origin and exposed through a harness override.
- This is a targeted modified-input calibration result, not proof that the unmodified pipeline would pass the case.
- Before any full 10-case rerun, reconfirm this result together with the temporal-event and abstention fixes so retrieval/query changes are tested for cross-interactions.
- Combined targeted validation later passed this case again, but the extracted fact regressed from explicit `phones, tablets, laptops, or TVs` to the broader `screens`. The answer and judges still passed, but the prompt was hardened again to require parenthetical examples and sleep-impact rationale when marked assistant advice includes them.

Hardened-prompt validation on image `29aa470632d6`:

- Artifact: `case_results_negative-pref-hardened-195-20260603214651/04_195a1a1b.json`
- The run used the intended stack: `gpt-5.4-mini-memory` for ingest and `text-embedding-3-large` for embeddings.
- Operationally it completed cleanly: 9/9 ingests succeeded, 9/9 Durable instances completed, 0 DLQ delta.
- Extraction preserved the concrete fact: `avoiding screens (phones, tablets, laptops, or TVs)`.
- Both judges failed because the final answer included unrelated dance-event suggestions and did not explicitly express the phone/TV avoidance rationale.

Interpretation:

- The remaining `195a1a1b` failure is answer-context selection/composition noise, not extraction coverage or model routing.
- The next fix should prioritize direct evening/sleep/relaxation/avoid-screen preference facts for forward-looking activity-suggestion questions while keeping this case excluded from unmodified pipeline accuracy.

Preference-context validation on image `ccf55b968111`:

- Artifact: `case_results_negative-pref-context-195-20260603223433/04_195a1a1b.json`
- The run used the intended stack and completed cleanly: 9/9 ingests succeeded, 9/9 Durable instances completed, 0 DLQ delta.
- The final context included the explicit negative fact: `avoiding screens (phones, tablets, laptops, or TVs)`.
- The answer recommended a calming wind-down routine, screen-free activities, Headspace's `Sleep`, and winding down by 9:30 pm.
- Both judges passed (`primary=true`, `cross=true`).

Remaining caveat:

- One lower-ranked dance-class fact still appeared in the returned context tail, but the answer no longer used it. Combined targeted validation should confirm this does not interfere with the temporal-event and abstention fixes.

Combined targeted validation on image `ccf55b968111`:

- Artifact: `case_results_targeted-fixed-3-context-20260603224910/aggregate.json`
- Operationally clean: 3/3 cases completed, 31/31 Durable instances completed, 0 DLQ delta, 0 content-filter skips.
- Pipeline-eligible cases passed:
  - `gpt4_d6585ce8`: all five musical events in order.
  - `gpt4_70e84552_abs`: insufficient-information abstention.
- `195a1a1b` remained modified-input calibration only and had a judge disagreement (`primary=false`, `cross=true`).

Interpretation:

- Context selection now surfaces the right negative-preference facts, but answer wording still collapsed the explicit phone/TV evidence to `screens` and omitted the sleep-quality rationale.
- The next narrow slice is answer-generation fidelity for preference/recommendation/activity questions: preserve concrete avoidance examples and stated reasons when they are present in memory context, while keeping abstention rules unchanged.

Answer-fidelity validation on image `4e4ac0bf6a71`:

- Artifact: `case_results_negative-pref-answer-195-20260603234938/04_195a1a1b.json`
- The run used the intended stack and completed cleanly: 9/9 ingests succeeded, 9/9 Durable instances completed, 0 DLQ delta.
- The answer explicitly recommended relaxing evening activities, winding down by 9:30 pm, and avoiding `phones, tablets, laptops, or TVs` for the last 30 minutes before bed.
- Both judges passed (`primary=true`, `cross=true`).

Decision:

- `195a1a1b` is ready for another combined targeted validation, still as modified-input calibration only.

Combined answer-fidelity validation on image `4e4ac0bf6a71`:

- Artifact: `case_results_targeted-fixed-3-answer-20260604000326/aggregate.json`
- Operationally clean: 3/3 cases completed, 31/31 Durable instances completed, 0 DLQ delta, 0 content-filter skips, 0 Durable failures.
- All judges passed:
  - `195a1a1b`: primary/cross true, still modified-input calibration only.
  - `gpt4_d6585ce8`: primary/cross true, all five musical events in order.
  - `gpt4_70e84552_abs`: primary/cross true, insufficient-information abstention.

Decision:

- The three diagnosed Phase 2 quality failures are now fixed in targeted validation.
- Next step is rubber-duck review of readiness for a full 10-case intended-stack pilot rerun.

## Full rerun abort: `full-intended-rerun-20260604003854`

Artifact:

- `case_results_full-intended-rerun-20260604003854/`

Outcome:

- The run is operationally invalid and must not be counted as a Phase 2 quality result.
- Case `4b24c848` completed and passed before the abort.
- Case `8e91e7d9` reached its final answer-bearing ingest but did not complete the case.
- Cases 3-10 were not reached.

Root causes:

1. Cosmos firewall blocked the Container Apps egress IP `192.0.2.8`, causing a 500 during checkpoint resume.
2. A mid-run Durable package deployment introduced a changed orchestrator while a run checkpoint was still active. The final case-2 ingest then failed with Durable replay nondeterminism:

```text
Non-Deterministic workflow detected: A previous execution of this orchestration scheduled an activity task with sequence ID 0 and name 'materialize_write_job'...
```

Follow-up fix:

- Added an idempotent retry for transient API-to-Durable start timeouts in `DurableJobStarter`; retries reuse the same Durable `instance_id`, so a duplicate start conflict remains safe.
- Cosmos is configured to allow the current Container Apps egress IP `192.0.2.8`.
- Storage public network access was temporarily opened for Function/Kudu deployment; the next run must record the exact storage network posture used.

Next rerun requirements:

- Start a fresh 10-case run; do not resume `full-intended-rerun-20260604003854`.
- Use stable image/package `f732d22d8239` with no mid-run deployments.
- Use a fresh Durable task hub for the rerun so old orchestration history cannot replay under changed code.
- Keep reporting headline unmodified-pipeline accuracy separately from modified-input calibration.

## Fresh rerun abort: `full-intended-clean-20260604030439`

Artifact:

- `case_results_full-intended-clean-20260604030439/`

Outcome:

- The run is operationally invalid and must not be counted as a Phase 2 quality result.
- The run used a captured environment preflight with:
  - API image `f732d22d8239`;
  - fresh Durable hub `MemForestWritesP2Rerun20260604A`;
  - intended model/search stack;
  - Cosmos public access restricted to `192.0.2.8`;
  - Storage public/default allow documented as a dev-run caveat.
- It failed on the first ingest with Durable replay nondeterminism before materialization progressed.

Additional smoke after deterministic orchestrator change:

- Code change `efbe4df544e7` simplified `write_orchestrator` to a single `materialize_write_job` activity and made transient handler errors terminal inside the activity.
- A one-ingest smoke on fresh hub `MemForestWritesSmoke20260604B` still failed immediately with the same Durable replay nondeterminism.
- The deployed Flex package was inspected directly from the `durable-package-memforestexample/released-package.zip` blob and confirmed to contain the simplified single-activity orchestrator.
- A follow-up canonical-yield syntax change (`result = yield ...; return result`) was deployed and smoke-tested on fresh hub `MemForestWritesSmoke20260604C`; it also failed immediately with the same Durable replay nondeterminism and was reverted.
- Application Insights showed the underlying activity failures were Cosmos firewall denials from changing Function outbound IPs, including `135.222.140.49`, `192.0.2.7`, and `20.15.73.80`.

Interpretation:

- The remaining blocker is Function/Cosmos network egress stability, not Phase 2 quality logic.
- Do not run another full Phase 2 pilot until Function access to Cosmos is stable enough for a one-ingest fresh-hub smoke to complete successfully.
- Further orchestrator syntax changes or host recreation are not justified unless the network issue is first eliminated.

## Temporal fallback follow-up

Artifacts:

- `query_only_temporal_a552ada7aaf1.json`
- `query_only_temporal_86196df38172.json`
- `query_only_temporal_f7ca1cb4b37f.json`

Findings:

- The first temporal fallback attempt regressed to `I don't know`.
- The corrected query-expansion attempt still returned `I don't know`.
- The direct event-fact fallback attempt on `f7ca1cb4b37f` consistently returned only Billie Eilish facts across three repeated query-only checks.
- The refined temporal-context attempt on `62149d58ba89` returned all five expected events in chronological order across three repeated query-only checks.

Interpretation:

- The temporal failure is now localized to fallback retrieval/selection for event facts.
- Answer composition is using the provided context, but the context is still incomplete.
- Further work should add instrumentation for fallback query strings, raw candidate counts, location-filter drops, and selected temporal facts before another fix attempt.

Follow-up caveats:

- The fix is validated for the `gpt4_d6585ce8` event-list failure, not every possible event-list phrasing.
- The event selector intentionally requires "happened" language such as attended, saw, went to, got back from, or enjoyed; future cases may need additional synonyms.
- Event-list questions can include more facts than the caller's requested `max_facts` so the answer model receives the complete chronological event set.

## Combined targeted validation

Artifact:

- `case_results_targeted-fixed-3-20260603203620/aggregate.json`

Findings:

- The three fixed failure modes completed together on image `093f448d4cc2`.
- Cases:
  - `195a1a1b`: passed as modified-input calibration only; the answer avoided screens, but the fact text did not preserve `phones/tablets/laptops/TVs` in this repeat.
  - `gpt4_d6585ce8`: passed with all five expected musical events in the expected order.
  - `gpt4_70e84552_abs`: passed with an insufficient-information abstention.
- Aggregate:
  - 3/3 cases completed.
  - primary/cross judges passed for all 3.
  - 31/31 Durable instances completed.
  - 0 DLQ delta, 0 content-filter skips, 0 Durable failures.

Decision:

- The temporal-event and abstention fixes are ready for the next full-pilot rerun.
- The negative-preference fix needs one more targeted validation on the hardened assistant-advice prompt because exact concrete examples were not stable across repeats.
