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

Findings:

- The harness override successfully retained `answer_6dc4305e:5` as extractable assistant advice context rather than synthetic user text.
- The extracted facts still did not include `phone`, `TV`, `screens`, `melatonin`, or `sleep quality`.
- The answer remained meditation-focused and both judges still disagreed (`primary=false`, `cross=true`).

Interpretation:

- Allowing the assistant turn through extraction is not enough; the extraction prompt needs stronger, scoped instructions for `assistant_advice_context=true`.
- This case remains excluded from unmodified pipeline accuracy because the gold evidence lives in assistant advice.

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
