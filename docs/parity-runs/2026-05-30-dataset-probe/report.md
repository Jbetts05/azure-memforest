# Dataset acquisition probe for Azure-MemForest evaluation

Run date: 2026-05-30

## Goal

Before building a full evaluation suite, verify whether established long-memory datasets can be obtained and normalized into Azure-MemForest replay cases.

## Sources found

| Dataset | Candidate source | Status | Notes |
| --- | --- | --- | --- |
| LongMemEval | `xiaowu0162/longmemeval-cleaned` on Hugging Face | Usable | MIT-tagged dataset repo. Contains `longmemeval_s_cleaned.json`, `longmemeval_m_cleaned.json`, and `longmemeval_oracle.json`. |
| LongMemEval | `xiaowu0162/longmemeval` on Hugging Face | Usable | MIT-tagged dataset repo. Contains raw-looking split files/directories. |
| LoCoMo | `Aman279/Locomo` on Hugging Face | Partially usable | Public CSV mirror with `dialogue_id` and `turns`; no QA fields in this mirror. |
| LoCoMo | `HuggingFaceH4/locomo` on Hugging Face | Not accessible in this probe | HF API returned 401. May require auth/gated access or may not be the canonical source. |

Search/metadata artifacts:

- `hf_dataset_search.json`
- `hf_dataset_repos.json`
- `download_summary.json`

## Samples downloaded

| File | Source | Notes |
| --- | --- | --- |
| `longmemeval_s_cleaned_sample.json` | HF `xiaowu0162/longmemeval-cleaned` | First 5 MB only, enough to inspect initial records. |
| `locomo_sample.csv` | HF `Aman279/Locomo` | Entire public CSV mirror downloaded, about 2.1 MB. |

## Schema findings

### LongMemEval cleaned

Observed keys:

- `question_id`
- `question_type`
- `question`
- `question_date`
- `answer`
- `answer_session_ids`
- `haystack_dates`
- `haystack_session_ids`
- `haystack_sessions`

This is directly suitable for replay:

```json
{
  "case_id": "question_id",
  "sessions": [
    {
      "session_id": "haystack_session_ids[i]",
      "turns": "haystack_sessions[i]",
      "metadata": {"date": "haystack_dates[i]"}
    }
  ],
  "question": "question",
  "gold_answer": "answer",
  "question_type": "question_type"
}
```

The first sampled LongMemEval case normalized to **550 turns** across its haystack sessions, so it is a realistic long-memory replay target.

### LoCoMo mirror

Observed columns:

- `dialogue_id`
- `turns`

This mirror is useful for ingest/retrieval smoke tests over long dialogues, but not enough for answer-quality evaluation unless we pair it with QA/gold-answer files from the official LoCoMo repository or another dataset source.

## Normalized sample

`normalized_sample.json` contains:

- 2 normalized LongMemEval cases
- 2 normalized LoCoMo dialogues

The normalized shape is intentionally close to the replay format we would use for Azure-MemForest:

```json
{
  "case_id": "...",
  "question": "...",
  "gold_answer": "...",
  "question_type": "...",
  "sessions": [
    {
      "session_id": "...",
      "turns": [
        {
          "role": "...",
          "content": "...",
          "timestamp": "...",
          "content_id": "..."
        }
      ]
    }
  ]
}
```

## Feasibility assessment

LongMemEval is the best immediate target for a full eval harness because it already contains sessions, questions, and gold answers in one cleaned JSON.

LoCoMo needs one more acquisition step: find the official QA annotations/gold answers. The public `Aman279/Locomo` mirror appears to include only dialogue turns.

## Recommended next step

Build a 5-case LongMemEval-S smoke runner:

1. Load first 5 normalized cases from `longmemeval_s_cleaned.json`.
2. For each case:
   - clear test user
   - ingest haystack sessions
   - wait for jobs
   - query question
   - fetch trace and facts
   - judge answer against `answer`
3. Store per-case JSON outputs.
4. Only after this works, add:
   - local MemForest runner
   - flat-fact baseline
   - transcript-vector baseline
   - LoCoMo QA source

## Conclusion

The data acquisition path works. LongMemEval cleaned JSON is immediately usable for a small Azure-MemForest replay experiment. LoCoMo dialogue data is available, but QA/gold-answer acquisition needs a follow-up before it can support answer-quality scoring.
