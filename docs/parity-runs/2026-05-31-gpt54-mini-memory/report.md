# GPT-5.4-mini memory-model validation

Run date: 2026-05-31

## Goal

Replace the older `gpt-4o-mini-memory` internal memory-building deployment with a newer GPT-5-family small model, while keeping the larger `gpt-5.4` deployment for final answer paths.

## Deployment

Created deployment on `memforest-example-foundry`:

| Deployment | Model | Version | SKU | Capacity |
| --- | --- | --- | --- | --- |
| `gpt-5.4-mini-memory` | `gpt-5.4-mini` | `2026-03-17` | `GlobalStandard` | `500` |

The deployment was tested successfully with the current GPT-5-compatible request shape (`max_completion_tokens`).

Live worker routing after update:

- Memory/default/chat/extraction/dedupe/planner/browse: `gpt-5.4-mini-memory`
- Answer: `gpt-5.4`
- Summarization env: `gpt-5.4`
- Embeddings: `text-embedding-3-large`
- Image: `ba0e9ce3ecf2`

Artifacts:

- `worker_model_env.json`
- `model_deployments.json`
- `queue_counts_final.json`
- `deadletter_peek_final.json`

## Target-session comparison

The same two target LongMemEval sessions from the previous pipeline-improvement validation were rerun.

| Target session | Previous all-GPT-5.4 path | `gpt-4o-mini-memory` | `gpt-5.4-mini-memory` | Result |
| --- | ---: | ---: | ---: | --- |
| `8e91e7d9 / da71721f` | related run had ~735s / ~711s late ingests and DLQ | ~42.3s | ~53.0s | succeeded |
| `4b24c848 / answer_2cec623b_2` | ~156.9s | ~54.0s | ~59.1s | succeeded |

## Telemetry comparison

For `da71721f`:

| Model | Chat calls | Chat latency | Embedding items | Ingest |
| --- | ---: | ---: | ---: | ---: |
| `gpt-4o-mini-memory` | 23 | ~35.4s | 76 | ~42.3s |
| `gpt-5.4-mini-memory` | 45 | ~60.7s | 134 | ~53.0s |

For `answer_2cec623b_2`:

| Model | Chat calls | Chat latency | Embedding items | Ingest |
| --- | ---: | ---: | ---: | ---: |
| `gpt-4o-mini-memory` | 16 | ~42.2s | 78 | ~54.0s |
| `gpt-5.4-mini-memory` | 52 | ~72.7s | 142 | ~59.1s |

The important finding is not just latency: `gpt-5.4-mini-memory` extracted/materialized substantially more items/calls on the same inputs. That may be richer memory, or it may be noisier memory; this run did not judge extraction quality.

## Readback quality spot-check

After ingesting `answer_2cec623b_2`, querying:

```text
How many tops had I bought from H&M?
```

returned:

```text
5 tops
```

with supporting fact:

```text
The user has already bought five tops from H&M.
```

## Queue/DLQ

Final queue state:

```json
{
  "active": 0,
  "deadletter": 12,
  "scheduled": 0
}
```

`deadletter_peek_final.json` shows no new DLQ entries from the GPT-5.4-mini target runs; the latest entry was the earlier failed `pipeline-smoke` validation before storage access was restored.

## Conclusion

`gpt-5.4-mini-memory` is a viable modern replacement candidate for the older `gpt-4o-mini-memory` internal model:

- It deployed cleanly.
- It works with the current GPT-5 request path.
- Both targeted session ingests succeeded.
- The readback query returned the correct fact-backed answer.
- It is still dramatically better than routing all memory work through full `gpt-5.4`.

But it is not a proven default yet:

- It was slower than `gpt-4o-mini-memory` on both target sessions.
- It made many more chat calls and produced more embedded items.
- More extracted memory may improve recall or may add noise; this requires benchmark scoring.
- The validation is only two target sessions plus one readback query, not a full benchmark.

Recommended next step: keep `gpt-5.4-mini-memory` as the current modern candidate and run the planned small 3-case LongMemEval batch with official/LLM-judge scoring to compare quality against the `gpt-4o-mini-memory` run.
