# Azure-MemForest model upgrade run

Run date: 2026-05-30

## Goal

Check whether Azure-MemForest can use the latest available high-capacity deployments from an existing AI Services resource instead of creating duplicate deployments on the app-owned Azure OpenAI resource.

## Existing deployment selected

Resource: `memforest-example-foundry` in resource group `jobe-learn`

Endpoint:

```text
https://memforest-example-foundry.cognitiveservices.azure.com/
```

Deployments:

| Deployment | Model | Version | SKU | Capacity |
| --- | --- | --- | --- | --- |
| `gpt-5.4` | `gpt-5.4` | `2026-03-05` | `GlobalStandard` | `1000` |
| `text-embedding-3-large` | `text-embedding-3-large` | `1` | `GlobalStandard` | `1000` |

## Required compatibility changes

- `gpt-5.4` rejects `max_tokens`; the model client now sends `max_completion_tokens` for deployment names starting with `gpt-5`.
- GPT-5-generation deployments use a larger default output budget (`4096`) to avoid starving JSON-producing extraction/summarization calls.
- `text-embedding-3-large` returns 3072-dimensional vectors, so Azure-MemForest was switched to:
  - `AZURE_OPENAI_EMBEDDING_DIMENSION=3072`
  - `AZURE_OPENAI_EMBEDDING_MODEL_VERSION=text-embedding-3-large-1`
  - `AZURE_SEARCH_RECALL_INDEX_NAME=memforest-recall-large`

## Azure changes applied

- Granted the Azure-MemForest managed identity `Cognitive Services OpenAI User` on `memforest-example-foundry`.
- Deployed image `memforestexampleacr.azurecr.io/azure-memforest-api:990944081f7c` to API and worker.
- Updated API and worker env vars to point all chat roles to `gpt-5.4` and embeddings to `text-embedding-3-large`.
- Created a new Azure AI Search recall index `memforest-recall-large` with 3072-dimensional vector field. The existing 1536-dimensional `memforest-recall` index was left intact.

## Validation

Focused tests:

```text
python -m pytest tests\test_cloud_config_models.py tests\test_cloud_chat_service.py tests\test_service_app.py -q
44 passed
```

Deployed smoke:

Artifact: `synthetic_smoke_gpt54_embedding_large_after_index.json`

| Step | Status | Notes |
| --- | --- | --- |
| clear memory | `succeeded` | Completed in about 7 seconds. |
| ingest | `succeeded` | Completed in about 13 seconds. |
| query | `succeeded` | Answered `Python examples.` |

Additional evidence:

- `api_model_env.json`
- `worker_model_env.json`
- `large_recall_index_schema.json`
- `queue_counts_after_smoke.json`
- `worker_logs_after_smoke.txt`

## Notes and caveats

- The first upgraded ingest failed because the new 3072-dimensional Search index did not exist yet. After creating `memforest-recall-large`, the smoke passed.
- Queue count after the final smoke had `active=0`; DLQ count remained from previous failed runs plus the failed pre-index upgrade attempt.
- This confirms the app can use the external GPT-5.4 and embedding-large deployments, but it does not yet prove benchmark-quality accuracy or throughput. Larger runs should use pacing and telemetry because prior smokes saw Azure OpenAI 429 retries.
