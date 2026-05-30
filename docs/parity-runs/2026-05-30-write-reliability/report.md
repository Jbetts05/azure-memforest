# Azure-MemForest write-reliability slice

Run date: 2026-05-30

## Goal

Make the deployed write path reliable enough for benchmark smoke runs: clear memory must reach a terminal state, ingest must not remain `running` after poison/final delivery failures, and a tiny clear -> ingest -> query smoke must pass before retrying LongMemEval.

## Changes validated

- Worker consumer now logs transient/unhandled handler exceptions.
- On final delivery, transient/unhandled handler failures mark the Cosmos job `failed` and dead-letter the Service Bus message instead of leaving the job `running`.
- Permanent-handler `fail_job` failures also dead-letter on final delivery.
- `complete_job` failures are dead-lettered on final delivery without falsely marking already-executed work as failed.
- The job-record-before-publish race fix from the prior slice remains deployed.

Focused validation:

```text
python -m pytest tests\test_worker_consumer.py tests\test_cosmos_jobs.py tests\test_service_app.py -q
61 passed
```

## Deployment

- Built and deployed image: `memforestexampleacr.azurecr.io/azure-memforest-api:81e9e956fc1e`
- API and worker were both updated to image/version `81e9e956fc1e`.

## Synthetic smoke result

Artifact: `synthetic_smoke_result_after_storage_fix.json`

| Step | Status | Notes |
| --- | --- | --- |
| clear memory | `succeeded` | Completed in about 7 seconds. |
| ingest | `succeeded` | Completed in about 7 seconds. |
| query | `succeeded` | Answered `Josh prefers Python examples.` with the expected supporting fact. |

## Additional blocker found and fixed

The first post-image smoke converted the old ingest hang into a terminal failed job with the real error: Blob snapshot write returned Azure Storage `AuthorizationFailure`. The managed identity already had `Storage Blob Data Contributor`; the blocker was network access. The dev Storage account had public network access disabled while the current Container Apps environment has no private endpoint/VNet path.

Temporary dev unblock applied:

```text
az storage account update --name memforestexamplestorage --resource-group azure-memforest --public-network-access Enabled --default-action Allow
```

This is a dev-only deviation. Production still needs private networking/VNet integration instead of public Storage access.

## Queue and DLQ state

After the successful synthetic smoke:

- `queue_counts_after_storage_fix.json`: active queue count was `0`.
- Existing DLQ messages were retained for provenance rather than purged.

The later LongMemEval smoke recorded `deadletter_peek_after_token_refresh.json`, confirming the 8 DLQ messages are from earlier failed runs, including pre-race-fix `job_not_found`, pre-final-delivery `MaxDeliveryCountExceeded`, and one post-image/pre-storage-fix `AuthorizationFailure`.

## Conclusion

The write-reliability gate for a bounded benchmark smoke is passed: clear, ingest, and query complete end-to-end on the deployed service. The remaining risks for larger runs are throughput-related: Azure OpenAI `429` retries, Service Bus timeout noise, and the temporary dev Storage network posture.
