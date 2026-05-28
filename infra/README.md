# Azure infrastructure

This directory contains compile-only Bicep infrastructure for Azure-MemForest.

Do not deploy these templates until the required decisions in `docs/azure-decisions.md` are confirmed, including subscription, resource group, region, naming, tenancy, retention, and deployment approval.

## Validate locally

```powershell
az bicep build --file infra/main.bicep --stdout > $null
```

## Future deployment command

Deployment requires explicit approval and a confirmed subscription/resource group:

```powershell
az account set --subscription "<subscription-id>"
az deployment group create `
  --resource-group "<resource-group>" `
  --template-file infra/main.bicep `
  --parameters @infra/main.parameters.json `
  --parameters containerImage="<acr-login-server>/azure-memforest-api:<tag>"
```

The current template includes the API hosting foundation, Cosmos DB account/database foundation, Azure AI Search recall foundation, Blob snapshot storage foundation, Service Bus write-plane foundation, and Azure OpenAI deployments. A live worker Container App is intentionally not enabled until the materialization handler and receive loop are implemented; see `docs/write-worker.md`.

## Current dev deployment

The initial dev hosting foundation has been deployed to:

| Setting | Value |
| --- | --- |
| Resource group | `azure-memforest` |
| Region | `eastus2` |
| Azure AI Search region | `eastus` |
| Container App | `memforest-example-api` |
| FQDN | `memforest-api.example.invalid` |
| ACR | `memforestexampleacr.azurecr.io` |
| Image | `memforestexampleacr.azurecr.io/azure-memforest-api:d06d77ec0a40` |
| Cosmos DB account | `memforestexamplecosmos` |
| Cosmos DB database | `memforest` |
| Azure AI Search | `memforestexamplesearch` |
| Recall index name | `memforest-recall` |
| Snapshot container | `snapshots` |
| Service Bus namespace | `memforestexamplesb` |
| Writes queue | `memforest-writes` |
| Azure OpenAI account | `memforestexampleopenai` |
| Chat deployment | `gpt-4-1-mini-dev` (`gpt-4.1-mini`, `2025-04-14`) |
| Embedding deployment | `text-embedding-3-small-dev` (`text-embedding-3-small`, version `1`, dimension `1536`) |

Azure AI Search is deployed in `eastus` for the current dev environment because `eastus2` returned `InsufficientResourcesAvailable` for new Search services on 2026-05-27. Revisit this topology before production.

Verification:

```powershell
$base = "https://memforest-api.example.invalid"
Invoke-WebRequest "$base/health/live" -UseBasicParsing
Invoke-WebRequest "$base/version" -UseBasicParsing
Invoke-WebRequest "$base/health/ready" -UseBasicParsing -SkipHttpErrorCheck
```

Expected current behavior:

- `/health/live` returns `200` with `{"status":"ok"}`.
- `/version` returns `200` with service name/version.
- `/health/ready` returns `200` when Cosmos DB, Azure AI Search, Blob Storage, and Service Bus configuration checks are healthy.

Cosmos containers are declared in Bicep. The Azure AI Search recall index `memforest-recall` is created idempotently by the Search bootstrap code because its vector dimensions are tied to the configured embedding deployment. The Blob `snapshots` container is declared in Bicep because it has no schema or partition-key coupling.

The Service Bus writes queue has sessions enabled for per-tenant/user write ordering. The dev namespace uses Standard SKU; private endpoints require Premium and should be revisited before production.

Worker hosting is declared as a conditional no-ingress Container App and defaults off through `deployWorker=false`. The worker entrypoint idles without connecting to Service Bus unless both `AZURE_MEMFOREST_WORKER_ENABLED=true` and `AZURE_MEMFOREST_WORKER_FACTORY_READY=true` are set, preventing accidental queue consumption before the final smoke gate. The first enabled worker deployment should use `workerMinReplicas=1`/`workerMaxReplicas=1`; scale-to-zero requires a separately validated managed-identity Service Bus scaler for the session-enabled queue.
