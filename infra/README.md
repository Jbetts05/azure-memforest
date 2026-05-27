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

The current template includes the hosting foundation, Cosmos DB account/database foundation, and Azure AI Search recall foundation. Redis, Service Bus, and model endpoint resources should be added in the implementation slices that first use them.

## Current dev deployment

The initial dev hosting foundation has been deployed to:

| Setting | Value |
| --- | --- |
| Resource group | `azure-memforest` |
| Region | `eastus2` |
| Container App | `memforest-example-api` |
| FQDN | `memforest-api.example.invalid` |
| ACR | `memforestexampleacr.azurecr.io` |
| Image | `memforestexampleacr.azurecr.io/azure-memforest-api:5497651751c4` |
| Cosmos DB account | `memforestexamplecosmos` |
| Cosmos DB database | `memforest` |
| Azure AI Search | `memforestexamplesearch` |
| Recall index name | `memforest-recall` |

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
- `/health/ready` returns `503` until real Azure dependency checks are wired.

Cosmos containers and Azure AI Search indexes are intentionally not declared in Bicep yet. Store/search adapters will create or migrate containers and indexes idempotently with the correct partition keys, vector dimensions, model versions, and indexing policies when those adapters are implemented.
