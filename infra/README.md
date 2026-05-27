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

The current template is the hosting foundation only. Cosmos DB, Azure AI Search, Redis, Service Bus, and model endpoint resources should be added in the implementation slices that first use them.
