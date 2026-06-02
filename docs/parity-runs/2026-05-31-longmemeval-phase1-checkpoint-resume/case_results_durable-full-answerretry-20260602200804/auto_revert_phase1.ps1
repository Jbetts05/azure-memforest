$ErrorActionPreference='Stop'
az containerapp update -g 'azure-memforest' -n 'memforest-example-api' --remove-env-vars AZURE_MEMFOREST_DISPATCH_MODE AZURE_MEMFOREST_DURABLE_START_URL AZURE_MEMFOREST_DURABLE_START_TOKEN --set-env-vars AZURE_MEMFOREST_DEV_AUTH=False | Out-Null
