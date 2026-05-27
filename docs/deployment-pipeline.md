# Deployment pipeline plan

The current dev environment was deployed manually with Azure CLI. Automated deployment should be added only after GitHub OIDC federation and environment approvals are configured.

## Deployment identity

Use a dedicated deploy identity, separate from the Container App runtime managed identity.

Recommended GitHub federated credential subject:

```text
repo:Jbetts05/azure-memforest:environment:dev
```

Do not use a broad branch-only subject for deployment credentials.

Required Azure permissions on resource group `azure-memforest`:

- Contributor, to deploy and update resources.
- RBAC Administrator, User Access Administrator, or Owner, because `infra/main.bicep` creates role assignments for ACR, Cosmos DB, Azure AI Search, Storage, and Service Bus.

Prefer the narrowest role-assignment permission policy available in the tenant.

## Initial workflow shape

Start with a manual dev deployment workflow:

```yaml
on:
  workflow_dispatch:
```

Required permissions:

```yaml
permissions:
  contents: read
  id-token: write
```

Add deployment concurrency:

```yaml
concurrency:
  group: deploy-dev
  cancel-in-progress: false
```

## Steps

1. Check out the repository.
2. Log in to Azure with `azure/login` using OIDC.
3. Run Python tests.
4. Build Bicep.
5. Build and push the image with ACR Tasks:

   ```powershell
   az acr build `
     --registry memforestexampleacr `
     --image azure-memforest-api:${{ github.sha }} `
     .
   ```

6. Run `az deployment group what-if` for visibility. Do not fail solely because the diff is non-empty; role assignments and provider defaults can be noisy.
7. Require GitHub environment approval.
8. Deploy Bicep with the immutable image tag:

   ```powershell
   az deployment group create `
     --resource-group azure-memforest `
     --template-file infra/main.bicep `
     --parameters @infra/main.parameters.json `
     --parameters containerImage=memforestexampleacr.azurecr.io/azure-memforest-api:${{ github.sha }} `
     --parameters containerPort=8000 `
     --parameters minReplicas=1
   ```

9. Verify deployed endpoints:
   - `/health/live` returns `200`;
   - `/version` returns service name/version;
   - `/health/ready` returns expected status for configured dependencies.
10. Verify the active Container App revision is running the expected image tag or digest.
11. Write deployment outputs to the GitHub Actions job summary.

## First automation gate

Before enabling deploy, run a what-if-only workflow against the existing resource group and review the diff manually. The environment already contains resources created by CLI, so the first pipeline run must confirm it will adopt rather than unexpectedly replace resources.

## Future hardening

- Configure ACR repository tag immutability for SHA tags.
- Add vulnerability scanning/SBOM.
- Add rollback workflow using prior image tag.
- Split runtime and worker identities before write workers are deployed.
- Add production environment with separate approval and parameters.
