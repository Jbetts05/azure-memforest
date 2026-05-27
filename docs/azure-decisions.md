# Azure deployment decisions

No Azure resource should be created, modified, or deployed until the required decisions below are confirmed.

| Decision | Status | Options / notes |
| --- | --- | --- |
| Azure tenant ID | TBD | Confirm before any Azure login, OIDC setup, or deployment. |
| Azure subscription ID | TBD | Confirm exact subscription before any IaC validation against Azure. |
| Resource group | TBD | Existing vs new resource group, naming convention, and tags. |
| Primary Azure region | TBD | Must match data residency and model availability requirements. |
| IaC tool | TBD | Recommended default: Bicep. Terraform remains an option if preferred. |
| Hosting target | TBD | Recommended default: Azure Container Apps unless AKS/App Service is required. |
| Tenancy model | TBD | Single-tenant deployment vs shared multi-tenant logical/physical isolation. |
| Data residency | TBD | Region pinning, retention defaults, and export constraints. |
| Encryption posture | TBD | Microsoft-managed keys vs customer-managed keys. |
| Erasure SLA | TBD | Required deletion/retention completion window. |
| GitHub Actions deployment mode | TBD | Manual approval first; auto-deploy only after a working environment is approved. |
| OIDC federation | TBD | Use GitHub OIDC federated credentials, not stored Azure client secrets. |
| Paper-parity tolerance | TBD | Define accepted benchmark/retrieval drift before replacing local behavior. |

## Non-negotiable guardrails

- Do not use process-local state as the enterprise source of truth.
- Do not treat Azure AI Search as a complete replacement for the current `NodeIndex`; split recall from browse embedding and tree-node access.
- Do not use Redis as authoritative storage.
- Do not store active large trees only as monolithic JSON blobs for production.
- Do not allow bottom-up or union recall to scan all trees at query time.
- Do not log full prompts or responses by default.
- Do not deploy without tenant isolation, deletion semantics, model versioning, re-indexing/re-embedding plans, redacted telemetry, and explainability traces.
