# Security and operations plan

Azure-MemForest stores long-lived user memory. Security and operations must be designed as part of the product, not added after cloud migration.

## Current mitigations

Already implemented:

- sanitized `x-request-id` request correlation;
- structured JSON logging without request/response body logging;
- safe-by-default query trace events;
- tenant/auth context contracts that fail closed;
- dev header auth provider blocked outside local/test/dev environments;
- Cosmos DB local auth disabled;
- Azure AI Search local auth disabled;
- Storage shared key access disabled;
- Service Bus local auth disabled;
- managed identity and RBAC assignments for deployed services;
- benchmark parity diff gate;
- CI for Python tests, container build, and Bicep validation.

## Threat model register

| Threat | Current mitigation | Required before production |
| --- | --- | --- |
| Cross-tenant data leakage | Tenant context contracts, tenant metadata invariants | Tenant isolation tests across service, stores, search, traces, and queues |
| Intra-tenant authorization bugs | ActorContext and `memory.admin` role contract | Forest/session ownership checks and tests |
| Prompt injection through retrieved memories | None yet beyond planned prompt separation | Grounding prompts, evidence/instruction separation, red-team tests |
| Raw payload logging/tracing | No body logging, safe trace docs | Integration tests proving fixture prompts do not enter logs/traces |
| Embedding inversion/privacy leakage | Embedding model version metadata planned | Treat embeddings as sensitive, enforce deletion and retention |
| Deletion propagation failures | Deletion contract documented | End-to-end erasure tests across Cosmos, Search, Storage, Redis, traces, logs |
| Queue poison messages / duplicate writes | Service Bus DLQ and sessions deployed | DLQ runbook, idempotency enforcement, lease fencing tests |
| Abuse and cost DoS | None yet | Per-tenant rate limits, token budgets, quota dashboards |
| Model/embedding version drift | Model config/version scaffold | Re-embedding/re-index migration tests |
| Public network exposure in dev | Public access documented as dev-only | Private endpoint/VNet decision or approved exception |
| RBAC over-permission | Runtime UAMI uses data-plane roles | Separate deploy/runtime/worker identities and least-privilege review |
| Supply-chain compromise | CI tests/container build | SBOM, vulnerability scan, base image digest pinning, ACR policy |
| Secrets/key leakage | No stored Azure secrets/OIDC yet | Secret leak response runbook, key rotation, no keys in logs/git |
| App Insights/telemetry retention leak | No raw payload logging by default | Retention policy and tenant-tagged/purgeable telemetry strategy |

## Production decision register

| Decision | Status | Required outcome |
| --- | --- | --- |
| Entra JWT validation | Open | Auth provider validates issuer, audience, tenant, actor, scopes/roles |
| Tenant isolation model | Open | Contract tests prove no cross-tenant or cross-user memory access |
| Data classification | Open | Memory, summaries, embeddings, traces, logs, and snapshots classified |
| Retention policy | Open | TTLs for traces, jobs, deleted records, snapshots, logs |
| CMK vs Microsoft-managed keys | Open | Decision documented per data store |
| Private endpoints/VNet | Open | Implement or approve public network exception |
| Deletion/erasure SLA | Open | Tested propagation and audit model |
| Model capacity/quota | Open | TPM/RPM, Search QPS, Cosmos RU/serverless cost model |
| Deployment approvals | Open | GitHub environments and OIDC subject scoped by environment |

## Required gates before production

- Entra JWT auth provider is implemented and tested.
- No route can access memory without ActorContext.
- Tenant and intra-tenant authorization tests pass.
- Deletion/erasure tests cover source and projection stores.
- Query traces are redacted and retention-limited.
- Benchmark parity diff passes within approved tolerance.
- Load test meets initial SLOs for query and ingest.
- Dependency readiness checks are wired for Cosmos, Search, Storage, Service Bus, model endpoint, and config.
- CI includes tests, container build, Bicep validation, vulnerability scan, and SBOM.
- Deployment is OIDC-based and approval-gated.
- Rollback and incident runbooks exist.

## Operations runbooks to add

- Deploy verification.
- Rollback to prior image tag.
- Rebuild Azure AI Search indexes from Cosmos source of truth.
- Re-embed and re-index after embedding model migration.
- Drain, inspect, and replay Service Bus DLQ.
- Tenant hard delete / erasure.
- Restore from backup and snapshot.
- Key/credential rotation.
- Secret leak response.
- Compromised tenant response.
- Cost spike response.

Backup and restore runbooks must include periodic restore drills; untested backups are not an operational control.

## DLQ hygiene note

The P1 dev DLQ was cleared after exporting message metadata to a session
artifact. That exploratory export did not include message bodies, so it is not a
replay-capable artifact. Future DLQ hygiene must use
`scripts/export_servicebus_dlq.py`, which preserves message bodies as base64,
normalizes application properties, and records completion receipts before any
destructive clear.

## Dev-to-production risks

The current dev deployment intentionally uses public network access for faster iteration. Production may require private endpoints, VNet integration, DNS changes, and different SKU choices. These changes can break readiness probes and SDK connectivity, so they must be tested in a production-shaped environment before launch.

Service Bus Standard is acceptable for dev, but private endpoint support requires Premium and a significant cost increase. Revisit this before production.
