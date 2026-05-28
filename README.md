# Azure-MemForest

![Animated Azure-MemForest chat memory flow](docs/assets/azure-memforest-chat.svg)

Azure-MemForest is an enterprise Azure implementation of **MemForest**, a long-memory substrate for agents that turns conversations into durable, queryable memory forests.

The original MemForest research artifact showed how to preserve agent memory as canonical facts organized into temporal trees. This repository extends that idea into a cloud architecture with Azure Container Apps, Cosmos DB, Azure AI Search, Blob Storage, Service Bus, managed identity, Azure OpenAI, Entra authentication, explainability traces, and benchmark parity tooling.

## What Azure-MemForest does

Azure-MemForest gives an agent a memory system that behaves less like a transcript dump and more like a living forest:

1. **Ingest conversations** as ordered turns.
2. **Extract canonical facts** with stable IDs, time anchors, entities, topics, and semantic metadata.
3. **Organize facts into MemTrees** across session, entity, and scene views.
4. **Summarize internal tree nodes** so large memories can be searched coarsely before browsing finely.
5. **Project committed memory generations** into cloud stores:
   - Cosmos DB for source records, canonical facts, tree nodes, fact locations, manifests, jobs, leases, deletions, and traces.
   - Azure AI Search for managed recall over committed tree roots.
   - Blob Storage for restorable UserForest snapshots and fingerprint artifacts.
   - Service Bus sessions for ordered per-user writes.
6. **Query memory** by recalling trees, browsing tree nodes, loading facts, reranking with stored fact embeddings, and composing grounded answers.
7. **Explain decisions** through safe trace records that expose recall, browse, rerank, and answer steps without raw prompt or memory payload leakage.

## Why this exists

Long-running agents need memory that is:

- **Durable** across sessions and deployments.
- **Scoped** per tenant and user.
- **Updatable** without rebuilding everything.
- **Queryable** without putting every past turn into the prompt.
- **Explainable** enough to show which memories shaped an answer.
- **Governed** by identity, audit, retention, and cloud operational controls.

Azure-MemForest is the path from a research artifact to a usable cloud memory service.

## Current status

The dev Azure environment has been built and smoke-tested end to end.

- API and worker run on Azure Container Apps.
- Entra bearer-token auth is configured and verified.
- The worker ingests Service Bus session messages and materializes committed memory generations.
- Two same-user live ingests successfully committed generation 1 and generation 2.
- Snapshot hydration preserved generation 1 while writing generation 2.
- Cloud query returns grounded answers from latest committed memory.
- Query trace readback is implemented.
- Build provenance and dirty-tree image guards are in place.
- Benchmark and parity tooling is present, including judge variance study support.

The project has completed more than 190 tracked implementation tasks using a goal-driven, dynamically revised backlog.

## Architecture

```text
Client / Agent
    |
    |  Ingest / Query / Delete
    v
Azure Container Apps API
    |
    |  accepted write job
    v
Service Bus session queue  ----->  Worker Container App
                                      |
                                      | acquire per-user lease
                                      | hydrate latest snapshot
                                      | run MemForest core
                                      | project committed generation
                                      v
       +----------------------+----------------------+----------------------+
       |                      |                      |                      |
   Cosmos DB              Azure AI Search        Blob Storage          Azure OpenAI
   source of truth        tree-root recall       snapshots             extraction,
   facts, trees,          vector index           fingerprint           summary,
   manifests, jobs,                             artifacts              query,
   leases, traces                                                      answer
```

### Write path

The write worker is intentionally strict:

1. Accept a Service Bus message scoped by `tenant_id:user_id`.
2. Claim the job in Cosmos DB.
3. Acquire a fenced per-user write lease.
4. Read the latest committed manifest.
5. Hydrate the previous UserForest snapshot from Blob Storage.
6. Apply ingest or delete through the synchronous MemForest core.
7. Project facts, trees, locations, embeddings, Search documents, summaries, snapshots, and fingerprints.
8. Assert the lease is still current.
9. Commit the new manifest.

Only committed manifests are visible to query readers.

### Query path

Cloud query follows the MemForest paper semantics:

1. Read the latest committed manifest.
2. Embed the question with the configured Azure OpenAI embedding deployment.
3. Recall candidate tree roots from Azure AI Search filtered by tenant, user, forest version, and embedding model version.
4. Browse Cosmos-backed tree nodes using browse embeddings.
5. Collect leaf fact IDs.
6. Load canonical facts from Cosmos DB.
7. Rerank facts using stored extraction-time fact embeddings.
8. Compose a grounded answer and reason.
9. Persist safe explainability traces.

## Repository layout

```text
.
├── benchmark/              # Frozen paper benchmark CSVs and baseline summary
├── docs/                   # Azure design, auth, worker, security, production readiness docs
├── infra/                  # Bicep infrastructure for Azure Container Apps and data plane
├── paper/                  # Original MemForest paper assets
├── scripts/                # Build/deploy helper scripts
├── src/
│   ├── build/              # MemTree construction, summaries, node indexes
│   ├── cloud/              # Cosmos, Search, Blob, model, trace, query adapters
│   ├── config/             # Dataclass-backed configuration
│   ├── extraction/         # Chunk extraction and canonical fact management
│   ├── forest/             # UserForest and MemForest orchestration
│   ├── query/              # Local recall, browse, rerank, answer pipeline
│   ├── service/            # FastAPI service, auth, routes, readiness, logging
│   └── worker/             # Service Bus receive loop and materialization runtime
└── tests/                  # Contract, service, worker, cloud, benchmark tests
```

## Azure components

| Concern | Azure service |
| --- | --- |
| API and worker hosting | Azure Container Apps |
| Container images | Azure Container Registry |
| Source of truth | Azure Cosmos DB |
| Recall | Azure AI Search |
| Snapshots and cold artifacts | Azure Blob Storage |
| Ordered writes | Azure Service Bus sessions |
| Model deployments | Azure OpenAI |
| Identity | Microsoft Entra ID and managed identity |
| Observability | Application Insights, Log Analytics, query trace records |

## Development

Use the repo-local virtual environment.

```powershell
python -m venv .venv
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Run tests:

```powershell
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m pytest -q
```

Build a clean ACR image:

```powershell
.\scripts\build_acr.ps1
```

The build script refuses dirty trees and stamps the image with Git SHA metadata.

## Deployment notes

Infrastructure is defined in `infra/main.bicep`.

Important deployment switches:

- `containerImage`: immutable image to deploy.
- `deployWorker`: deploy the worker Container App.
- `workerFactoryReady`: allow the worker to connect to Service Bus.
- `enableDevHeaderAuth`: development-only header auth.
- `entraTenantId` and `entraAudiences`: production bearer-token validation settings.

Dev-header auth should remain disabled outside controlled smoke tests.

## Benchmark and parity

Azure-MemForest keeps the original benchmark CSVs and adds deterministic summary/diff tooling:

- `src.evaluation.benchmark_summary`
- `src.evaluation.benchmark_diff`
- `src.evaluation.judge_variance`
- `src.evaluation.cloud_replay`

Current policy:

- Frozen benchmark labels are preserved for reproducibility.
- New cloud parity must compare local and cloud candidates under the same approved judge prompt/deployment.
- Candidate-only methods fail under strict parity.
- Manifest fingerprints record model/config/code inputs for each committed generation.

## Applications

Azure-MemForest can be used anywhere an agent needs durable, explainable, tenant-scoped memory:

- Customer support copilots that remember long-running customer histories.
- Enterprise assistants that keep project, entity, and meeting memory over months.
- Research agents that preserve evolving findings and evidence trails.
- Personal productivity agents with controlled memory retention and deletion.
- Multi-agent systems that need shared, auditable memory state.
- Regulated domain assistants where retrieval must be explainable and deletions must be enforceable.

## Original research artifact

The original MemForest paper assets remain in `paper/`, and benchmark CSVs remain in `benchmark/`. Azure-MemForest preserves the research behavior contract while adding cloud durability, identity, operational readiness, and deployment scaffolding.

