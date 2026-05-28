# Building Azure-MemForest: 191 Tasks, One Goal, and a Dynamic Backlog

This project started with a research artifact and a question: can MemForest become a real Azure service without losing the behavior that made the paper interesting?

The answer is yes, but not by doing a single heroic rewrite. We got there through goal-based execution, rubber-duck validation, and a backlog that changed whenever reality disproved an assumption.

By the end of the build, the todo database had recorded **191 completed tasks**. Those tasks covered infrastructure, identity, worker materialization, cloud query, explainability, benchmark tooling, production runbooks, and the many sharp edges that only appear when code meets a live cloud environment.

## The goal

The goal was not simply "deploy the repo."

The goal was:

- preserve MemForest's paper semantics;
- turn canonical facts into durable tenant/user memory;
- keep session, entity, and scene MemTrees;
- preserve summary reuse, tree recall, tree browsing, reranking, and benchmark parity tooling;
- make it run in Azure with enterprise identity and operational controls;
- prove it with live smoke tests, not just architecture diagrams.

That distinction shaped every decision.

## The method: goal-based work with a dynamic backlog

The backlog was not a static checklist. It was a control system.

At each milestone we asked:

1. What does the goal require next?
2. What is now proven?
3. What did the latest failure reveal?
4. Which tasks are now blocked, obsolete, or too vague?
5. What should be decomposed before we continue?

Rubber-duck reviews acted like independent design reviews. They caught issues early:

- cloud query was premature before worker materialization existed;
- a scratch UserForest per message would erase prior generations;
- manifests needed immutable snapshot pointers and hashes;
- Service Bus session ordering comes from session receivers, not replica count;
- cloud rerank should use stored fact embeddings, not re-embed facts at query time;
- fingerprint metadata could exceed Cosmos item limits if kept inline.

Each critique changed the backlog. That is why the final system is much stronger than the first plan.

## Phase 1: build the cloud foundation

We started with safety:

- tests and benchmark summary/diff tools;
- CI and Bicep validation;
- Docker image provenance;
- dirty-tree build guard;
- Azure Container Apps API shell;
- readiness endpoints;
- request correlation and JSON logging;
- Entra JWT auth provider;
- Service Bus write-job contracts.

Then we deployed the base Azure estate:

- Container Apps;
- Azure Container Registry;
- Cosmos DB;
- Azure AI Search;
- Blob Storage;
- Service Bus session queue;
- Azure OpenAI deployments;
- managed identity and RBAC.

At this stage the API was live, but memory was not yet truly cloud-backed.

## Phase 2: make writes real

The first major turning point was the worker.

The worker had to do more than consume a queue. It had to preserve MemForest's semantics:

- ordered per-user writes;
- canonical facts;
- session/entity/scene MemTrees;
- Search recall documents;
- browse embeddings;
- snapshots;
- manifest commit boundaries.

The materialization worker became the heart of the cloud system.

It now:

1. receives Service Bus session messages;
2. claims jobs in Cosmos;
3. acquires a fenced per-user write lease;
4. hydrates the latest committed UserForest snapshot;
5. applies ingest or delete through the MemForest core;
6. projects facts, tree nodes, fact locations, browse embeddings, Search documents, and snapshots;
7. asserts the lease is still current;
8. commits a new manifest.

The manifest is the visibility boundary. Query readers only read the latest committed `forest_version`.

## Phase 3: solve hydration

A rubber-duck review caught a critical flaw: if each worker job created a fresh scratch UserForest, the second ingest would publish a generation containing only the second session. It would silently erase the first session from query visibility.

So we added snapshot hydration.

Each committed generation writes:

- a restorable UserForest archive;
- a `snapshot_id`;
- a `snapshot_content_sha256`;
- materialization fingerprint metadata.

The next write restores that snapshot before applying new changes. A live smoke test proved it:

- ingest session 1 -> generation 1;
- ingest session 2 for the same user -> generation 2;
- generation 2 preserved generation 1.

That was the moment Azure-MemForest became a real persistent memory service rather than a stateless demo.

## Phase 4: make query cloud-native

Cloud query follows the MemForest shape:

1. read the latest committed manifest;
2. embed the question;
3. recall candidate tree roots from Azure AI Search;
4. browse Cosmos tree nodes with stored browse embeddings;
5. collect leaf fact IDs;
6. load canonical facts from Cosmos;
7. rerank with stored fact embeddings;
8. compose a grounded answer;
9. persist safe explainability traces.

The live smoke query asked:

> Where did Alice move?

The service answered:

> Alice moved to Seattle.

It returned the supporting facts from the latest committed forest version and persisted a trace that could be read back through the trace API.

## Phase 5: identity and operational discipline

We configured Microsoft Entra authentication with:

- app registration;
- v2 access tokens;
- `access_as_user` scope;
- accepted audiences;
- route-level user enforcement.

Verification covered:

- no token -> 401;
- wrong user path -> 403;
- correct user token reaches the route.

Dev-header auth was kept only as a controlled smoke-test tool and hardened so it cannot default silently to a local environment.

We also added:

- production readiness runbooks;
- operational dashboard and alert requirements;
- compaction and erasure planning;
- CI/CD promotion workflow;
- backup, restore, load, and DR planning.

## Phase 6: parity and the uncomfortable truth about judging

Benchmark parity is more subtle than "run the CSVs."

The checked-in benchmark files contain frozen answers and judge labels. But the original judge prompt was not present in the repository snapshot. We built tooling to:

- summarize benchmark outputs;
- enforce strict diffs;
- reject candidate-only method false greens;
- sample judge prompts;
- replay judge decisions through Azure OpenAI.

The frozen labels showed a 4.17% answer-label disagreement floor. A 200-sample replay through the approved new judge prompt showed 20.5% disagreement against the frozen labels.

That does not mean the system failed. It means we cannot honestly claim the new judge prompt is identical to the missing original judge. So the policy changed:

> Cloud parity must compare local and cloud candidates under the same approved judge prompt and deployment.

That is the right kind of engineering honesty.

## What was achieved

Azure-MemForest now has:

- live Azure infrastructure;
- live API and worker;
- ordered Service Bus writes;
- fenced per-user materialization;
- snapshot hydration;
- committed manifests;
- Cosmos source-of-truth state;
- Azure AI Search recall;
- Blob snapshots and fingerprint artifacts;
- cloud query recall, browse, rerank, answer composition;
- safe query traces and trace read API;
- Entra authentication;
- production readiness docs;
- benchmark and judge variance tooling.

All tracked todos were completed: **191 done, 0 pending, 0 blocked**.

## What this enables

Azure-MemForest is useful anywhere agents need durable, explainable, tenant-scoped memory:

- customer support copilots with long customer histories;
- enterprise assistants with project and meeting memory;
- personal productivity agents with retention and deletion controls;
- research agents that preserve evidence trails;
- regulated assistants where retrieved memory must be explainable;
- multi-agent systems that need shared memory without shared prompts.

## The lesson

The work succeeded because we did not blindly follow the first plan.

We followed the goal.

The backlog changed when the facts changed. Rubber-duck reviews forced uncomfortable questions. Live Azure smoke tests exposed SDK incompatibilities, auth edge cases, Docker rate limits, and missing trace writes. Each failure became a task, and each task moved the system closer to the goal.

That is how a research artifact became Azure-MemForest.

