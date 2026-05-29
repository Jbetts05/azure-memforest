# Azure-MemForest UI implementation plan

## Visual thesis

Azure-MemForest should feel like a premium dark memory observatory: chat turns bloom into living trees, and enterprise users can inspect exactly how memory becomes a grounded answer.

## Content plan

1. **Product entry:** explain Azure-MemForest in one full-bleed visual: conversation -> facts -> MemTrees -> grounded answer.
2. **Workspace:** operate on real tenant/user memory: ingest sessions, query, inspect jobs, browse conversations.
3. **Memory forest:** visualize latest committed session/entity/scene trees and fact links.
4. **Explainability:** replay recall, browse, rerank, and answer-generation traces.
5. **Operations:** show readiness, worker queue/DLQ, latest manifest, model versions, and parity status.

## Interaction thesis

- Chat turns should visually branch into facts and tree nodes as jobs commit.
- Query traces should play back as a path from recalled roots to browsed leaves to selected facts.
- Hover/focus should reveal provenance: source turn, canonical fact, tree locations, score, model/index version.

## Existing UI-capable API surface

| Need | Existing endpoint | Status |
| --- | --- | --- |
| Submit batch conversation memory | `POST /v1/users/{user_id}/sessions/{session_id}:ingest` | Exists |
| Delete session memory | `POST /v1/users/{user_id}/sessions/{session_id}:delete` | Exists |
| Query memory | `POST /v1/users/{user_id}/query` | Exists |
| Get write job status | `GET /v1/users/{user_id}/jobs/{job_id}` | Exists |
| Read query trace | `GET /v1/users/{user_id}/queries/{query_id}/trace` | Exists |

The current API can support a basic query UI, but it cannot yet support the full memory forest visualization without additional read endpoints.

## Required backend additions

Every read endpoint must derive tenant scope from the authenticated actor context and include a mandatory tenant/user predicate in the underlying query. No UI endpoint may accept a tenant id that can override the authenticated tenant. Admin cross-user access remains role-gated by `memory.admin`.

| Endpoint | Backing store | Partition key | Required caps |
| --- | --- | --- | --- |
| `GET /v1/users/{user_id}/sessions` | Cosmos `sessions` | `tenant_user_key` | `limit`, continuation token |
| `GET /v1/users/{user_id}/sessions/{session_id}` | Cosmos `sessions` | `tenant_user_key` | single item |
| `GET /v1/users/{user_id}/sessions/{session_id}/turns` | Cosmos `turns` | `tenant_user_key` | `limit`, continuation token |
| `GET /v1/users/{user_id}/forest/latest` | Cosmos `manifests` | `tenant_id` | latest committed only |
| `GET /v1/users/{user_id}/forest/{forest_version}/trees` | Cosmos `tree_nodes` | `tenant_user_key` | roots only, tree type filter |
| `GET /v1/users/{user_id}/forest/{forest_version}/trees/{tree_id}/nodes/{node_id}` | Cosmos `tree_nodes` | `tenant_user_key` | single node |
| `GET /v1/users/{user_id}/forest/{forest_version}/trees/{tree_id}/subtree` | Cosmos `tree_nodes` | `tenant_user_key` | `max_depth`, `max_nodes`, cursor |
| `GET /v1/users/{user_id}/forest/{forest_version}/facts/{fact_id}` | Cosmos `facts` + `fact_locations` | `tenant_user_key` | single fact plus locations |
| `GET /v1/users/{user_id}/forest/{forest_version}/facts/{fact_id}/locations` | Cosmos `fact_locations` | `tenant_user_key` | page size |
| `GET /v1/users/{user_id}/jobs/{job_id}/events` | Cosmos jobs/traces or future event store | `tenant_id` | polling v1, `Retry-After` |

### Latest forest semantics

`latest` always means the latest **committed** manifest. Pending manifests and failed jobs are not UI-visible except through job status. Trace views should prefer the `forest_version` recorded on the trace so highlighting is stable even after a newer generation commits.

If a trace references a `forest_version` that has been compacted or pruned, the trace API must return the trace metadata plus a `forest_version_unavailable` flag so the UI can explain why tree highlighting is unavailable instead of silently showing latest-state nodes.

### Tree read caps

Subtree APIs must include:

- `max_depth` default `2`, maximum `6`;
- `max_nodes` default `250`, maximum `1000`;
- continuation token for large sibling sets;
- server-side tenant/user/forest filters;
- no raw prompt/model payloads.

### Trace-to-tree provenance contract

The UI trace player requires typed, stable event records. The API should project raw trace events into a discriminated response with:

- `event_type`;
- `tree_id`;
- `node_id`;
- `fact_id`;
- `score`;
- `candidate_rank`;
- `path_from_root` when available;
- `forest_version`;
- `embedding_model_version`;
- `recall_strategy`;
- `stage_duration_ms`;
- truncation and redaction flags.

Events that do not meet the allowlist should be omitted or returned as `unknown` with safe scalar metadata only. Existing persisted traces should be treated as legacy and sanitized on read.

Trace projection APIs must include `max_events` and continuation support, because long browse/rerank traces can grow as quickly as tree subgraphs.

Job event reads must enforce both tenant scope and user scope. A tenant member may not read another user's job events unless the actor has `memory.admin`.

## Chat semantics

The MVP should not promise streaming chat. It should start with:

- batch session ingest;
- polling job status;
- optional polling job events;
- conversation history from committed source turns.

Per-turn ingest, token streaming, and live tree-growth events are future enhancements unless explicitly prioritized. If added later, per-turn ingest must define idempotency at the turn level and its relationship to manifest commits.

## Authentication and browser security

The UI must use Entra authorization code + PKCE or an approved BFF pattern. It needs:

- configured API audience/scope;
- CORS allowlist for the UI origin;
- token refresh and logout;
- `/me` or equivalent identity display;
- no local-storage tokens unless explicitly accepted;
- clear 401/403 states.

Development mode may use a mock actor only behind the explicit UI feature flag and non-production environment.

`AZURE_MEMFOREST_UI_ENABLED` is currently a safe default-off feature flag. Future UI routes, static assets, CORS origins, and any UI-specific dev auth behavior must check this flag before becoming visible.

## MVP vertical slice

The first useful UI should deliver:

1. Entra sign-in.
2. Session browser over a seeded tenant/user.
3. Latest committed forest overview.
4. Read-only tree visualization for one session/entity/scene tree.
5. Query form returning answer and facts.
6. Trace playback that highlights recalled tree, browsed nodes, and selected facts.

Success criterion: a user can trace a returned answer back to a source turn and tree path in two clicks or fewer.

## Deployment scaffold

The Bicep template includes a default-off Azure Static Web Apps scaffold controlled by `deployUi`. The UI API feature flag is independent and controlled by `enableUiFeature` plus `uiAllowedOrigins`.

Deployment sequence:

1. Build `ui/` with `npm run build`.
2. Deploy static assets to the Static Web App.
3. Set `enableUiFeature=true` and `uiAllowedOrigins` to the Static Web App origin.
4. Verify Entra sign-in, session list, forest latest, query, and trace read.
5. Keep dev-header auth disabled outside controlled smoke tests.

## Non-goals for UI MVP

- Editing canonical facts.
- Sharing memories between users.
- Full historical manifest browsing.
- Full benchmark runner UI.
- Per-token chat streaming.
- Production support for arbitrary graph sizes without LOD/pagination.
