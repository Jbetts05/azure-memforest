# Cloud data contracts

Azure-MemForest cloud adapters must preserve paper behavior while replacing local state with durable, tenant-isolated stores.

## Required record metadata

Every durable source-of-truth record must carry:

- `schema_version`
- `tenant_id`
- `forest_version`
- `user_id` for user-scoped memory records
- server-assigned create/update timestamps where the backing store supports them

Every embedding-bearing record or search document must also carry:

- `embedding_model_version`
- `embedding_dimension`
- embedding role, such as `query`, `tree_summary`, `fact`, or `browse_node`

## Partitioning and tenant isolation

The default logical partition shape is:

```text
tenant_id:user_id
```

Tenant-wide records may use `tenant_id` alone. `tenant_id` and `user_id` values used in partition keys must not contain `:`. Cloud adapters must filter by tenant at the service boundary and at the storage/index layer. Contract tests for cloud adapters must include cross-tenant isolation cases.

## Deletion semantics

Soft delete means a source-of-truth record is tombstoned and excluded from queries immediately. Projection stores such as Azure AI Search, browse embedding stores, Redis caches, and trace views must drop or hide tombstoned records during asynchronous propagation.

Hard delete means purging source records and all derived projections after the retention policy allows it. Hard delete must cover:

- canonical facts and occurrences
- sessions, turns, and cells
- tree leaves and affected ancestor summaries
- browse embeddings
- fact-location index records
- Azure AI Search documents
- Redis cache entries
- trace metadata and optional payload blobs according to retention policy

Deletion audit records may remain if required by policy, but must not contain raw user-memory payloads unless explicitly approved.

`FactStore.tombstone_facts` and `TreeNodeStore.tombstone_tree` represent soft-delete entry points. Hard-delete/erasure jobs will be implemented as explicit compaction workflows, not silent cache eviction.

## Migration and rebuild requirements

Schema bumps require either a documented forward migration or a rebuild-from-source path.

Derived projections are disposable and must be rebuildable from source-of-truth records:

- Azure AI Search recall indexes
- browse embedding stores
- Redis caches
- Blob snapshots
- fact-location projections

Embedding model changes require a re-embedding and re-index plan keyed by `embedding_model_version`. Cloud-mode query composition is not complete until it passes the frozen benchmark parity diff within the approved tolerance.
