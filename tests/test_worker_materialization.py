from __future__ import annotations

import pytest
import asyncio

from src.build.tree_types import MemTree, MemTreeNode, SessionLeaf
from src.cloud.cosmos_leases import LeaseConflict
from src.cloud.contracts import Lease
from src.cloud.cosmos_manifests import ForestManifest
from src.forest.user_forest import ForestMaterializationSnapshot
from src.service.auth import ActorContext, TenantContext
from src.service.jobs import build_write_job_envelope
from src.worker.consumer import TransientHandlerError
from src.worker.materialization import (
    MaterializationHandlerConfig,
    MaterializationStores,
    MemForestMaterializationHandler,
    project_forest_snapshot,
)
from src.worker.consumer import PermanentHandlerError


def test_project_forest_snapshot_tags_cloud_artifacts() -> None:
    entity_tree = MemTree(
        tree_id="entity:alice",
        tree_type="entity",
        tree_key="alice",
        label="Alice",
        k=4,
        root_node_id="entity-root",
        nodes={
            "entity-root": MemTreeNode(
                node_id="entity-root",
                tree_id="entity:alice",
                level=1,
                time_start=1.0,
                time_end=2.0,
                summary="Alice summary",
                summary_dirty=False,
                child_ids=["entity-leaf"],
                item_count=1,
            ),
            "entity-leaf": MemTreeNode(
                node_id="entity-leaf",
                tree_id="entity:alice",
                level=0,
                time_start=1.0,
                time_end=1.0,
                summary="Fact summary",
                summary_dirty=False,
                child_ids=["fact-1"],
                item_count=1,
                parent_id="entity-root",
            ),
        },
        session_leaves={},
        fact_ids_ordered=["fact-1"],
        centroid=None,
        version=1,
        lifecycle_state="active",
        deactivated=False,
        replacement_tree_ids=[],
        created_at=1.0,
        updated_at=2.0,
    )
    session_tree = MemTree(
        tree_id="session:s1",
        tree_type="session",
        tree_key="s1",
        label="s1",
        k=4,
        root_node_id="session-root",
        nodes={
            "session-root": MemTreeNode(
                node_id="session-root",
                tree_id="session:s1",
                level=1,
                time_start=1.0,
                time_end=2.0,
                summary="Session summary",
                summary_dirty=False,
                child_ids=["session-leaf"],
                item_count=1,
            ),
            "session-leaf": MemTreeNode(
                node_id="session-leaf",
                tree_id="session:s1",
                level=0,
                time_start=1.0,
                time_end=1.0,
                summary="Cell summary",
                summary_dirty=False,
                child_ids=["cell-1"],
                item_count=1,
                parent_id="session-root",
            ),
        },
        session_leaves={
            "cell-1": SessionLeaf(
                cell_id="cell-1",
                session_id="s1",
                cell_index=0,
                time_start=1.0,
                time_end=1.0,
                raw_turns=[],
                raw_text="",
                fact_ids=["fact-2"],
            )
        },
        fact_ids_ordered=[],
        centroid=None,
        version=1,
        lifecycle_state="active",
        deactivated=False,
        replacement_tree_ids=[],
        created_at=1.0,
        updated_at=2.0,
    )
    snapshot = ForestMaterializationSnapshot(
        facts=[],
        trees=[entity_tree, session_tree],
        node_embeddings={
            "entity-root": [1.0, 0.0],
            "entity-leaf": [0.5, 0.5],
            "session-root": [0.0, 1.0],
        },
        summary_cache={"abc": "cached summary"},
    )

    projection = project_forest_snapshot(
        snapshot,
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v2",
        generation=2,
        embedding_model_version="embed-v1",
    )

    assert {node.forest_version for node in projection.stored_nodes} == {"v2"}
    assert {node.tenant_id for node in projection.stored_nodes} == {"tenant-1"}
    assert {(location.fact_id, location.tree_id) for location in projection.fact_locations} == {
        ("fact-1", "entity:alice"),
        ("fact-2", "session:s1"),
    }
    assert {doc.node_id for doc in projection.recall_documents} == {"entity-root", "session-root"}
    assert projection.recall_documents[0].embedding_model_version == "embed-v1"
    assert set(projection.browse_embeddings) == {"entity-root", "entity-leaf", "session-root"}
    assert projection.summary_cache_entries[0].content_hash == "abc"
    assert b'"forest_version":"v2"' in projection.snapshot_payload


class FakeLeaseStore:
    def __init__(self, *, conflict=False, fail_assert=False):
        self.conflict = conflict
        self.fail_assert = fail_assert
        self.released = False
        self.asserted = False

    async def acquire(self, *, owner_id, ttl_seconds):
        if self.conflict:
            raise LeaseConflict("held")
        return Lease("lease-1", owner_id, "tenant-1:user-1", "1", 9999999999.0)

    async def renew(self, lease, *, ttl_seconds):
        return lease

    async def assert_current(self, lease):
        if self.fail_assert:
            raise LeaseConflict("expired")
        self.asserted = True

    async def release(self, lease):
        self.released = True


class FakeManifestStore:
    def __init__(self):
        self.latest = None
        self.pending = None
        self.committed = []

    async def latest_committed(self):
        return self.latest

    async def create_pending(self, *, forest_version, generation, metadata=None):
        self.pending = ForestManifest(
            manifest_id=f"manifest-{forest_version}",
            tenant_id="tenant-1",
            user_id="user-1",
            forest_version=forest_version,
            generation=generation,
            status="pending",
            created_at_unix=1.0,
            metadata=dict(metadata or {}),
        )
        return self.pending

    async def commit_manifest(self, manifest_id, *, metadata_updates=None):
        assert self.pending is not None
        committed = ForestManifest(
            manifest_id=self.pending.manifest_id,
            tenant_id=self.pending.tenant_id,
            user_id=self.pending.user_id,
            forest_version=self.pending.forest_version,
            generation=self.pending.generation,
            status="committed",
            created_at_unix=self.pending.created_at_unix,
            committed_at_unix=2.0,
            metadata={**dict(self.pending.metadata), **dict(metadata_updates or {})},
        )
        self.latest = committed
        self.committed.append(manifest_id)
        return committed


class RecordingStore:
    def __init__(self, calls, name, *, fail=False):
        self.calls = calls
        self.name = name
        self.items = []
        self.fail = fail

    async def upsert_session(self, session_id, *, cell_ids=None, metadata=None):
        self.calls.append(self.name)
        self.items.append((session_id, metadata))

    async def upsert_turns(self, session_id, turns):
        self.calls.append(self.name)
        self.items.extend(turns)

    async def tombstone_session(self, session_id, *, reason, scrub=False):
        self.calls.append(self.name)
        self.items.append((session_id, reason, scrub))

    async def tombstone_session_turns(self, session_id, *, reason, scrub_content=True):
        self.calls.append(self.name)
        self.items.append((session_id, reason, scrub_content))

    async def upsert_facts(self, facts):
        self.calls.append(self.name)
        self.items.extend(facts)

    async def upsert_nodes(self, nodes):
        self.calls.append(self.name)
        if self.fail:
            raise RuntimeError("node write failed")
        self.items.extend(nodes)

    async def upsert_locations(self, locations):
        self.calls.append(self.name)
        self.items.extend(locations)

    def upsert_documents(self, documents):
        self.calls.append(self.name)
        self.items.extend(documents)

    def upsert_embeddings(self, embeddings, *, model_version):
        self.calls.append(self.name)
        self.items.append((dict(embeddings), model_version))

    def put(self, content_hash, summary, *, metadata=None):
        self.calls.append(self.name)
        self.items.append((content_hash, summary, metadata))

    def write_snapshot(self, *, snapshot_id, payload, metadata):
        self.calls.append(self.name)
        self.items.append((snapshot_id, payload, metadata))


class PurgeContainer:
    def __init__(self, docs):
        self.docs = {doc["id"]: dict(doc) for doc in docs}

    def query_items(self, *, query, parameters, partition_key):
        if "tenant_user_key" in query:
            value = next(param["value"] for param in parameters if param["name"] == "@pk")
            return [{"id": doc["id"]} for doc in self.docs.values() if doc.get("tenant_user_key") == value]
        tenant = next(param["value"] for param in parameters if param["name"] == "@tenant_id")
        user = next(param["value"] for param in parameters if param["name"] == "@user_id")
        return [{"id": doc["id"]} for doc in self.docs.values() if doc.get("tenant_id") == tenant and doc.get("user_id") == user]

    def delete_item(self, *, item, partition_key):
        self.docs.pop(item, None)


class PurgeStore:
    def __init__(self, docs):
        self.container = PurgeContainer(docs)


class SlowPurgeContainer(PurgeContainer):
    async def query_items(self, *, query, parameters, partition_key):
        await asyncio.sleep(1)
        for item in []:
            yield item


class SlowPurgeStore:
    def __init__(self):
        self.container = SlowPurgeContainer([])


class RecordingRecall:
    def __init__(self):
        self.deleted = None

    def delete_by_user(self, *, tenant_id, user_id):
        self.deleted = (tenant_id, user_id)

    def upsert_documents(self, documents):
        pass


class HangingRecall:
    def delete_by_user(self, *, tenant_id, user_id):
        import time
        time.sleep(5)


class FakeIngestCore:
    def __init__(self, snapshot, *, delete_missing=False):
        self.snapshot = snapshot
        self.called = False
        self.deleted = False
        self.delete_missing = delete_missing
        self.hydrated_from = None
        self.ingested_turns = None

    def ingest_session(self, *, session_id, turns, metadata):
        self.called = True
        self.ingested_turns = turns
        return self.snapshot

    def delete_session(self, *, session_id):
        if self.delete_missing:
            raise KeyError(session_id)
        self.deleted = True
        return self.snapshot

    def snapshot_for_materialization(self):
        return self.snapshot

    def snapshot_payload(self):
        return b"full-archive"

    def hydrate_from_manifest(self, manifest):
        self.hydrated_from = manifest


class FakeDeletionLog:
    def __init__(self, calls):
        self.calls = calls
        self.records = []

    async def record_deletion(self, **kwargs):
        self.calls.append("deletion_log")
        self.records.append(kwargs)
        return kwargs


def _snapshot():
    tree = MemTree(
        tree_id="entity:alice",
        tree_type="entity",
        tree_key="alice",
        label="Alice",
        k=4,
        root_node_id="root",
        nodes={
            "root": MemTreeNode(
                node_id="root",
                tree_id="entity:alice",
                level=1,
                time_start=1.0,
                time_end=2.0,
                summary="root",
                summary_dirty=False,
                child_ids=["leaf"],
                item_count=1,
            ),
            "leaf": MemTreeNode(
                node_id="leaf",
                tree_id="entity:alice",
                level=0,
                time_start=1.0,
                time_end=1.0,
                summary="leaf",
                summary_dirty=False,
                child_ids=["fact-1"],
                item_count=1,
                parent_id="root",
            ),
        },
        session_leaves={},
        fact_ids_ordered=["fact-1"],
        centroid=None,
        version=1,
        lifecycle_state="active",
        deactivated=False,
        replacement_tree_ids=[],
        created_at=1.0,
        updated_at=2.0,
    )
    return ForestMaterializationSnapshot(facts=[], trees=[tree], node_embeddings={"root": [1.0, 0.0]}, summary_cache={"k": "summary"})


def _factless_snapshot():
    return ForestMaterializationSnapshot(facts=[], trees=[], node_embeddings={}, summary_cache={})


def _envelope(job_type="ingest_session"):
    return build_write_job_envelope(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        session_id="session-1",
        job_type=job_type,
        idempotency_key="idem-1",
        payload={"turns": [{"role": "user", "content": "hello"}], "metadata": {"source": "test"}} if job_type == "ingest_session" else {},
    )


def _handler(*, manifest_store=None, lease_store=None, node_store=None, calls=None, core=None, deletion_log=None):
    calls = calls if calls is not None else []
    return MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=lease_store or FakeLeaseStore(),
            source_session_store=RecordingStore(calls, "source_session"),
            source_turn_store=RecordingStore(calls, "source_turns"),
            fact_store=RecordingStore(calls, "facts"),
            tree_node_store=node_store or RecordingStore(calls, "tree_nodes"),
            fact_location_index=RecordingStore(calls, "locations"),
            manifest_store=manifest_store or FakeManifestStore(),
            recall_index=RecordingStore(calls, "recall"),
            browse_embedding_store=RecordingStore(calls, "browse_embeddings"),
            summary_cache=RecordingStore(calls, "summary_cache"),
            snapshot_store=RecordingStore(calls, "snapshot"),
            deletion_log=deletion_log,
        ),
        ingest_core=core or FakeIngestCore(_snapshot()),
        config=MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1", code_version="test-sha", prompt_config_hash="prompt-hash", lease_renewal_seconds=9999),
    )


@pytest.mark.asyncio
async def test_materialization_handler_commits_after_projection_writes() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    lease_store = FakeLeaseStore()
    handler = _handler(manifest_store=manifest_store, lease_store=lease_store, calls=calls)

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert manifest_store.committed
    assert calls == [
        "source_session",
        "source_turns",
        "facts",
        "tree_nodes",
        "locations",
        "browse_embeddings",
        "summary_cache",
        "recall",
        "snapshot",
        "snapshot",
    ]
    assert lease_store.asserted is True
    assert lease_store.released is True


@pytest.mark.asyncio
async def test_materialization_handler_does_not_commit_factless_ingest() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    handler = _handler(manifest_store=manifest_store, calls=calls, core=FakeIngestCore(_factless_snapshot()))

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert manifest_store.pending is None
    assert manifest_store.committed == []
    assert calls == ["source_session", "source_turns"]


@pytest.mark.asyncio
async def test_materialization_handler_stores_assistant_turns_but_skips_extraction() -> None:
    calls = []
    core = FakeIngestCore(_factless_snapshot())
    handler = _handler(calls=calls, core=core)
    envelope = build_write_job_envelope(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-assistant",
        payload={
            "turns": [
                {"role": "user", "content": "I work at OpenAI as an FDE", "metadata": {"source": "chat-ui"}},
                {"role": "assistant", "content": "Thanks for the update.", "metadata": {"skip_extraction": True}},
            ],
            "metadata": {"source": "test"},
        },
    )

    await handler.handle(envelope, {"fencing_token": 1})

    assert len(core.ingested_turns) == 1
    assert core.ingested_turns[0]["role"] == "user"
    assert core.ingested_turns[0]["content"] == "I work at OpenAI as an FDE"


@pytest.mark.asyncio
async def test_materialization_handler_clear_memory_purges_user_docs() -> None:
    tenant = "tenant-1"
    user = "user-1"
    pk = f"{tenant}:{user}"
    recall = RecordingRecall()
    handler = MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=FakeLeaseStore(),
            source_session_store=PurgeStore([{"id": "session", "tenant_user_key": pk}]),
            source_turn_store=PurgeStore([{"id": "turn", "tenant_user_key": pk}]),
            fact_store=PurgeStore([{"id": "fact", "tenant_user_key": pk}]),
            tree_node_store=PurgeStore([{"id": "node", "tenant_user_key": pk}]),
            fact_location_index=PurgeStore([{"id": "loc", "tenant_user_key": pk}]),
            browse_embedding_store=PurgeStore([{"id": "emb", "tenant_user_key": pk}]),
            manifest_store=PurgeStore([{"id": "manifest", "tenant_id": tenant, "user_id": user}]),
            recall_index=recall,
        ),
        ingest_core=FakeIngestCore(_factless_snapshot()),
        config=MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1", code_version="test-sha", prompt_config_hash="prompt-hash", lease_renewal_seconds=9999),
    )

    await handler.handle(_envelope(job_type="clear_memory"), {"fencing_token": 1})

    assert handler._stores.source_session_store.container.docs == {}
    assert handler._stores.manifest_store.container.docs == {}
    assert recall.deleted == (tenant, user)


@pytest.mark.asyncio
async def test_materialization_handler_clear_memory_bounds_optional_cleanup() -> None:
    tenant = "tenant-1"
    user = "user-1"
    pk = f"{tenant}:{user}"
    handler = MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=FakeLeaseStore(),
            source_session_store=PurgeStore([{"id": "session", "tenant_user_key": pk}]),
            source_turn_store=PurgeStore([]),
            fact_store=PurgeStore([]),
            tree_node_store=PurgeStore([]),
            fact_location_index=PurgeStore([]),
            browse_embedding_store=PurgeStore([]),
            manifest_store=PurgeStore([]),
            recall_index=HangingRecall(),
        ),
        ingest_core=FakeIngestCore(_factless_snapshot()),
        config=MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1", code_version="test-sha", prompt_config_hash="prompt-hash", lease_renewal_seconds=9999, clear_optional_timeout_seconds=0.01),
    )

    await handler.handle(_envelope(job_type="clear_memory"), {"fencing_token": 1})

    assert handler._stores.source_session_store.container.docs == {}
    assert handler._stores.fact_store.container.docs == {}


@pytest.mark.asyncio
async def test_materialization_handler_clear_memory_times_out_critical_phase() -> None:
    handler = MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=FakeLeaseStore(),
            source_session_store=SlowPurgeStore(),
            source_turn_store=PurgeStore([]),
            fact_store=PurgeStore([]),
            tree_node_store=PurgeStore([]),
            fact_location_index=PurgeStore([]),
            browse_embedding_store=PurgeStore([]),
            manifest_store=PurgeStore([]),
            recall_index=RecordingRecall(),
        ),
        ingest_core=FakeIngestCore(_factless_snapshot()),
        config=MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1", code_version="test-sha", prompt_config_hash="prompt-hash", lease_renewal_seconds=9999, clear_phase_timeout_seconds=0.01),
    )

    with pytest.raises(TransientHandlerError, match="clear_memory phase timed out"):
        await handler.handle(_envelope(job_type="clear_memory"), {"fencing_token": 1})


@pytest.mark.asyncio
async def test_materialization_handler_writes_core_snapshot_payload() -> None:
    calls = []
    snapshot_store = RecordingStore(calls, "snapshot")
    handler = MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=FakeLeaseStore(),
            source_session_store=RecordingStore(calls, "source_session"),
            source_turn_store=RecordingStore(calls, "source_turns"),
            fact_store=RecordingStore(calls, "facts"),
            tree_node_store=RecordingStore(calls, "tree_nodes"),
            fact_location_index=RecordingStore(calls, "locations"),
            manifest_store=FakeManifestStore(),
            recall_index=RecordingStore(calls, "recall"),
            snapshot_store=snapshot_store,
        ),
        ingest_core=FakeIngestCore(_snapshot()),
        config=MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1", code_version="test-sha", prompt_config_hash="prompt-hash", lease_renewal_seconds=9999),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert snapshot_store.items[0][1] == b"full-archive"


@pytest.mark.asyncio
async def test_materialization_handler_commits_snapshot_pointer_metadata() -> None:
    manifest_store = FakeManifestStore()
    handler = _handler(manifest_store=manifest_store)

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert manifest_store.latest is not None
    metadata = manifest_store.latest.metadata
    assert metadata["snapshot_id"].endswith(f"{manifest_store.latest.forest_version}.json")
    assert metadata["snapshot_content_sha256"]
    assert metadata["snapshot_size_bytes"] == len(b"full-archive")
    assert metadata["materialization_fingerprint"]
    assert metadata["embedding_model_version"] == "embed-v1"
    assert metadata["code_version"] == "test-sha"
    assert metadata["prompt_config_hash"] == "prompt-hash"
    assert "materialization_fingerprint_inputs" not in metadata
    assert metadata["fingerprint_inputs_id"].endswith(f"{manifest_store.latest.forest_version}.fingerprint.json")


@pytest.mark.asyncio
async def test_materialization_handler_hydrates_from_latest_manifest_before_ingest() -> None:
    envelope = _envelope()
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session"},
    )
    core = FakeIngestCore(_snapshot())
    handler = _handler(manifest_store=manifest_store, core=core)

    await handler.handle(envelope, {"fencing_token": 1})

    assert core.hydrated_from.manifest_id == "manifest-v1"


@pytest.mark.asyncio
async def test_materialization_handler_rejects_hydration_embedding_model_mismatch() -> None:
    envelope = _envelope()
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "other-embed"},
    )
    handler = _handler(manifest_store=manifest_store, core=FakeIngestCore(_snapshot()))

    with pytest.raises(PermanentHandlerError):
        await handler.handle(envelope, {"fencing_token": 1})


@pytest.mark.asyncio
async def test_materialization_handler_noops_for_already_committed_job() -> None:
    envelope = _envelope()
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": envelope.job_id, "job_type": "ingest_session"},
    )
    calls = []
    handler = _handler(manifest_store=manifest_store, calls=calls)

    await handler.handle(envelope, {"fencing_token": 1})

    assert calls == []
    assert manifest_store.committed == []


@pytest.mark.asyncio
async def test_materialization_handler_leaves_manifest_pending_on_projection_failure() -> None:
    manifest_store = FakeManifestStore()
    calls = []
    handler = _handler(manifest_store=manifest_store, node_store=RecordingStore(calls, "tree_nodes", fail=True), calls=calls)

    with pytest.raises(RuntimeError):
        await handler.handle(_envelope(), {"fencing_token": 1})

    assert manifest_store.pending is not None
    assert manifest_store.committed == []


@pytest.mark.asyncio
async def test_materialization_handler_aborts_commit_when_lease_lost_before_commit() -> None:
    manifest_store = FakeManifestStore()
    lease_store = FakeLeaseStore(fail_assert=True)
    handler = _handler(manifest_store=manifest_store, lease_store=lease_store)

    with pytest.raises(TransientHandlerError):
        await handler.handle(_envelope(), {"fencing_token": 1})

    assert manifest_store.pending is not None
    assert manifest_store.committed == []
    assert lease_store.released is True


@pytest.mark.asyncio
async def test_materialization_handler_maps_lease_conflict_to_transient() -> None:
    handler = _handler(lease_store=FakeLeaseStore(conflict=True))

    with pytest.raises(TransientHandlerError):
        await handler.handle(_envelope(), {"fencing_token": 1})


@pytest.mark.asyncio
async def test_delete_handler_tombstones_sources_records_deletion_and_commits() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    deletion_log = FakeDeletionLog(calls)
    core = FakeIngestCore(_snapshot())
    handler = _handler(manifest_store=manifest_store, calls=calls, core=core, deletion_log=deletion_log)

    await handler.handle(_envelope("delete_session"), {"fencing_token": 1})

    assert core.deleted is True
    assert manifest_store.committed
    assert "source_session" in calls
    assert "source_turns" in calls
    assert calls[-1] == "deletion_log"
    assert deletion_log.records[0]["deletion_type"] == "session"
    assert deletion_log.records[0]["metadata"]["deleted_session_id"] == "session-1"


@pytest.mark.asyncio
async def test_delete_handler_treats_missing_session_as_idempotent() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    handler = _handler(manifest_store=manifest_store, calls=calls, core=FakeIngestCore(_snapshot(), delete_missing=True))

    await handler.handle(_envelope("delete_session"), {"fencing_token": 1})

    assert manifest_store.committed


@pytest.mark.asyncio
async def test_delete_handler_duplicate_committed_job_records_deletion_and_noops() -> None:
    envelope = _envelope("delete_session")
    calls = []
    deletion_log = FakeDeletionLog(calls)
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": envelope.job_id, "job_type": "delete_session"},
    )
    handler = _handler(manifest_store=manifest_store, calls=calls, deletion_log=deletion_log)

    await handler.handle(envelope, {"fencing_token": 1})

    assert calls == ["deletion_log"]
    assert manifest_store.committed == []
