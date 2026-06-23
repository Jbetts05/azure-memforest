from __future__ import annotations

import pytest
import asyncio
import time
from dataclasses import replace

from src.build.tree_types import MemTree, MemTreeNode, SessionLeaf
from src.cloud.cosmos_leases import LeaseConflict
from src.cloud.contracts import Lease
from src.cloud.cosmos_manifests import ForestManifest
from src.cloud.projection_baseline import CommittedProjectionBaseline
from src.cloud.projection_plan import ProjectionRecordRef, tree_node_body_content_id
from src.evaluation.incremental_cloud_validation import ProjectionWriteCounts, compare_projection_write_reduction
from src.forest.user_forest import ForestMaterializationSnapshot
from src.service.auth import ActorContext, TenantContext
from src.service.jobs import build_write_job_envelope
from src.worker.consumer import TransientHandlerError
from src.observability.model_telemetry import record_model_call
from src.worker.progress import JobProgressBudget
from src.worker.materialization import (
    MaterializationHandlerConfig,
    MaterializationStores,
    MemForestMaterializationHandler,
    SNAPSHOT_POLICY_ALWAYS,
    SNAPSHOT_POLICY_CLOUD_CHECKPOINT,
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
        changed_tree_ids=frozenset({"entity:alice"}),
        deleted_tree_ids=frozenset({"entity:old"}),
        baseline_forest_version="v1",
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
    assert {node.metadata["k"] for node in projection.stored_nodes} == {4}
    entity_root = next(node for node in projection.stored_nodes if node.node_id == "entity-root")
    entity_root_ref = next(
        record
        for record in projection.projection_skip_plans["tree_node_membership"].upsert
        if record.logical_id == "entity:alice/entity-root"
    )
    assert entity_root_ref.content_id == tree_node_body_content_id(
        tenant_user_key="tenant-1:user-1",
        tree_id=entity_root.tree_id,
        node_id=entity_root.node_id,
        content_sha256=entity_root_ref.content_hash,
    )
    assert b'"forest_version":"v2"' in projection.snapshot_payload
    assert projection.changed_tree_ids == frozenset({"entity:alice"})
    assert projection.deleted_tree_ids == frozenset({"entity:old"})
    assert projection.baseline_forest_version == "v1"
    assert len(projection.projection_skip_plans["tree_node_membership"].unchanged) == 0
    assert len(projection.projection_skip_plans["tree_node_membership"].upsert) == 4
    assert len(projection.projection_skip_plans["fact_location"].unchanged) == 0
    assert len(projection.projection_skip_plans["fact_location"].upsert) == 2
    assert len(projection.projection_skip_plans["recall_document"].unchanged) == 0
    assert len(projection.projection_skip_plans["recall_document"].upsert) == 2
    assert len(projection.projection_skip_plans["browse_embedding"].unchanged) == 0
    assert len(projection.projection_skip_plans["browse_embedding"].upsert) == 3
    assert len(projection.projection_skip_plans["summary_cache"].unchanged) == 0
    assert len(projection.projection_skip_plans["summary_cache"].upsert) == 1


def test_project_forest_snapshot_normalizes_cloud_fanout_k_for_stable_hashes() -> None:
    def _projection_for_k(k: int):
        tree = MemTree(
            tree_id="session:s1",
            tree_type="session",
            tree_key="s1",
            label="s1",
            k=k,
            root_node_id="session-root",
            nodes={
                "session-root": MemTreeNode(
                    node_id="session-root",
                    tree_id="session:s1",
                    level=0,
                    time_start=1.0,
                    time_end=1.0,
                    summary="User bought a blue kayak.",
                    summary_dirty=False,
                    child_ids=["session-raw#cell_0000"],
                    item_count=1,
                )
            },
            session_leaves={
                "session-raw#cell_0000": SessionLeaf(
                    cell_id="session-raw#cell_0000",
                    session_id="session-raw",
                    cell_index=0,
                    time_start=1.0,
                    time_end=1.0,
                    raw_turns=[],
                    raw_text="User bought a blue kayak.",
                    fact_ids=["fact-1"],
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
        return project_forest_snapshot(
            ForestMaterializationSnapshot(
                facts=[],
                trees=[tree],
                node_embeddings={},
                summary_cache={},
            ),
            tenant_id="tenant-1",
            user_id="user-1",
            forest_version=f"v{k}",
            generation=k,
            embedding_model_version="embed-v1",
        )

    k2_projection = _projection_for_k(2)
    k3_projection = _projection_for_k(3)
    k2_ref = k2_projection.projection_skip_plans["tree_node_membership"].upsert[0]
    k3_ref = k3_projection.projection_skip_plans["tree_node_membership"].upsert[0]

    assert k2_projection.stored_nodes[0].metadata["k"] == 3
    assert k3_projection.stored_nodes[0].metadata["k"] == 3
    assert k2_ref.content_hash == k3_ref.content_hash
    assert k2_ref.content_id == k3_ref.content_id


def test_project_forest_snapshot_defaults_to_full_projection_required() -> None:
    snapshot = ForestMaterializationSnapshot(
        facts=[],
        trees=[],
        node_embeddings={},
        summary_cache={},
    )

    projection = project_forest_snapshot(
        snapshot,
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        embedding_model_version="embed-v1",
    )

    assert projection.changed_tree_ids is None
    assert projection.deleted_tree_ids == frozenset()
    assert projection.baseline_forest_version is None


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

    async def upsert_nodes_delta(self, nodes, *, delete_keys=None, unchanged_keys=None):
        self.calls.append(self.name)
        if self.fail:
            raise RuntimeError("node write failed")
        self.items.append({"upsert_nodes": list(nodes), "delete_keys": list(delete_keys or []), "unchanged_keys": list(unchanged_keys or [])})

    async def upsert_locations(self, locations):
        self.calls.append(self.name)
        self.items.extend(locations)

    async def upsert_locations_delta(self, locations, *, delete_keys=None, unchanged_keys=None):
        self.calls.append(self.name)
        self.items.append({"upsert_locations": list(locations), "delete_keys": list(delete_keys or []), "unchanged_keys": list(unchanged_keys or [])})

    def upsert_documents(self, documents):
        self.calls.append(self.name)
        self.items.extend(documents)

    def upsert_documents_delta(self, documents, *, close_refs=None, forest_version=None):
        self.calls.append(self.name)
        self.items.append({"upsert_documents": list(documents), "close_refs": list(close_refs or []), "forest_version": forest_version})

    def upsert_embeddings(self, embeddings, *, model_version):
        self.calls.append(self.name)
        self.items.append((dict(embeddings), model_version))

    def put(self, content_hash, summary, *, metadata=None):
        self.calls.append(self.name)
        self.items.append((content_hash, summary, metadata))

    def write_snapshot(self, *, snapshot_id, payload, metadata):
        self.calls.append(self.name)
        self.items.append((snapshot_id, payload, metadata))


class BaselineContainer:
    def __init__(self, docs):
        self.docs = list(docs)

    def query_items(self, *, query, parameters, partition_key):
        params = {param["name"]: param["value"] for param in parameters}
        result = []
        for doc in self.docs:
            if doc.get("tenant_user_key", partition_key) != partition_key:
                continue
            if "@forest_version" in params and doc.get("forest_version") != params["@forest_version"]:
                continue
            if "@embedding_model_version" in params and doc.get("embedding_model_version") != params["@embedding_model_version"]:
                continue
            result.append(dict(doc))
        return result


class BaselineRecordingStore(RecordingStore):
    def __init__(self, calls, name, docs):
        super().__init__(calls, name)
        self.container = BaselineContainer(docs)


class BaselineRecall(RecordingStore):
    def __init__(self, calls, docs):
        super().__init__(calls, "recall")
        self.docs = list(docs)
        self._search_client = self

    def search(self, **kwargs):
        return list(self.docs)


class SummaryCacheRecordingStore(RecordingStore):
    def __init__(self, calls, entries):
        super().__init__(calls, "summary_cache")
        self.entries = dict(entries)

    def get_many(self, keys):
        return {key: self.entries[key] for key in keys if key in self.entries}


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


class FakeJobStore:
    def __init__(self):
        self.stage_updates = []

    async def update_stage(self, **kwargs):
        self.stage_updates.append(kwargs)


class FakeIngestCore:
    def __init__(self, snapshot, *, delete_missing=False, ingest_delay=0.0, model_calls=0):
        self.snapshot = snapshot
        self.called = False
        self.deleted = False
        self.delete_missing = delete_missing
        self.hydrated_from = None
        self.ingested_turns = None
        self.ingest_delay = ingest_delay
        self.model_calls = model_calls
        self.last_hydration_result = None
        self.partial_hydration_checks = []
        self.partial_hydration_equivalence_checks = []
        self.partial_hydration_equivalence_snapshot = snapshot
        self.partial_hydration_check_result = {
            "supported": True,
            "complete": True,
            "tree_count": 1,
            "fact_count": 1,
            "summary_cache_count": 0,
            "node_embedding_count": 1,
            "missing_fact_count": 0,
            "missing_summary_cache_count": 0,
            "missing_node_embedding_count": 0,
        }

    def ingest_session(self, *, session_id, turns, metadata):
        self.called = True
        self.ingested_turns = turns
        for _ in range(self.model_calls):
            record_model_call(role="extraction", deployment="test", operation="chat", elapsed_seconds=0.001)
        if self.ingest_delay:
            time.sleep(self.ingest_delay)
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
        if manifest is not None:
            self.last_hydration_result = type(
                "HydrationResult",
                (),
                {
                    "tree_count": 2,
                    "fact_count": 1,
                    "summary_cache_count": 3,
                    "node_embedding_count": 4,
                    "missing_fact_ids": (),
                    "missing_summary_cache_keys": ("missing-summary",),
                    "missing_node_embedding_ids": (),
                    "used_snapshot_fallback": False,
                },
            )()

    def check_partial_hydration_from_manifest(self, manifest, *, tree_ids):
        self.partial_hydration_checks.append({"manifest": manifest, "tree_ids": tuple(tree_ids)})
        return dict(self.partial_hydration_check_result)

    def partial_hydration_shadow_snapshot_from_manifest(self, manifest, *, tree_ids, session_id, turns, metadata):
        self.partial_hydration_equivalence_checks.append(
            {
                "manifest": manifest,
                "tree_ids": tuple(tree_ids),
                "session_id": session_id,
                "turns": list(turns),
                "metadata": dict(metadata),
            }
        )
        return self.partial_hydration_equivalence_snapshot


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


def _unchanged_incremental_snapshot():
    snapshot = _snapshot()
    return ForestMaterializationSnapshot(
        facts=snapshot.facts,
        trees=snapshot.trees,
        node_embeddings=snapshot.node_embeddings,
        summary_cache=snapshot.summary_cache,
        changed_tree_ids=frozenset(),
        deleted_tree_ids=frozenset(),
        baseline_forest_version="v1",
    )


def _two_tree_incremental_snapshot():
    changed = _snapshot().trees[0]
    unchanged = MemTree(
        tree_id="entity:bob",
        tree_type="entity",
        tree_key="bob",
        label="Bob",
        k=4,
        root_node_id="bob-root",
        nodes={
            "bob-root": MemTreeNode(
                node_id="bob-root",
                tree_id="entity:bob",
                level=1,
                time_start=1.0,
                time_end=2.0,
                summary="bob root",
                summary_dirty=False,
                child_ids=["bob-leaf"],
                item_count=1,
            ),
            "bob-leaf": MemTreeNode(
                node_id="bob-leaf",
                tree_id="entity:bob",
                level=0,
                time_start=1.0,
                time_end=1.0,
                summary="bob leaf",
                summary_dirty=False,
                child_ids=["fact-bob"],
                item_count=1,
                parent_id="bob-root",
            ),
        },
        session_leaves={},
        fact_ids_ordered=["fact-bob"],
        centroid=None,
        version=1,
        lifecycle_state="active",
        deactivated=False,
        replacement_tree_ids=[],
        created_at=1.0,
        updated_at=2.0,
    )
    return ForestMaterializationSnapshot(
        facts=[],
        trees=[changed, unchanged],
        node_embeddings={
            "root": [1.0, 0.0],
            "bob-root": [0.0, 1.0],
        },
        summary_cache={"k": "summary"},
        changed_tree_ids=frozenset({"entity:alice"}),
        deleted_tree_ids=frozenset(),
        baseline_forest_version="v1",
    )


def _two_tree_incremental_snapshot_with_deleted_tree():
    snapshot = _two_tree_incremental_snapshot()
    return ForestMaterializationSnapshot(
        facts=snapshot.facts,
        trees=snapshot.trees,
        node_embeddings=snapshot.node_embeddings,
        summary_cache=snapshot.summary_cache,
        changed_tree_ids=snapshot.changed_tree_ids,
        deleted_tree_ids=frozenset({"entity:old"}),
        baseline_forest_version=snapshot.baseline_forest_version,
    )


def _many_changed_tree_snapshot(count: int):
    trees = []
    node_embeddings = {}
    changed_tree_ids = set()
    for index in range(count):
        tree_id = f"entity:tree-{index:03d}"
        node_id = f"root-{index:03d}"
        fact_id = f"fact-{index:03d}"
        trees.append(
            MemTree(
                tree_id=tree_id,
                tree_type="entity",
                tree_key=f"tree-{index:03d}",
                label=f"Tree {index:03d}",
                k=4,
                root_node_id=node_id,
                nodes={
                    node_id: MemTreeNode(
                        node_id=node_id,
                        tree_id=tree_id,
                        level=0,
                        time_start=1.0,
                        time_end=1.0,
                        summary=f"summary {index}",
                        summary_dirty=False,
                        child_ids=[fact_id],
                        item_count=1,
                    )
                },
                session_leaves={},
                fact_ids_ordered=[fact_id],
                centroid=None,
                version=1,
                lifecycle_state="active",
                deactivated=False,
                replacement_tree_ids=[],
                created_at=1.0,
                updated_at=1.0,
            )
        )
        node_embeddings[node_id] = [1.0, 0.0]
        changed_tree_ids.add(tree_id)
    return ForestMaterializationSnapshot(
        facts=[],
        trees=trees,
        node_embeddings=node_embeddings,
        summary_cache={},
        changed_tree_ids=frozenset(changed_tree_ids),
        deleted_tree_ids=frozenset(),
        baseline_forest_version="v1",
    )


def _envelope(job_type="ingest_session"):
    return build_write_job_envelope(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        session_id="session-1",
        job_type=job_type,
        idempotency_key="idem-1",
        payload={"turns": [{"role": "user", "content": "hello"}], "metadata": {"source": "test"}} if job_type == "ingest_session" else {},
    )


def test_materialization_config_defaults_to_always_snapshot_policy() -> None:
    config = MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1")

    assert config.snapshot_policy == SNAPSHOT_POLICY_ALWAYS
    assert config.cloud_checkpoint_hydration_supported is False


def test_materialization_config_rejects_checkpoint_policy_without_hydration_support() -> None:
    with pytest.raises(ValueError, match="requires explicit cloud checkpoint hydration support"):
        MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            snapshot_policy="cloud_checkpoint",
        )


def _baseline_doc(pk, doc_kind, **overrides):
    doc = {
        "tenant_id": "tenant-1",
        "user_id": "user-1",
        "tenant_user_key": pk,
        "forest_version": "v1",
        "tombstoned": False,
    }
    if doc_kind is not None:
        doc["doc_kind"] = doc_kind
    doc.update(overrides)
    return doc


class MatchingProjectionBaselineProvider:
    def __init__(self):
        self.calls = []

    def __call__(self, *, scope, projection):
        self.calls.append(scope)
        records = {
            kind: tuple(plan.upsert)
            for kind, plan in projection.projection_skip_plans.items()
        }
        records["browse_embedding"] = (
            *records["browse_embedding"],
            ProjectionRecordRef("browse_embedding", "removed-node/embed-v1", content_hash="removed"),
        )
        records["summary_cache"] = (
            *records["summary_cache"],
            ProjectionRecordRef("summary_cache", "removed-summary", content_hash="removed"),
        )
        return CommittedProjectionBaseline(
            scope=scope,
            records=records,
        )


class FailingProjectionBaselineProvider:
    def __call__(self, *, scope, projection):
        raise RuntimeError("baseline unavailable")


def _handler(*, manifest_store=None, lease_store=None, node_store=None, location_store=None, recall_index=None, browse_store=None, calls=None, core=None, deletion_log=None, job_store=None, config=None, baseline_provider=None):
    calls = calls if calls is not None else []
    return MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=lease_store or FakeLeaseStore(),
            source_session_store=RecordingStore(calls, "source_session"),
            source_turn_store=RecordingStore(calls, "source_turns"),
            fact_store=RecordingStore(calls, "facts"),
            tree_node_store=node_store or RecordingStore(calls, "tree_nodes"),
            fact_location_index=location_store or RecordingStore(calls, "locations"),
            manifest_store=manifest_store or FakeManifestStore(),
            recall_index=recall_index or RecordingStore(calls, "recall"),
            browse_embedding_store=browse_store or RecordingStore(calls, "browse_embeddings"),
            summary_cache=RecordingStore(calls, "summary_cache"),
            snapshot_store=RecordingStore(calls, "snapshot"),
            deletion_log=deletion_log,
            job_store=job_store,
            baseline_provider=baseline_provider,
        ),
        ingest_core=core or FakeIngestCore(_snapshot()),
        config=config
        or MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1", code_version="test-sha", prompt_config_hash="prompt-hash", lease_renewal_seconds=9999),
    )


def _actual_projection_write_counts(tree_nodes, locations, recall, browse, summary) -> ProjectionWriteCounts:
    return ProjectionWriteCounts(
        stored_nodes=sum(
            len(item["upsert_nodes"]) if isinstance(item, dict) else 1
            for item in tree_nodes.items
        ),
        fact_locations=sum(
            len(item["upsert_locations"]) if isinstance(item, dict) else 1
            for item in locations.items
        ),
        recall_documents=sum(
            len(item["upsert_documents"]) if isinstance(item, dict) else 1
            for item in recall.items
        ),
        browse_embeddings=len(browse.items),
        summary_cache_entries=len(summary.items),
    )


@pytest.mark.asyncio
async def test_materialization_handler_commits_after_projection_writes() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    lease_store = FakeLeaseStore()
    handler = _handler(manifest_store=manifest_store, lease_store=lease_store, calls=calls)

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert manifest_store.committed
    assert calls[:2] == ["source_session", "source_turns"]
    assert calls[-2:] == ["snapshot", "snapshot"]
    assert set(calls[2:-2]) == {
        "facts",
        "tree_nodes",
        "locations",
        "browse_embeddings",
        "summary_cache",
        "recall",
    }
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
async def test_materialization_handler_heartbeats_during_extract_build() -> None:
    job_store = FakeJobStore()
    handler = _handler(
        core=FakeIngestCore(_factless_snapshot(), ingest_delay=0.2),
        job_store=job_store,
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            extract_heartbeat_seconds=0.01,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    extract_updates = [update for update in job_store.stage_updates if update["stage"] == "extract_build"]
    assert len(extract_updates) >= 2
    assert extract_updates[-1]["telemetry"]["extract_build_heartbeat_count"] >= 1


@pytest.mark.asyncio
async def test_materialization_handler_times_out_ingest_session() -> None:
    lease_store = FakeLeaseStore()
    handler = _handler(
        lease_store=lease_store,
        core=FakeIngestCore(_snapshot(), ingest_delay=0.1),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            ingest_timeout_seconds=0.01,
        ),
    )

    with pytest.raises(TransientHandlerError, match="ingest_session timed out") as exc_info:
        await handler.handle(_envelope(), {"fencing_token": 1})
    assert exc_info.value.exit_code is None
    assert lease_store.released is True


@pytest.mark.asyncio
async def test_materialization_handler_marks_timeout_as_fatal_when_configured() -> None:
    handler = _handler(
        core=FakeIngestCore(_snapshot(), ingest_delay=0.1),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            ingest_timeout_seconds=0.01,
            exit_on_ingest_timeout=True,
        ),
    )

    with pytest.raises(TransientHandlerError, match="ingest_session timed out") as exc_info:
        await handler.handle(_envelope(), {"fencing_token": 1})
    assert exc_info.value.exit_code == 75


@pytest.mark.asyncio
async def test_materialization_handler_stops_when_model_call_budget_exceeded(monkeypatch) -> None:
    def tiny_budget(envelope):
        return JobProgressBudget(
            job_type="ingest_session",
            stall_timeout_seconds=999,
            max_completed_model_calls=0,
            max_retries=1,
        )

    monkeypatch.setattr("src.worker.materialization.budget_for_envelope", tiny_budget)
    job_store = FakeJobStore()
    handler = _handler(
        core=FakeIngestCore(_snapshot(), ingest_delay=0.2, model_calls=1),
        job_store=job_store,
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            extract_heartbeat_seconds=0.01,
        ),
    )

    with pytest.raises(TransientHandlerError, match="model-call budget"):
        await handler.handle(_envelope(), {"fencing_token": 1})
    final_update = job_store.stage_updates[-1]
    assert final_update["stage"] == "extract_build"
    assert final_update["telemetry"]["total_calls"] > 0


@pytest.mark.asyncio
async def test_materialization_handler_uses_claimed_job_progress_budget(monkeypatch) -> None:
    def tiny_budget(envelope):
        return JobProgressBudget(
            job_type="ingest_session",
            stall_timeout_seconds=999,
            max_completed_model_calls=0,
            max_retries=1,
        )

    monkeypatch.setattr("src.worker.materialization.budget_for_envelope", tiny_budget)
    handler = _handler(
        core=FakeIngestCore(_snapshot(), ingest_delay=0.2, model_calls=1),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            extract_heartbeat_seconds=0.01,
        ),
    )

    await handler.handle(
        _envelope(),
        {
            "fencing_token": 1,
            "progress_budget": {
                "job_type": "ingest_session",
                "stall_timeout_seconds": 999,
                "max_completed_model_calls": 10,
                "max_retries": 1,
            },
        },
    )


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

    assert handler._config.snapshot_policy == SNAPSHOT_POLICY_ALWAYS
    assert snapshot_store.items[0][1] == b"full-archive"
    assert len(snapshot_store.items) == 2


@pytest.mark.asyncio
async def test_materialization_handler_cloud_checkpoint_skips_archive_snapshot_payload() -> None:
    calls = []
    snapshot_store = RecordingStore(calls, "snapshot")
    manifest_store = FakeManifestStore()
    handler = MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=FakeLeaseStore(),
            source_session_store=RecordingStore(calls, "source_session"),
            source_turn_store=RecordingStore(calls, "source_turns"),
            fact_store=RecordingStore(calls, "facts"),
            tree_node_store=RecordingStore(calls, "tree_nodes"),
            fact_location_index=RecordingStore(calls, "locations"),
            manifest_store=manifest_store,
            recall_index=RecordingStore(calls, "recall"),
            snapshot_store=snapshot_store,
        ),
        ingest_core=FakeIngestCore(_snapshot()),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            snapshot_policy=SNAPSHOT_POLICY_CLOUD_CHECKPOINT,
            cloud_checkpoint_hydration_supported=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert manifest_store.latest is not None
    assert manifest_store.latest.metadata["snapshot_policy"] == SNAPSHOT_POLICY_CLOUD_CHECKPOINT
    assert "snapshot_id" not in manifest_store.latest.metadata
    assert len(snapshot_store.items) == 1
    assert snapshot_store.items[0][0].endswith(".fingerprint.json")


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
    assert metadata["projection_delta_full_required"] is True
    assert metadata["projection_delta_changed_tree_ids"] is None
    assert metadata["projection_delta_deleted_tree_ids"] == []
    assert metadata["projection_plan_counts"]["tree_node_membership"]["upsert"] == 2
    assert metadata["projection_plan_counts"]["browse_embedding"]["unchanged"] == 0
    assert metadata["dirty_path_plan"]["mode"] == "instrumentation_only"
    assert metadata["dirty_path_plan"]["skip_plans_present"] is True
    assert metadata["dirty_path_plan"]["id_arrays_truncated"] is False
    assert metadata["dirty_path_plan"]["full_projection_required"] is True
    assert metadata["dirty_path_plan"]["affected_tree_count"] == 1
    assert metadata["dirty_path_plan"]["dirty_node_count"] == 2
    assert metadata["dirty_path_plan"]["affected_fact_count"] == 1
    assert metadata["projection_tree_scope_plan"]["mode"] == "instrumentation_only"
    assert metadata["projection_tree_scope_plan"]["full_projection_required"] is True
    assert metadata["projection_tree_scope_plan"]["projected_tree_count"] == 1
    assert metadata["projection_tree_scope_plan"]["omitted_tree_count"] == 0
    assert metadata["projection_tree_scope_plan"]["scoped_tree_node_ref_count"] == 2
    assert metadata["projection_tree_scope_plan"]["full_tree_node_ref_count"] == 2
    assert metadata["projection_tree_scope_shadow"]["mode"] == "shadow_compare"
    assert metadata["projection_tree_scope_shadow"]["comparison_supported"] is False
    assert metadata["projection_tree_scope_shadow"]["reason"] == "full_projection_required"
    assert metadata["partial_hydration_plan"]["mode"] == "instrumentation_only"
    assert metadata["partial_hydration_plan"]["full_hydration_required"] is True
    assert metadata["partial_hydration_plan"]["planned_tree_load_count"] == 1
    assert metadata["partial_hydration_plan"]["current_full_tree_count"] == 1
    assert set(metadata["materialization_stage_timings_ms"]) >= {
        "hydrate",
        "source_write",
        "extract_build",
        "manifest_pending",
        "project",
        "baseline",
        "write_projection",
        "total_before_commit",
    }
    assert metadata["projection_input_counts"]["stored_nodes"] == 2
    assert metadata["projection_input_counts"]["recall_documents"] == 1
    assert metadata["projection_input_counts"]["browse_embedding_records"] == 1
    assert metadata["actual_projection_write_plan_counts"]["tree_node_membership"]["upsert"] == 2
    assert metadata["actual_projection_write_plan_counts"]["recall_document"]["upsert"] == 1
    assert metadata["hydration_summary"] == {"available": False}
    assert metadata["partial_hydration_shadow"] == {
        "schema_version": 1,
        "enabled": False,
        "supported": False,
        "reason": "disabled",
    }
    assert metadata["partial_hydration_equivalence_shadow"] == {
        "schema_version": 1,
        "enabled": False,
        "supported": False,
        "reason": "disabled",
    }
    assert metadata["partial_hydration_eligibility"]["enabled"] is False
    assert metadata["partial_hydration_eligibility"]["eligible"] is False
    assert metadata["partial_hydration_eligibility"]["reason"] == "disabled"


@pytest.mark.asyncio
async def test_materialization_handler_records_hydration_summary_when_manifest_exists() -> None:
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    handler = _handler(
        manifest_store=manifest_store,
        core=FakeIngestCore(_unchanged_incremental_snapshot()),
        baseline_provider=MatchingProjectionBaselineProvider(),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert manifest_store.latest.metadata["hydration_summary"] == {
        "available": True,
        "tree_count": 2,
        "fact_count": 1,
        "summary_cache_count": 3,
        "node_embedding_count": 4,
        "missing_fact_count": 0,
        "missing_summary_cache_count": 1,
        "missing_node_embedding_count": 0,
        "used_snapshot_fallback": False,
    }


@pytest.mark.asyncio
async def test_materialization_handler_records_partial_hydration_shadow_when_enabled() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    core = FakeIngestCore(_two_tree_incremental_snapshot())
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=core,
        baseline_provider=MatchingProjectionBaselineProvider(),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_partial_hydration_shadow=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    shadow = manifest_store.latest.metadata["partial_hydration_shadow"]
    assert shadow["enabled"] is True
    assert shadow["supported"] is True
    assert shadow["complete"] is True
    assert shadow["plan"]["full_hydration_required"] is False
    assert shadow["plan"]["planned_tree_ids"] == ["entity:alice"]
    assert shadow["check"]["tree_count"] == 1
    assert core.partial_hydration_checks[0]["tree_ids"] == ("entity:alice",)


@pytest.mark.asyncio
async def test_materialization_handler_partial_hydration_shadow_checks_full_planned_tree_set() -> None:
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    core = FakeIngestCore(_many_changed_tree_snapshot(101))
    handler = _handler(
        manifest_store=manifest_store,
        core=core,
        baseline_provider=MatchingProjectionBaselineProvider(),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_partial_hydration_shadow=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    shadow = manifest_store.latest.metadata["partial_hydration_shadow"]
    assert shadow["plan"]["planned_tree_load_count"] == 101
    assert len(shadow["plan"]["planned_tree_ids"]) == 100
    assert shadow["plan"]["id_arrays_truncated"] is True
    assert len(core.partial_hydration_checks[0]["tree_ids"]) == 101
    assert "entity:tree-100" in core.partial_hydration_checks[0]["tree_ids"]


@pytest.mark.asyncio
async def test_materialization_handler_records_partial_hydration_equivalence_shadow_when_enabled() -> None:
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    core = FakeIngestCore(_two_tree_incremental_snapshot())
    core.partial_hydration_equivalence_snapshot = replace(
        _two_tree_incremental_snapshot(),
        baseline_forest_version=None,
    )
    handler = _handler(
        manifest_store=manifest_store,
        core=core,
        baseline_provider=MatchingProjectionBaselineProvider(),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_partial_hydration_shadow=True,
            enable_partial_hydration_equivalence_shadow=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    shadow = manifest_store.latest.metadata["partial_hydration_equivalence_shadow"]
    assert shadow["enabled"] is True
    assert shadow["supported"] is True
    assert shadow["equivalent"] is True
    assert shadow["comparison"]["projection_refs_equivalent"] is True
    assert shadow["comparison"]["delta_metadata_equivalent"] is True
    assert shadow["comparison"]["same_baseline_forest_version"] is False
    assert shadow["comparison"]["shadow_baseline_forest_version"] is None
    assert core.partial_hydration_equivalence_checks[0]["tree_ids"] == ("entity:alice",)
    assert core.partial_hydration_equivalence_checks[0]["session_id"] == "session-1"
    eligibility = manifest_store.latest.metadata["partial_hydration_eligibility"]
    assert eligibility["mode"] == "metadata_only"
    assert eligibility["eligible"] is True
    assert eligibility["reason"] == "eligible"
    assert eligibility["global_fact_scope"] == "all"
    assert eligibility["loaded_fact_count"] == 1
    assert eligibility["loaded_subset_contains_changed"] is True
    assert eligibility["check"]["complete"] is True
    assert eligibility["check"]["loaded_fact_count"] == 1
    assert eligibility["equivalence"] == {"supported": True, "equivalent": True, "reason": None}


@pytest.mark.asyncio
async def test_materialization_handler_partial_hydration_equivalence_shadow_reports_divergence() -> None:
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    core = FakeIngestCore(_two_tree_incremental_snapshot())
    core.partial_hydration_equivalence_snapshot = _snapshot()
    handler = _handler(
        manifest_store=manifest_store,
        core=core,
        baseline_provider=MatchingProjectionBaselineProvider(),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_partial_hydration_shadow=True,
            enable_partial_hydration_equivalence_shadow=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    shadow = manifest_store.latest.metadata["partial_hydration_equivalence_shadow"]
    assert shadow["enabled"] is True
    assert shadow["supported"] is True
    assert shadow["equivalent"] is False
    assert shadow["comparison"]["projection_refs_equivalent"] is False
    assert shadow["comparison"]["delta_metadata_equivalent"] is False
    assert shadow["comparison"]["by_kind"]["tree_node_membership"]["missing_count"] > 0
    eligibility = manifest_store.latest.metadata["partial_hydration_eligibility"]
    assert eligibility["eligible"] is False
    assert eligibility["reason"] == "not_equivalent"


@pytest.mark.asyncio
async def test_materialization_handler_partial_hydration_shadow_fails_closed_on_unknown_delta() -> None:
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    core = FakeIngestCore(_snapshot())
    handler = _handler(
        manifest_store=manifest_store,
        core=core,
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_partial_hydration_shadow=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    shadow = manifest_store.latest.metadata["partial_hydration_shadow"]
    assert shadow["enabled"] is True
    assert shadow["supported"] is False
    assert shadow["reason"] == "full_hydration_required"
    assert core.partial_hydration_checks == []
    eligibility = manifest_store.latest.metadata["partial_hydration_eligibility"]
    assert eligibility["eligible"] is False
    assert eligibility["reason"] == "disabled"


@pytest.mark.asyncio
async def test_materialization_handler_partial_hydration_eligibility_reports_unknown_delta() -> None:
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    handler = _handler(
        manifest_store=manifest_store,
        core=FakeIngestCore(_snapshot()),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_partial_hydration_shadow=True,
            enable_partial_hydration_equivalence_shadow=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    eligibility = manifest_store.latest.metadata["partial_hydration_eligibility"]
    assert eligibility["enabled"] is True
    assert eligibility["eligible"] is False
    assert eligibility["reason"] == "unknown_delta"
    assert eligibility["loaded_subset_contains_changed"] is None


@pytest.mark.asyncio
async def test_materialization_handler_partial_hydration_eligibility_requires_complete_check() -> None:
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    core = FakeIngestCore(_two_tree_incremental_snapshot())
    core.partial_hydration_equivalence_snapshot = replace(
        _two_tree_incremental_snapshot(),
        baseline_forest_version=None,
    )
    core.partial_hydration_check_result = {
        **core.partial_hydration_check_result,
        "complete": False,
        "missing_fact_count": 1,
    }
    handler = _handler(
        manifest_store=manifest_store,
        core=core,
        baseline_provider=MatchingProjectionBaselineProvider(),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_partial_hydration_shadow=True,
            enable_partial_hydration_equivalence_shadow=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    eligibility = manifest_store.latest.metadata["partial_hydration_eligibility"]
    assert eligibility["eligible"] is False
    assert eligibility["reason"] == "missing_referenced_facts"
    assert eligibility["check"]["missing_fact_count"] == 1


@pytest.mark.asyncio
async def test_materialization_handler_records_projection_plan_without_unsafe_live_skips() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_unchanged_incremental_snapshot()),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert manifest_store.latest is not None
    metadata = manifest_store.latest.metadata
    assert metadata["projection_plan_counts"]["tree_node_membership"] == {"unchanged": 0, "upsert": 2, "delete": 0}
    assert metadata["projection_plan_counts"]["fact_location"] == {"unchanged": 0, "upsert": 1, "delete": 0}
    assert metadata["projection_plan_counts"]["recall_document"] == {"unchanged": 0, "upsert": 1, "delete": 0}
    assert metadata["projection_plan_counts"]["browse_embedding"] == {"unchanged": 0, "upsert": 1, "delete": 0}
    assert metadata["projection_plan_counts"]["summary_cache"] == {"unchanged": 0, "upsert": 1, "delete": 0}
    assert metadata["projection_tree_scope_plan"]["full_projection_required"] is False
    assert metadata["projection_tree_scope_plan"]["projected_tree_count"] == 0
    assert metadata["projection_tree_scope_plan"]["omitted_tree_count"] == 1
    assert metadata["projection_tree_scope_plan"]["scoped_tree_node_ref_count"] == 0
    assert metadata["projection_tree_scope_plan"]["full_tree_node_ref_count"] == 2
    assert metadata["projection_tree_scope_shadow"]["comparison_supported"] is True
    assert metadata["projection_tree_scope_shadow"]["divergent"] is True
    assert metadata["projection_tree_scope_shadow"]["by_kind"]["tree_node_membership"]["missing_upsert"] == 2
    assert metadata["partial_hydration_plan"]["full_hydration_required"] is False
    assert metadata["partial_hydration_plan"]["planned_tree_load_count"] == 0
    assert metadata["partial_hydration_plan"]["omitted_tree_count"] == 1
    assert metadata["projection_plan_live_skipped_browse_embedding_count"] == 0
    assert metadata["projection_plan_live_skipped_summary_cache_count"] == 0
    assert "tree_nodes" in calls
    assert "locations" in calls
    assert "recall" in calls
    assert "browse_embeddings" in calls
    assert "summary_cache" in calls


@pytest.mark.asyncio
async def test_materialization_handler_records_partial_hydration_deleted_tree_plan() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_two_tree_incremental_snapshot_with_deleted_tree()),
        baseline_provider=MatchingProjectionBaselineProvider(),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    plan = manifest_store.latest.metadata["partial_hydration_plan"]
    assert plan["full_hydration_required"] is False
    assert plan["current_full_tree_count"] == 2
    assert plan["projected_surface_tree_count"] == 2
    assert plan["planned_tree_load_count"] == 2
    assert plan["planned_tree_ids"] == ["entity:alice", "entity:old"]
    assert plan["deleted_tree_count"] == 1
    assert plan["deleted_tree_ids"] == ["entity:old"]
    assert plan["omitted_tree_count"] == 1
    assert plan["omitted_tree_ids"] == ["entity:bob"]


@pytest.mark.asyncio
async def test_materialization_handler_skips_only_safe_live_writes_from_committed_baseline() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        committed_at_unix=2.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    pk = "tenant-1:user-1"
    tree_nodes = BaselineRecordingStore(
        calls,
        "tree_nodes",
        [
            _baseline_doc(pk, "tree_node_membership", tree_id="entity:alice", node_id="root", content_sha256="same-root"),
            _baseline_doc(pk, "tree_node_membership", tree_id="entity:alice", node_id="leaf", content_sha256="same-leaf"),
        ],
    )
    locations = BaselineRecordingStore(
        calls,
        "locations",
        [_baseline_doc(pk, "fact_location", fact_id="fact-1", tree_id="entity:alice", node_id="leaf", tree_type="entity", level="L0", content_sha256="same-location")],
    )
    browse_embeddings = BaselineRecordingStore(
        calls,
        "browse_embeddings",
        [
            _baseline_doc(
                pk,
                None,
                node_id="root",
                tree_id="entity:alice",
                tree_type="entity",
                embedding=[1.0, 0.0],
                embedding_model_version="embed-v1",
                forest_version="v0",
            ),
            _baseline_doc(
                pk,
                None,
                node_id="removed-node",
                tree_id="entity:alice",
                tree_type="entity",
                embedding=[0.0, 1.0],
                embedding_model_version="embed-v1",
                forest_version="v0",
            )
        ],
    )
    recall = BaselineRecall(
        calls,
        [_baseline_doc(pk, None, tree_id="entity:alice", node_id="root", embedding_model_version="embed-v1", content_sha256="same-recall")],
    )
    summary_cache = SummaryCacheRecordingStore(calls, {"k": "summary"})
    handler = MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=FakeLeaseStore(),
            source_session_store=RecordingStore(calls, "source_session"),
            source_turn_store=RecordingStore(calls, "source_turns"),
            fact_store=RecordingStore(calls, "facts"),
            tree_node_store=tree_nodes,
            fact_location_index=locations,
            manifest_store=manifest_store,
            recall_index=recall,
            browse_embedding_store=browse_embeddings,
            summary_cache=summary_cache,
            snapshot_store=RecordingStore(calls, "snapshot"),
        ),
        ingest_core=FakeIngestCore(_unchanged_incremental_snapshot()),
        config=MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1", code_version="test-sha", prompt_config_hash="prompt-hash", lease_renewal_seconds=9999),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    metadata = manifest_store.latest.metadata
    assert metadata["projection_plan_counts"]["tree_node_membership"] == {"unchanged": 0, "upsert": 2, "delete": 0}
    assert metadata["projection_plan_counts"]["fact_location"] == {"unchanged": 0, "upsert": 1, "delete": 0}
    assert metadata["projection_plan_counts"]["recall_document"] == {"unchanged": 0, "upsert": 1, "delete": 0}
    assert metadata["projection_plan_counts"]["browse_embedding"] == {"unchanged": 1, "upsert": 0, "delete": 0}
    assert metadata["projection_plan_counts"]["summary_cache"] == {"unchanged": 1, "upsert": 0, "delete": 0}
    assert metadata["projection_plan_live_skipped_browse_embedding_count"] == 1
    assert metadata["projection_plan_live_skipped_summary_cache_count"] == 1
    assert "tree_nodes" in calls
    assert "locations" in calls
    assert "recall" in calls
    assert "browse_embeddings" not in calls
    assert "summary_cache" not in calls


@pytest.mark.asyncio
async def test_materialization_handler_uses_authoritative_committed_baseline_for_counts_and_safe_skips() -> None:
    calls = []
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
    baseline_provider = MatchingProjectionBaselineProvider()
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_unchanged_incremental_snapshot()),
        baseline_provider=baseline_provider,
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert len(baseline_provider.calls) == 1
    assert manifest_store.latest is not None
    metadata = manifest_store.latest.metadata
    assert metadata["projection_plan_counts"]["tree_node_membership"] == {"unchanged": 2, "upsert": 0, "delete": 0}
    assert metadata["projection_plan_counts"]["fact_location"] == {"unchanged": 1, "upsert": 0, "delete": 0}
    assert metadata["projection_plan_counts"]["recall_document"] == {"unchanged": 1, "upsert": 0, "delete": 0}
    assert metadata["projection_plan_counts"]["browse_embedding"] == {"unchanged": 1, "upsert": 0, "delete": 0}
    assert metadata["projection_plan_counts"]["summary_cache"] == {"unchanged": 1, "upsert": 0, "delete": 0}
    assert metadata["projection_plan_live_skipped_browse_embedding_count"] == 1
    assert metadata["projection_plan_live_skipped_summary_cache_count"] == 1
    assert metadata["dirty_path_plan"]["mode"] == "instrumentation_only"
    assert metadata["dirty_path_plan"]["skip_plans_present"] is True
    assert metadata["dirty_path_plan"]["id_arrays_truncated"] is False
    assert metadata["dirty_path_plan"]["affected_tree_count"] == 0
    assert metadata["dirty_path_plan"]["affected_fact_count"] == 0
    assert metadata["dirty_path_plan"]["dirty_node_count"] == 0
    assert metadata["projection_tree_scope_shadow"]["comparison_supported"] is True
    assert metadata["projection_tree_scope_shadow"]["divergent"] is False
    assert metadata["projection_tree_scope_shadow"]["by_kind"]["tree_node_membership"]["missing_unchanged"] == 2
    assert metadata["projection_tree_scope_shadow"]["by_kind"]["tree_node_membership"]["missing_upsert"] == 0
    assert "tree_nodes" in calls
    assert "locations" in calls
    assert "recall" in calls
    assert "browse_embeddings" not in calls
    assert "summary_cache" not in calls


@pytest.mark.asyncio
async def test_materialization_handler_applies_tree_scoped_writes_when_shadow_is_clean() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_two_tree_incremental_snapshot()),
        baseline_provider=MatchingProjectionBaselineProvider(),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_tree_scoped_projection_writes=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    metadata = manifest_store.latest.metadata
    assert metadata["projection_tree_scope_writes"]["enabled"] is True
    assert metadata["projection_tree_scope_writes"]["applied"] is True
    assert metadata["projection_tree_scope_writes"]["reason"] == "shadow_clean"
    assert metadata["projection_tree_scope_writes"]["scoped_projection_plan_counts"]["tree_node_membership"] == {
        "unchanged": 2,
        "upsert": 0,
        "delete": 0,
        "superseded": 0,
    }
    tree_write = next(item for item in handler._stores.tree_node_store.items if isinstance(item, dict))
    assert {tree_id for tree_id, _node_id in tree_write["unchanged_keys"]} == {"entity:alice"}
    assert len(tree_write["unchanged_keys"]) == 2
    location_write = next(item for item in handler._stores.fact_location_index.items if isinstance(item, dict))
    assert {key.split("/", 2)[1] for key in location_write["unchanged_keys"]} == {"entity:alice"}


@pytest.mark.asyncio
async def test_materialization_handler_tree_scoped_writes_fall_back_on_shadow_divergence() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_two_tree_incremental_snapshot()),
        baseline_provider=lambda **kwargs: CommittedProjectionBaseline(scope=kwargs["scope"], records={}),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_tree_scoped_projection_writes=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    metadata = manifest_store.latest.metadata
    assert metadata["projection_tree_scope_writes"]["enabled"] is True
    assert metadata["projection_tree_scope_writes"]["applied"] is False
    assert metadata["projection_tree_scope_writes"]["reason"] == "shadow_divergent"
    tree_write = next(item for item in handler._stores.tree_node_store.items if isinstance(item, dict))
    assert len(tree_write["upsert_nodes"]) == 4


@pytest.mark.asyncio
async def test_materialization_handler_partial_merge_without_baseline_does_not_delete_unloaded_committed_trees() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_snapshot()),
        baseline_provider=MatchingProjectionBaselineProvider(),
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_tree_scoped_projection_writes=True,
        ),
    )
    handler._ingest_core.snapshot = ForestMaterializationSnapshot(
        facts=handler._ingest_core.snapshot.facts,
        trees=handler._ingest_core.snapshot.trees,
        node_embeddings=handler._ingest_core.snapshot.node_embeddings,
        summary_cache=handler._ingest_core.snapshot.summary_cache,
        changed_tree_ids=None,
        deleted_tree_ids=frozenset(),
        baseline_forest_version=None,
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    metadata = manifest_store.latest.metadata
    assert metadata["projection_delta_baseline_forest_version"] is None
    assert metadata["projection_tree_scope_writes"]["applied"] is False
    assert metadata["projection_tree_scope_writes"]["reason"] == "full_projection_required"
    tree_write = next(item for item in handler._stores.tree_node_store.items if isinstance(item, dict))
    assert tree_write["delete_keys"] == []


@pytest.mark.asyncio
async def test_materialization_handler_tree_scoped_writes_preserve_scoped_deletes() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    baseline_provider = MatchingProjectionBaselineProvider()
    original_build = baseline_provider.__call__

    def _with_scoped_deletes(**kwargs):
        baseline = original_build(scope=kwargs["scope"], projection=kwargs["projection"])
        records = baseline.as_retained_committed_records()
        records["tree_node_membership"] = (
            *records["tree_node_membership"],
            ProjectionRecordRef("tree_node_membership", "entity:alice/removed", content_hash="removed-node", metadata={"tree_id": "entity:alice", "node_id": "removed"}),
            ProjectionRecordRef("tree_node_membership", "entity:old/root", content_hash="old-node", metadata={"tree_id": "entity:old", "node_id": "root"}),
        )
        records["fact_location"] = (
            *records["fact_location"],
            ProjectionRecordRef("fact_location", "old-fact/entity:alice/removed/entity/L0", content_hash="removed-location", metadata={"tree_id": "entity:alice", "node_id": "removed", "fact_id": "old-fact"}),
            ProjectionRecordRef("fact_location", "old-fact/entity:old/root/entity/L0", content_hash="old-location", metadata={"tree_id": "entity:old", "node_id": "root", "fact_id": "old-fact"}),
        )
        records["recall_document"] = (
            *records["recall_document"],
            ProjectionRecordRef("recall_document", "entity:alice/removed", content_hash="removed-recall", metadata={"tree_id": "entity:alice", "node_id": "removed"}),
            ProjectionRecordRef("recall_document", "entity:old/root", content_hash="old-recall", metadata={"tree_id": "entity:old", "node_id": "root"}),
        )
        return CommittedProjectionBaseline(scope=baseline.scope, records=records)

    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_two_tree_incremental_snapshot_with_deleted_tree()),
        baseline_provider=_with_scoped_deletes,
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_tree_scoped_projection_writes=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    metadata = manifest_store.latest.metadata
    assert metadata["projection_tree_scope_writes"]["applied"] is True
    tree_write = next(item for item in handler._stores.tree_node_store.items if isinstance(item, dict))
    assert ("entity:alice", "removed") in tree_write["delete_keys"]
    assert ("entity:old", "root") in tree_write["delete_keys"]
    location_write = next(item for item in handler._stores.fact_location_index.items if isinstance(item, dict))
    assert "old-fact/entity:alice/removed/entity/L0" in location_write["delete_keys"]
    assert "old-fact/entity:old/root/entity/L0" in location_write["delete_keys"]
    recall_write = next(item for item in handler._stores.recall_index.items if isinstance(item, dict))
    assert {ref.logical_id for ref in recall_write["close_refs"]} >= {"entity:alice/removed", "entity:old/root"}


@pytest.mark.asyncio
async def test_materialization_handler_tree_scoped_writes_fall_back_on_out_of_scope_delete() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    baseline_provider = MatchingProjectionBaselineProvider()
    original_build = baseline_provider.__call__

    def _with_out_of_scope_delete(**kwargs):
        baseline = original_build(scope=kwargs["scope"], projection=kwargs["projection"])
        records = baseline.as_retained_committed_records()
        records["tree_node_membership"] = (
            *records["tree_node_membership"],
            ProjectionRecordRef("tree_node_membership", "entity:omitted/removed", content_hash="removed", metadata={"tree_id": "entity:omitted", "node_id": "removed"}),
        )
        return CommittedProjectionBaseline(scope=baseline.scope, records=records)

    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_two_tree_incremental_snapshot()),
        baseline_provider=_with_out_of_scope_delete,
        config=MaterializationHandlerConfig(
            owner_id="worker-1",
            embedding_model_version="embed-v1",
            code_version="test-sha",
            prompt_config_hash="prompt-hash",
            lease_renewal_seconds=9999,
            enable_tree_scoped_projection_writes=True,
        ),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    metadata = manifest_store.latest.metadata
    assert metadata["projection_tree_scope_writes"]["applied"] is False
    assert metadata["projection_tree_scope_writes"]["reason"] == "scoped_write_divergent"
    assert metadata["projection_tree_scope_writes"]["by_kind"]["tree_node_membership"]["missing_delete"] == 1
    tree_write = next(item for item in handler._stores.tree_node_store.items if isinstance(item, dict))
    assert ("entity:omitted", "removed") in tree_write["delete_keys"]


@pytest.mark.asyncio
async def test_materialization_handler_shadow_compare_uses_embedding_model_version_for_scoped_browse() -> None:
    calls = []
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
    current_projection = project_forest_snapshot(
        _two_tree_incremental_snapshot(),
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v2",
        generation=2,
        embedding_model_version="embed-v1",
    )
    def _old_if_alice(ref: ProjectionRecordRef) -> ProjectionRecordRef:
        if str(ref.metadata.get("tree_id") or "") == "entity:alice":
            return replace(ref, content_hash=f"old-{ref.content_hash}", content_id="")
        return ref
    records = {
        kind: tuple(_old_if_alice(ref) for ref in plan.upsert)
        for kind, plan in current_projection.projection_skip_plans.items()
    }
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_two_tree_incremental_snapshot()),
        baseline_provider=lambda **kwargs: CommittedProjectionBaseline(scope=kwargs["scope"], records=records),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    shadow = manifest_store.latest.metadata["projection_tree_scope_shadow"]
    assert shadow["comparison_supported"] is True
    assert shadow["by_kind"]["browse_embedding"]["missing_upsert"] == 0
    assert shadow["by_kind"]["browse_embedding"]["extra_upsert"] == 0
    assert shadow["by_kind"]["tree_node_membership"]["missing_unchanged"] == 2
    assert shadow["by_kind"]["tree_node_membership"]["missing_upsert"] == 0


@pytest.mark.asyncio
async def test_materialization_handler_reuses_embedding_refs_for_unchanged_nodes() -> None:
    calls = []
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
    current_projection = project_forest_snapshot(
        _two_tree_incremental_snapshot(),
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v2",
        generation=2,
        embedding_model_version="embed-v1",
    )

    def _committed_ref(kind: str, ref: ProjectionRecordRef) -> ProjectionRecordRef:
        tree_id = str(ref.metadata.get("tree_id") or "")
        if tree_id == "entity:alice" and kind in {"tree_node_membership", "recall_document", "browse_embedding"}:
            return replace(ref, content_hash=f"old-{ref.content_hash}", content_id="")
        if tree_id == "entity:bob" and kind in {"recall_document", "browse_embedding"}:
            return replace(ref, content_hash=f"committed-{ref.content_hash}")
        return ref

    records = {
        kind: tuple(_committed_ref(kind, ref) for ref in plan.upsert)
        for kind, plan in current_projection.projection_skip_plans.items()
    }
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_two_tree_incremental_snapshot()),
        baseline_provider=lambda **kwargs: CommittedProjectionBaseline(scope=kwargs["scope"], records=records),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    metadata = manifest_store.latest.metadata
    assert metadata["projection_plan_counts"]["tree_node_membership"] == {"unchanged": 2, "upsert": 2, "delete": 0}
    assert metadata["projection_plan_counts"]["recall_document"] == {"unchanged": 1, "upsert": 1, "delete": 0}
    assert metadata["projection_plan_counts"]["browse_embedding"] == {"unchanged": 1, "upsert": 1, "delete": 0}
    shadow = metadata["projection_tree_scope_shadow"]
    assert shadow["comparison_supported"] is True
    assert shadow["divergent"] is False
    assert shadow["by_kind"]["recall_document"]["missing_upsert"] == 0
    assert shadow["by_kind"]["browse_embedding"]["missing_upsert"] == 0


@pytest.mark.asyncio
async def test_materialization_handler_reuses_recall_refs_from_default_baseline() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    pk = "tenant-1:user-1"
    current_projection = project_forest_snapshot(
        _two_tree_incremental_snapshot(),
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v2",
        generation=2,
        embedding_model_version="embed-v1",
    )
    records = {kind: tuple(plan.upsert) for kind, plan in current_projection.projection_skip_plans.items()}
    tree_nodes = BaselineRecordingStore(
        calls,
        "tree_nodes",
        [
            _baseline_doc(
                pk,
                "tree_node_membership",
                tree_id=str(ref.metadata["tree_id"]),
                node_id=str(ref.metadata["node_id"]),
                content_sha256=ref.content_hash,
                body_id=ref.content_id,
                valid_from_generation=1,
                valid_from_forest_version="v1",
                valid_to_generation=None,
                valid_to_forest_version=None,
            )
            for ref in records["tree_node_membership"]
        ],
    )
    locations = BaselineRecordingStore(
        calls,
        "locations",
        [
            _baseline_doc(
                pk,
                "fact_location",
                fact_id=str(ref.metadata["fact_id"]),
                tree_id=str(ref.metadata["tree_id"]),
                node_id=str(ref.metadata["node_id"]),
                tree_type="entity",
                level="L0",
                content_sha256=ref.content_hash,
                valid_from_generation=1,
                valid_from_forest_version="v1",
                valid_to_generation=None,
                valid_to_forest_version=None,
            )
            for ref in records["fact_location"]
            if str(ref.metadata.get("tree_id") or "") == "entity:bob"
        ],
    )
    recall = BaselineRecall(
        calls,
        [
            _baseline_doc(
                pk,
                None,
                tree_id=str(ref.metadata["tree_id"]),
                node_id=str(ref.metadata["node_id"]),
                embedding_model_version="embed-v1",
                content_sha256=f"committed-{ref.content_hash}",
                valid_from_generation=1,
                valid_from_forest_version="v1",
                valid_to_generation=None,
                valid_to_forest_version=None,
            )
            for ref in records["recall_document"]
            if str(ref.metadata.get("tree_id") or "") == "entity:bob"
        ],
    )
    browse = BaselineRecordingStore(
        calls,
        "browse_embeddings",
        [
            _baseline_doc(
                pk,
                None,
                tree_id=str(ref.metadata["tree_id"]),
                node_id=str(ref.metadata["node_id"]),
                tree_type="entity",
                embedding_model_version="embed-v1",
                embedding=[0.0, 1.0],
                embedding_sha256=f"committed-{ref.content_hash}",
            )
            for ref in records["browse_embedding"]
            if str(ref.metadata.get("tree_id") or "") == "entity:bob"
        ],
    )
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        node_store=tree_nodes,
        location_store=locations,
        recall_index=recall,
        browse_store=browse,
        core=FakeIngestCore(_two_tree_incremental_snapshot()),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    metadata = manifest_store.latest.metadata
    assert metadata["projection_plan_counts"]["recall_document"] == {"unchanged": 1, "upsert": 1, "delete": 0}
    assert metadata["projection_tree_scope_shadow"]["divergent"] is False


@pytest.mark.asyncio
async def test_materialization_handler_does_not_reuse_embeddings_across_model_versions() -> None:
    calls = []
    manifest_store = FakeManifestStore()
    manifest_store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata={"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"},
    )
    pk = "tenant-1:user-1"
    current_projection = project_forest_snapshot(
        _two_tree_incremental_snapshot(),
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v2",
        generation=2,
        embedding_model_version="embed-v1",
    )
    records = {kind: tuple(plan.upsert) for kind, plan in current_projection.projection_skip_plans.items()}
    tree_nodes = BaselineRecordingStore(
        calls,
        "tree_nodes",
        [
            _baseline_doc(
                pk,
                "tree_node_membership",
                tree_id=str(ref.metadata["tree_id"]),
                node_id=str(ref.metadata["node_id"]),
                content_sha256=ref.content_hash,
                body_id=ref.content_id,
                valid_from_generation=1,
                valid_from_forest_version="v1",
                valid_to_generation=None,
                valid_to_forest_version=None,
            )
            for ref in records["tree_node_membership"]
        ],
    )
    stale_recall = BaselineRecall(
        calls,
        [
            _baseline_doc(
                pk,
                None,
                tree_id=str(ref.metadata["tree_id"]),
                node_id=str(ref.metadata["node_id"]),
                embedding_model_version="old-embed",
                content_sha256=f"old-{ref.content_hash}",
                valid_from_generation=1,
                valid_from_forest_version="v1",
                valid_to_generation=None,
                valid_to_forest_version=None,
            )
            for ref in records["recall_document"]
        ],
    )
    stale_browse = BaselineRecordingStore(
        calls,
        "browse_embeddings",
        [
            _baseline_doc(
                pk,
                None,
                tree_id=str(ref.metadata["tree_id"]),
                node_id=str(ref.metadata["node_id"]),
                tree_type="entity",
                embedding_model_version="old-embed",
                embedding=[0.0, 1.0],
                embedding_sha256=f"old-{ref.content_hash}",
            )
            for ref in records["browse_embedding"]
        ],
    )
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        node_store=tree_nodes,
        location_store=BaselineRecordingStore(calls, "locations", []),
        recall_index=stale_recall,
        browse_store=stale_browse,
        core=FakeIngestCore(_two_tree_incremental_snapshot()),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    metadata = manifest_store.latest.metadata
    assert metadata["projection_plan_counts"]["recall_document"] == {"unchanged": 0, "upsert": 2, "delete": 0}
    assert metadata["projection_plan_counts"]["browse_embedding"] == {"unchanged": 0, "upsert": 2, "delete": 0}


@pytest.mark.asyncio
async def test_materialization_handler_baseline_failure_uses_full_structural_writes() -> None:
    calls = []
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
    tree_nodes = RecordingStore(calls, "tree_nodes")
    locations = RecordingStore(calls, "locations")
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        node_store=tree_nodes,
        location_store=locations,
        core=FakeIngestCore(_unchanged_incremental_snapshot()),
        baseline_provider=FailingProjectionBaselineProvider(),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert tree_nodes.items and all(not isinstance(item, dict) for item in tree_nodes.items)
    assert locations.items and all(not isinstance(item, dict) for item in locations.items)


@pytest.mark.asyncio
async def test_materialization_handler_actual_write_counts_show_incremental_reduction() -> None:
    full_calls = []
    full_tree_nodes = RecordingStore(full_calls, "tree_nodes")
    full_locations = RecordingStore(full_calls, "locations")
    full_recall = RecordingStore(full_calls, "recall")
    full_browse = RecordingStore(full_calls, "browse_embeddings")
    full_summary = SummaryCacheRecordingStore(full_calls, {})
    full_handler = MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=FakeLeaseStore(),
            source_session_store=RecordingStore(full_calls, "source_session"),
            source_turn_store=RecordingStore(full_calls, "source_turns"),
            fact_store=RecordingStore(full_calls, "facts"),
            tree_node_store=full_tree_nodes,
            fact_location_index=full_locations,
            manifest_store=FakeManifestStore(),
            recall_index=full_recall,
            browse_embedding_store=full_browse,
            summary_cache=full_summary,
            snapshot_store=RecordingStore(full_calls, "snapshot"),
        ),
        ingest_core=FakeIngestCore(_unchanged_incremental_snapshot()),
        config=MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1", code_version="test-sha", prompt_config_hash="prompt-hash", lease_renewal_seconds=9999),
    )

    await full_handler.handle(_envelope(), {"fencing_token": 1})
    full = _actual_projection_write_counts(full_tree_nodes, full_locations, full_recall, full_browse, full_summary)

    calls = []
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
    tree_nodes = RecordingStore(calls, "tree_nodes")
    locations = RecordingStore(calls, "locations")
    recall = RecordingStore(calls, "recall")
    browse = RecordingStore(calls, "browse_embeddings")
    summary = SummaryCacheRecordingStore(calls, {"k": "summary"})
    handler = MemForestMaterializationHandler(
        stores=MaterializationStores(
            lease_store=FakeLeaseStore(),
            source_session_store=RecordingStore(calls, "source_session"),
            source_turn_store=RecordingStore(calls, "source_turns"),
            fact_store=RecordingStore(calls, "facts"),
            tree_node_store=tree_nodes,
            fact_location_index=locations,
            manifest_store=manifest_store,
            recall_index=recall,
            browse_embedding_store=browse,
            summary_cache=summary,
            snapshot_store=RecordingStore(calls, "snapshot"),
            baseline_provider=MatchingProjectionBaselineProvider(),
        ),
        ingest_core=FakeIngestCore(_unchanged_incremental_snapshot()),
        config=MaterializationHandlerConfig(owner_id="worker-1", embedding_model_version="embed-v1", code_version="test-sha", prompt_config_hash="prompt-hash", lease_renewal_seconds=9999),
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    actual = _actual_projection_write_counts(tree_nodes, locations, recall, browse, summary)
    result = compare_projection_write_reduction(
        baseline_full=full,
        candidate_incremental=actual,
        min_total_reduction_ratio=0.5,
    )

    assert full == ProjectionWriteCounts(stored_nodes=2, fact_locations=1, recall_documents=1, browse_embeddings=1, summary_cache_entries=1)
    assert actual == ProjectionWriteCounts(stored_nodes=0, fact_locations=0, recall_documents=0, browse_embeddings=0, summary_cache_entries=0)
    assert result.ok


@pytest.mark.asyncio
async def test_materialization_handler_ignores_committed_baseline_when_snapshot_has_no_authoritative_baseline() -> None:
    calls = []
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
    baseline_provider = MatchingProjectionBaselineProvider()
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=FakeIngestCore(_snapshot()),
        baseline_provider=baseline_provider,
    )

    await handler.handle(_envelope(), {"fencing_token": 1})

    assert baseline_provider.calls == []
    assert manifest_store.latest is not None
    metadata = manifest_store.latest.metadata
    assert metadata["projection_plan_counts"]["browse_embedding"] == {"unchanged": 0, "upsert": 1, "delete": 0}
    assert metadata["projection_plan_counts"]["summary_cache"] == {"unchanged": 0, "upsert": 1, "delete": 0}
    assert metadata["projection_plan_live_skipped_browse_embedding_count"] == 0
    assert metadata["projection_plan_live_skipped_summary_cache_count"] == 0
    assert "browse_embeddings" in calls
    assert "summary_cache" in calls


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
