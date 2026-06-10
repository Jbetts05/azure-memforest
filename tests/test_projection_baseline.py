from __future__ import annotations

from dataclasses import dataclass

import pytest

from src.cloud.projection_baseline import (
    ProjectionBaselineScope,
    baseline_from_committed_docs,
    build_committed_projection_baseline,
)
from src.cloud.projection_plan import ProjectionRecordKey, ProjectionRecordRef, plan_projection_skip, tree_node_body_content_id


@dataclass(frozen=True)
class FakeManifest:
    tenant_id: str = "tenant-1"
    user_id: str = "user-1"
    forest_version: str = "v1"
    status: str = "committed"


class FakeContainer:
    def __init__(self, docs):
        self.docs = list(docs)
        self.upsert_count = 0
        self.delete_count = 0

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
            result.append(doc)
        return result

    def upsert_item(self, item):
        self.upsert_count += 1

    def delete_item(self, *, item, partition_key):
        self.delete_count += 1


class FakeSearchClient:
    def __init__(self, docs):
        self.docs = list(docs)
        self.calls = []

    def search(self, **kwargs):
        self.calls.append(kwargs)
        return list(self.docs)


def test_projection_baseline_scope_requires_committed_manifest() -> None:
    scope = ProjectionBaselineScope.from_manifest(FakeManifest(), embedding_model_version="embed-v1")

    assert scope.forest_version == "v1"
    assert scope.embedding_model_version == "embed-v1"
    with pytest.raises(ValueError, match="committed manifest"):
        ProjectionBaselineScope.from_manifest(FakeManifest(status="pending"))


def test_baseline_from_committed_docs_preserves_forest_version_isolation() -> None:
    scope = ProjectionBaselineScope.from_manifest(FakeManifest(), embedding_model_version="embed-v1")
    body_id = tree_node_body_content_id(tree_id="tree-1", node_id="node-1", content_sha256="node-hash")

    baseline = baseline_from_committed_docs(
        scope,
        tree_node_docs=[
            _doc("tree_node_membership", tree_id="tree-1", node_id="node-1", body_id=body_id, content_sha256="node-hash"),
            _doc("tree_node_body", tree_id="tree-1", node_id="node-1", content_sha256="node-hash"),
            _doc("tree_node_membership", tree_id="tree-2", node_id="old", content_sha256="old", forest_version="v0"),
            _doc("tree_node_membership", tree_id="tree-3", node_id="gone", content_sha256="gone", tombstoned=True),
        ],
        fact_location_docs=[
            _doc(
                "fact_location",
                fact_id="fact-1",
                tree_id="tree-1",
                node_id="node-1",
                tree_type="entity",
                level="L0",
                content_sha256="fact-hash",
            )
        ],
        recall_document_docs=[
            _doc(None, tree_id="tree-1", node_id="node-1", content_sha256="recall-hash", embedding_model_version="embed-v1"),
            _doc(None, tree_id="tree-1", node_id="other-model", content_sha256="skip", embedding_model_version="embed-v0"),
        ],
        browse_embedding_docs=[
            _doc(None, node_id="node-1", tree_id="tree-1", embedding_sha256="embedding-hash", embedding_model_version="embed-v1"),
            _doc(None, node_id="node-old-version", tree_id="tree-1", embedding_sha256="old-version-hash", embedding_model_version="embed-v1", forest_version="v0"),
            _doc(None, node_id="node-2", tree_id="tree-1", embedding_sha256="skip", embedding_model_version="embed-v0"),
        ],
    )

    assert baseline.forest_version == "v1"
    assert baseline.records_for("tree_node_membership") == (
        ProjectionRecordRef(
            "tree_node_membership",
            "tree-1/node-1",
            content_hash="node-hash",
            content_id=body_id,
            metadata={"tree_id": "tree-1", "node_id": "node-1", "forest_version": "v1"},
        ),
    )
    assert baseline.records_for("fact_location")[0].logical_id == "fact-1/tree-1/node-1/entity/L0"
    assert baseline.records_for("recall_document")[0].content_hash == "recall-hash"
    assert {record.logical_id for record in baseline.records_for("browse_embedding")} == {
        "node-1/embed-v1",
        "node-old-version/embed-v1",
    }


def test_committed_baseline_can_be_compared_without_live_write_skip_side_effects() -> None:
    scope = ProjectionBaselineScope.from_manifest(FakeManifest())
    baseline = baseline_from_committed_docs(
        scope,
        fact_location_docs=[
            _doc(
                "fact_location",
                fact_id="fact-1",
                tree_id="tree-1",
                node_id="node-1",
                tree_type="entity",
                level="L0",
                content_sha256="same",
            ),
            _doc(
                "fact_location",
                fact_id="removed",
                tree_id="tree-1",
                node_id="node-2",
                tree_type="entity",
                level="L0",
                content_sha256="old",
            ),
        ],
    )

    plan = plan_projection_skip(
        projected_records=[
            ProjectionRecordRef("fact_location", "fact-1/tree-1/node-1/entity/L0", content_hash="same"),
            ProjectionRecordRef("fact_location", "fact-2/tree-1/node-3/entity/L0", content_hash="new"),
        ],
        retained_committed_records=baseline.records_for("fact_location"),
    )

    assert plan.unchanged_keys == frozenset({ProjectionRecordKey("fact_location", "fact-1/tree-1/node-1/entity/L0")})
    assert plan.upsert_keys == frozenset({ProjectionRecordKey("fact_location", "fact-2/tree-1/node-3/entity/L0")})
    assert plan.delete_keys == frozenset({ProjectionRecordKey("fact_location", "removed/tree-1/node-2/entity/L0")})


def test_baseline_rejects_duplicate_committed_record_keys() -> None:
    scope = ProjectionBaselineScope.from_manifest(FakeManifest())

    with pytest.raises(ValueError, match="duplicate record key"):
        baseline_from_committed_docs(
            scope,
            recall_document_docs=[
                _doc(None, tree_id="tree-1", node_id="root", content_sha256="a"),
                _doc(None, tree_id="tree-1", node_id="root", content_sha256="b"),
            ],
        )


@pytest.mark.asyncio
async def test_build_committed_projection_baseline_reads_only_fake_stores() -> None:
    scope = ProjectionBaselineScope.from_manifest(FakeManifest(), embedding_model_version="embed-v1")
    tree_nodes = FakeContainer([_doc("tree_node_membership", tree_id="tree-1", node_id="node-1", content_sha256="node-hash")])
    fact_locations = FakeContainer(
        [_doc("fact_location", fact_id="fact-1", tree_id="tree-1", node_id="node-1", tree_type="entity", level="L0", content_sha256="fact-hash")]
    )
    browse_embeddings = FakeContainer(
        [_doc(None, node_id="node-1", tree_id="tree-1", embedding_model_version="embed-v1", embedding_sha256="embedding-hash")]
    )
    recall = FakeSearchClient([_doc(None, tree_id="tree-1", node_id="node-1", embedding_model_version="embed-v1", content_sha256="recall-hash")])

    baseline = await build_committed_projection_baseline(
        scope,
        tree_nodes_container=tree_nodes,
        fact_locations_container=fact_locations,
        recall_search_client=recall,
        browse_embeddings_container=browse_embeddings,
    )

    assert {kind: len(records) for kind, records in baseline.records.items()} == {
        "tree_node_membership": 1,
        "fact_location": 1,
        "recall_document": 1,
        "browse_embedding": 1,
    }
    assert tree_nodes.upsert_count == fact_locations.upsert_count == browse_embeddings.upsert_count == 0
    assert tree_nodes.delete_count == fact_locations.delete_count == browse_embeddings.delete_count == 0


@pytest.mark.asyncio
async def test_build_committed_projection_baseline_reads_browse_embeddings_across_forest_versions() -> None:
    scope = ProjectionBaselineScope.from_manifest(FakeManifest(), embedding_model_version="embed-v1")
    browse_embeddings = FakeContainer(
        [
            _doc(None, node_id="node-1", tree_id="tree-1", embedding_model_version="embed-v1", embedding_sha256="current", forest_version="v1"),
            _doc(None, node_id="node-2", tree_id="tree-1", embedding_model_version="embed-v1", embedding_sha256="older", forest_version="v0"),
            _doc(None, node_id="node-3", tree_id="tree-1", embedding_model_version="embed-v0", embedding_sha256="wrong-model", forest_version="v0"),
        ]
    )

    baseline = await build_committed_projection_baseline(
        scope,
        browse_embeddings_container=browse_embeddings,
    )

    assert {record.logical_id for record in baseline.records_for("browse_embedding")} == {
        "node-1/embed-v1",
        "node-2/embed-v1",
    }


def _doc(doc_kind, **overrides):
    doc = {
        "tenant_id": "tenant-1",
        "user_id": "user-1",
        "forest_version": "v1",
        "tombstoned": False,
    }
    if doc_kind is not None:
        doc["doc_kind"] = doc_kind
    doc.update(overrides)
    return doc
