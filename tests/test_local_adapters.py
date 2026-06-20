from __future__ import annotations

import pytest

from src.build.node_index import NodeEntry, NodeIndex
from src.build.tree_store import TreeStore
from src.build.tree_types import MemTree, MemTreeNode
from src.cloud import (
    FactLocation,
    InMemoryFactLocationIndex,
    InMemorySummaryCache,
    NodeIndexBrowseEmbeddingAdapter,
    NodeIndexRecallAdapter,
    TreeStoreNodeAdapter,
)


def test_node_index_recall_adapter_returns_candidates_with_scores(tmp_path):
    node_index = NodeIndex(index_dir=tmp_path, vector_dim=2)
    node_index.add_node(NodeEntry(node_id="node-a", tree_id="tree-a", level="root"), [1.0, 0.0])
    node_index.add_node(NodeEntry(node_id="node-b", tree_id="tree-b", level="root"), [0.0, 1.0])
    adapter = NodeIndexRecallAdapter(node_index)

    candidates = adapter.search([1.0, 0.0], top_k=2)

    assert [candidate.tree_id for candidate in candidates] == ["tree-a", "tree-b"]
    assert candidates[0].score == pytest.approx(1.0)
    assert candidates[1].score == pytest.approx(0.0)
    assert candidates[0].metadata["level"] == "root"


def test_node_index_recall_adapter_rejects_unsupported_filters(tmp_path):
    adapter = NodeIndexRecallAdapter(NodeIndex(index_dir=tmp_path, vector_dim=2))

    with pytest.raises(ValueError, match="Unsupported local recall filters"):
        adapter.search([1.0, 0.0], top_k=1, filters={"tenant_id": "tenant-a"})


def test_node_index_recall_adapter_filters_before_truncating(tmp_path):
    node_index = NodeIndex(index_dir=tmp_path, vector_dim=2)
    node_index.add_node(NodeEntry(node_id="top", tree_id="tree-top", level="root"), [1.0, 0.0])
    node_index.add_node(NodeEntry(node_id="middle", tree_id="tree-middle", level="root"), [0.8, 0.2])
    node_index.add_node(NodeEntry(node_id="filtered", tree_id="tree-filtered", level="root"), [0.0, 1.0])
    adapter = NodeIndexRecallAdapter(node_index)

    candidates = adapter.search(
        [1.0, 0.0],
        top_k=1,
        filters={"tree_ids": ["tree-filtered"]},
    )

    assert [candidate.tree_id for candidate in candidates] == ["tree-filtered"]


def test_node_index_recall_adapter_delete_by_tree_removes_search_and_browse_entries(tmp_path):
    node_index = NodeIndex(index_dir=tmp_path, vector_dim=2)
    node_index.add_node(NodeEntry(node_id="node-a", tree_id="tree-a", level="root"), [1.0, 0.0])
    adapter = NodeIndexRecallAdapter(node_index)

    adapter.delete_by_tree("tree-a")

    assert adapter.search([1.0, 0.0], top_k=1) == []
    assert node_index.get_embedding("node-a") is None


def test_browse_embedding_adapter_returns_available_embeddings_only(tmp_path):
    node_index = NodeIndex(index_dir=tmp_path, vector_dim=2)
    node_index.add_node(
        NodeEntry(node_id="node-a", tree_id="tree-a", level="L1"),
        [3.0, 4.0],
        searchable=False,
    )
    adapter = NodeIndexBrowseEmbeddingAdapter(node_index)

    embeddings = adapter.get_embeddings(["node-a", "missing-node"])

    assert list(embeddings) == ["node-a"]
    assert embeddings["node-a"] == pytest.approx([0.6, 0.8])


def test_tree_store_node_adapter_converts_loaded_tree_node(tmp_path):
    tree_store = TreeStore(tmp_path)
    tree = MemTree(
        tree_id="entity:user",
        tree_type="entity",
        tree_key="user",
        label="user",
        k=2,
        root_node_id="root",
        nodes={
            "root": MemTreeNode(
                node_id="root",
                tree_id="entity:user",
                level=1,
                time_start=1.0,
                time_end=2.0,
                summary="root summary",
                summary_dirty=False,
                child_ids=["leaf"],
                item_count=1,
            ),
            "leaf": MemTreeNode(
                node_id="leaf",
                tree_id="entity:user",
                level=0,
                time_start=1.0,
                time_end=1.0,
                summary="leaf summary",
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
    tree_store.register(tree)
    adapter = TreeStoreNodeAdapter(tree_store)

    root = adapter.get_node("entity:user", "root")
    leaf = adapter.get_node("entity:user", "leaf")

    assert root is not None
    assert root.child_ids == ("leaf",)
    assert root.level == "L1"
    assert root.metadata["tree_type"] == "entity"
    assert root.metadata["tree_key"] == "user"
    assert root.metadata["k"] == 3
    assert leaf is not None
    assert leaf.fact_ids == ("fact-1",)


def test_in_memory_fact_location_index_deletes_tree_locations_symmetrically():
    index = InMemoryFactLocationIndex()
    index.upsert_locations([
        FactLocation(fact_id="fact-1", tree_id="tree-a", node_id="node-a", tree_type="entity", level="L0"),
        FactLocation(fact_id="fact-1", tree_id="tree-b", node_id="node-b", tree_type="scene", level="L0"),
    ])

    index.delete_locations_for_tree("tree-a")

    locations = index.get_locations(["fact-1"])["fact-1"]
    assert [location.tree_id for location in locations] == ["tree-b"]


def test_in_memory_summary_cache_round_trips_and_deletes():
    cache = InMemorySummaryCache()

    cache.put("hash-1", "summary", metadata={"model": "local"})
    assert cache.get("hash-1") == "summary"

    cache.delete("hash-1")
    assert cache.get("hash-1") is None
