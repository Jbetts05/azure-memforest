from __future__ import annotations

import pytest

from src.cloud.contracts import StoredTreeNode
from src.forest.memforest import _content_key
from src.worker.cloud_hydration import (
    load_fact_ids_from_tree_store,
    load_cloud_hydration_prerequisites,
    load_facts_for_memtrees,
    load_memtrees_from_tree_store,
    load_summary_cache_for_memtrees,
)


class FakeTreeStore:
    def __init__(self, nodes_by_tree):
        self.nodes_by_tree = nodes_by_tree

    async def list_tree_ids(self, *, limit=10000):
        return list(self.nodes_by_tree)

    async def list_nodes_in_tree(self, tree_id, *, limit=100000):
        return list(self.nodes_by_tree.get(tree_id, []))[:limit]


class FakeFactStore:
    def __init__(self, facts_by_id):
        self.facts_by_id = facts_by_id
        self.requested = []

    async def get_facts(self, fact_ids):
        self.requested.append(list(fact_ids))
        return [self.facts_by_id[fact_id] for fact_id in reversed(fact_ids) if fact_id in self.facts_by_id]


class FakeSummaryCache:
    def __init__(self, summaries):
        self.summaries = summaries
        self.requested = []

    async def get(self, content_hash):
        self.requested.append(content_hash)
        return self.summaries.get(content_hash)


def _node(node_id, *, parent_id=None, level="L1", child_ids=(), fact_ids=(), tree_id="tree-1"):
    return StoredTreeNode(
        tree_id=tree_id,
        node_id=node_id,
        level=level,
        summary=f"summary {node_id}",
        child_ids=tuple(child_ids),
        fact_ids=tuple(fact_ids),
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        metadata={
            "parent_id": parent_id,
            "tree_type": "entity",
            "tree_key": "entity:alice",
            "tree_label": "Alice",
            "time_start": 1.0,
            "time_end": 2.0,
            "item_count": 1,
            "generation": 3,
        },
    )


@pytest.mark.asyncio
async def test_cloud_hydration_loads_memtrees_from_tree_store() -> None:
    store = FakeTreeStore(
        {
            "tree-1": [
                _node("root", child_ids=("leaf",)),
                _node("leaf", parent_id="root", level="L0", child_ids=("fact-1",), fact_ids=("fact-1",)),
            ]
        }
    )

    trees = await load_memtrees_from_tree_store(store)

    assert len(trees) == 1
    tree = trees[0]
    assert tree.tree_id == "tree-1"
    assert tree.tree_type == "entity"
    assert tree.tree_key == "entity:alice"
    assert tree.label == "Alice"
    assert tree.root_node_id == "root"
    assert set(tree.nodes) == {"root", "leaf"}
    assert tree.fact_ids_ordered == ["fact-1"]
    assert tree.version == 3


@pytest.mark.asyncio
async def test_cloud_hydration_uses_leaf_fact_ids_when_child_ids_absent() -> None:
    store = FakeTreeStore(
        {
            "tree-1": [
                _node("root", child_ids=("leaf",)),
                _node("leaf", parent_id="root", level="L0", fact_ids=("fact-1",)),
            ]
        }
    )

    tree = (await load_memtrees_from_tree_store(store))[0]

    assert tree.nodes["leaf"].child_ids == ["fact-1"]
    assert tree.fact_ids_ordered == ["fact-1"]


@pytest.mark.asyncio
async def test_cloud_hydration_rejects_ambiguous_roots() -> None:
    store = FakeTreeStore({"tree-1": [_node("root-a"), _node("root-b")]})

    with pytest.raises(ValueError, match="exactly one root"):
        await load_memtrees_from_tree_store(store)


@pytest.mark.asyncio
async def test_cloud_hydration_loads_referenced_facts_in_tree_order() -> None:
    trees = await load_memtrees_from_tree_store(
        FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf-a", "leaf-b")),
                    _node("leaf-a", parent_id="root", level="L0", child_ids=("fact-1",)),
                    _node("leaf-b", parent_id="root", level="L0", child_ids=("fact-2",)),
                ],
                "tree-2": [
                    _node("root-2", tree_id="tree-2", child_ids=("leaf-c",)),
                    _node("leaf-c", tree_id="tree-2", parent_id="root-2", level="L0", child_ids=("fact-1", "fact-3")),
                ],
            }
        )
    )
    fact_store = FakeFactStore(
        {
            "fact-1": {"fact_id": "fact-1", "fact_text": "one"},
            "fact-2": {"fact_id": "fact-2", "fact_text": "two"},
        }
    )

    facts, missing = await load_facts_for_memtrees(fact_store, trees)

    assert fact_store.requested == [["fact-1", "fact-2", "fact-3"]]
    assert [fact["fact_id"] for fact in facts] == ["fact-1", "fact-2"]
    assert missing == ("fact-3",)


@pytest.mark.asyncio
async def test_cloud_hydration_fact_ids_from_tree_store_prefer_fact_ids_over_cell_payloads() -> None:
    fact_ids = await load_fact_ids_from_tree_store(
        FakeTreeStore(
            {
                "session:one": [
                    _node("root", child_ids=("cell-leaf",)),
                    _node(
                        "cell-leaf",
                        parent_id="root",
                        level="L0",
                        child_ids=("cell-1",),
                        fact_ids=("fact-1", "fact-2"),
                    ),
                ]
            }
        )
    )

    assert fact_ids == ["fact-1", "fact-2"]


@pytest.mark.asyncio
async def test_cloud_hydration_loads_summary_cache_for_internal_node_hashes() -> None:
    trees = await load_memtrees_from_tree_store(
        FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("internal",)),
                    _node("internal", parent_id="root", child_ids=("leaf-a", "leaf-b")),
                    _node("leaf-a", parent_id="internal", level="L0", child_ids=("fact-1",)),
                    _node("leaf-b", parent_id="internal", level="L0", child_ids=("fact-2",)),
                ]
            }
        )
    )
    content_hash = _content_key(frozenset({"fact-1", "fact-2"}))
    cache = FakeSummaryCache({content_hash: "cached summary"})

    summaries, missing = await load_summary_cache_for_memtrees(cache, trees)

    assert summaries == {content_hash: "cached summary"}
    assert missing == ()
    assert cache.requested == [content_hash]


@pytest.mark.asyncio
async def test_cloud_hydration_summary_cache_handles_multi_level_tree() -> None:
    trees = await load_memtrees_from_tree_store(
        FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("internal-a", "internal-b")),
                    _node("internal-a", parent_id="root", child_ids=("leaf-a", "leaf-b")),
                    _node("internal-b", parent_id="root", child_ids=("leaf-c",)),
                    _node("leaf-a", parent_id="internal-a", level="L0", child_ids=("fact-1",)),
                    _node("leaf-b", parent_id="internal-a", level="L0", child_ids=("fact-2",)),
                    _node("leaf-c", parent_id="internal-b", level="L0", child_ids=("fact-3",)),
                ]
            }
        )
    )
    internal_a_hash = _content_key(frozenset({"fact-1", "fact-2"}))
    internal_b_hash = _content_key(frozenset({"fact-3"}))
    root_hash = _content_key(frozenset({"fact-1", "fact-2", "fact-3"}))
    cache = FakeSummaryCache(
        {
            internal_a_hash: "A",
            internal_b_hash: "B",
            root_hash: "root",
        }
    )

    summaries, missing = await load_summary_cache_for_memtrees(cache, trees)

    assert summaries == {
        internal_a_hash: "A",
        internal_b_hash: "B",
        root_hash: "root",
    }
    assert missing == ()


@pytest.mark.asyncio
async def test_cloud_hydration_prerequisites_reports_missing_state_without_mutating_hot_path() -> None:
    state = await load_cloud_hydration_prerequisites(
        tree_store=FakeTreeStore(
            {
                "tree-1": [
                    _node("root", child_ids=("leaf",)),
                    _node("leaf", parent_id="root", level="L0", child_ids=("fact-1",)),
                ]
            }
        ),
        fact_store=FakeFactStore({}),
        summary_cache=FakeSummaryCache({}),
    )

    assert len(state.trees) == 1
    assert state.facts == []
    assert state.summary_cache == {}
    assert state.missing_fact_ids == ("fact-1",)
    assert state.missing_summary_cache_keys == (_content_key(frozenset({"fact-1"})),)
