from __future__ import annotations

import pytest

from src.cloud.contracts import StoredTreeNode
from src.worker.cloud_hydration import load_memtrees_from_tree_store


class FakeTreeStore:
    def __init__(self, nodes_by_tree):
        self.nodes_by_tree = nodes_by_tree

    async def list_tree_ids(self, *, limit=10000):
        return list(self.nodes_by_tree)

    async def list_nodes_in_tree(self, tree_id, *, limit=100000):
        return list(self.nodes_by_tree.get(tree_id, []))[:limit]


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
async def test_cloud_hydration_rejects_ambiguous_roots() -> None:
    store = FakeTreeStore({"tree-1": [_node("root-a"), _node("root-b")]})

    with pytest.raises(ValueError, match="exactly one root"):
        await load_memtrees_from_tree_store(store)

