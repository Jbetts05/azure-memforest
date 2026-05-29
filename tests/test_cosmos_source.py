from __future__ import annotations

import pytest

from src.cloud.contracts import FactLocation, StoredTreeNode
from src.cloud.cosmos_source import CosmosFactLocationIndex, CosmosTreeNodeStore


class NotFoundError(Exception):
    status_code = 404


class FakeCosmosContainer:
    def __init__(self):
        self.items: dict[tuple[str, str], dict] = {}

    def upsert_item(self, item):
        self.items[(item["tenant_user_key"], item["id"])] = dict(item)

    def read_item(self, *, item, partition_key):
        key = (partition_key, item)
        if key not in self.items:
            raise NotFoundError()
        return dict(self.items[key])

    def delete_item(self, *, item, partition_key):
        self.items.pop((partition_key, item), None)

    def query_items(self, *, query, parameters, partition_key):
        params = {param["name"]: param["value"] for param in parameters}
        result = []
        for (pk, _id), item in self.items.items():
            if pk != partition_key:
                continue
            if "@tree_id" in params and item.get("tree_id") != params["@tree_id"]:
                continue
            if "@forest_version" in params and item.get("forest_version") != params["@forest_version"]:
                continue
            if "@fact_ids" in params and item.get("fact_id") not in params["@fact_ids"]:
                continue
            result.append({"id": item["id"]} if "SELECT c.id" in query else dict(item))
        return result


def _tree_node(node_id="node-1", tree_id="tree-1", *, parent_id=None, level="L1") -> StoredTreeNode:
    return StoredTreeNode(
        tree_id=tree_id,
        node_id=node_id,
        level=level,
        summary="summary",
        child_ids=("child-1",),
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        metadata={"parent_id": parent_id, "tree_type": "entity"},
    )


def _fact_location(fact_id="fact-1", tree_id="tree-1", node_id="node-1") -> FactLocation:
    return FactLocation(
        fact_id=fact_id,
        tree_id=tree_id,
        node_id=node_id,
        tree_type="entity",
        level="L0",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
    )


@pytest.mark.asyncio
async def test_cosmos_tree_node_store_upsert_get_and_tombstone() -> None:
    container = FakeCosmosContainer()
    store = CosmosTreeNodeStore(container, tenant_id="tenant-1", user_id="user-1", forest_version="v1")

    await store.upsert_nodes([_tree_node()])
    node = await store.get_node("tree-1", "node-1")
    await store.tombstone_tree("tree-1", reason="deleted")
    tombstoned = await store.get_node("tree-1", "node-1")

    assert node is not None
    assert node.summary == "summary"
    assert tombstoned is None


@pytest.mark.asyncio
async def test_cosmos_tree_node_store_lists_roots_and_tree_nodes() -> None:
    container = FakeCosmosContainer()
    store = CosmosTreeNodeStore(container, tenant_id="tenant-1", user_id="user-1", forest_version="v1")
    await store.upsert_nodes([
        _tree_node("root", parent_id=None),
        _tree_node("child", parent_id="root", level="L0"),
    ])

    roots = await store.list_root_nodes()
    nodes = await store.list_nodes_in_tree("tree-1")

    assert [node.node_id for node in roots] == ["root"]
    assert {node.node_id for node in nodes} == {"root", "child"}


@pytest.mark.asyncio
async def test_cosmos_tree_node_store_rejects_scope_mismatch() -> None:
    store = CosmosTreeNodeStore(FakeCosmosContainer(), tenant_id="tenant-1", user_id="user-1", forest_version="v1")

    with pytest.raises(ValueError, match="scope"):
        await store.upsert_nodes([
            StoredTreeNode(
                tree_id="tree-1",
                node_id="node-1",
                level="L1",
                summary="summary",
                tenant_id="tenant-2",
                user_id="user-1",
                forest_version="v1",
            )
        ])


@pytest.mark.asyncio
async def test_cosmos_fact_location_index_upsert_get_delete() -> None:
    container = FakeCosmosContainer()
    index = CosmosFactLocationIndex(container, tenant_id="tenant-1", user_id="user-1", forest_version="v1")

    await index.upsert_locations([
        _fact_location(),
        _fact_location(fact_id="fact-2", tree_id="tree-2", node_id="node-2"),
    ])
    locations = await index.get_locations(["fact-1", "missing"])
    await index.delete_locations_for_tree("tree-1")
    after_delete = await index.get_locations(["fact-1", "fact-2"])

    assert [loc.tree_id for loc in locations["fact-1"]] == ["tree-1"]
    assert locations["missing"] == []
    assert after_delete["fact-1"] == []
    assert [loc.tree_id for loc in after_delete["fact-2"]] == ["tree-2"]


@pytest.mark.asyncio
async def test_cosmos_fact_location_index_rejects_scope_mismatch() -> None:
    index = CosmosFactLocationIndex(FakeCosmosContainer(), tenant_id="tenant-1", user_id="user-1", forest_version="v1")

    with pytest.raises(ValueError, match="scope"):
        await index.upsert_locations([
            FactLocation(
                fact_id="fact-1",
                tree_id="tree-1",
                node_id="node-1",
                tree_type="entity",
                level="L0",
                tenant_id="tenant-1",
                user_id="user-2",
                forest_version="v1",
            )
        ])
