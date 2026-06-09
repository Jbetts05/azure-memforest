from __future__ import annotations

import asyncio

import pytest

from src.cloud.contracts import FactLocation, StoredTreeNode
from src.cloud.cosmos_source import CosmosFactLocationIndex, CosmosTreeNodeStore


class NotFoundError(Exception):
    status_code = 404


class FakeCosmosContainer:
    def __init__(self):
        self.items: dict[tuple[str, str], dict] = {}
        self.upsert_count = 0
        self.read_count = 0

    def upsert_item(self, item):
        self.upsert_count += 1
        self.items[(item["tenant_user_key"], item["id"])] = dict(item)

    def read_item(self, *, item, partition_key):
        self.read_count += 1
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


class AsyncTrackingContainer(FakeCosmosContainer):
    def __init__(self):
        super().__init__()
        self.active = 0
        self.max_active = 0

    async def upsert_item(self, item):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.01)
        try:
            return super().upsert_item(item)
        finally:
            self.active -= 1


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


def _tree_node_for_version(version: str, node_id="node-1", tree_id="tree-1", *, summary="summary", parent_id=None, level="L1") -> StoredTreeNode:
    node = _tree_node(node_id=node_id, tree_id=tree_id, parent_id=parent_id, level=level)
    return StoredTreeNode(
        tree_id=node.tree_id,
        node_id=node.node_id,
        level=node.level,
        summary=summary,
        child_ids=node.child_ids,
        fact_ids=node.fact_ids,
        tenant_id=node.tenant_id,
        user_id=node.user_id,
        forest_version=version,
        schema_version=node.schema_version,
        metadata=node.metadata,
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
async def test_cosmos_tree_node_store_upserts_with_bounded_overlap() -> None:
    container = AsyncTrackingContainer()
    store = CosmosTreeNodeStore(container, tenant_id="tenant-1", user_id="user-1", forest_version="v1")

    await store.upsert_nodes([_tree_node(f"node-{index}") for index in range(4)])

    assert len(container.items) == 8
    assert container.max_active > 1


@pytest.mark.asyncio
async def test_cosmos_tree_node_store_reuses_stable_body_across_versions() -> None:
    container = FakeCosmosContainer()
    first = CosmosTreeNodeStore(container, tenant_id="tenant-1", user_id="user-1", forest_version="v1")
    second = CosmosTreeNodeStore(container, tenant_id="tenant-1", user_id="user-1", forest_version="v2")
    await first.upsert_nodes([_tree_node_for_version("v1")])
    await second.upsert_nodes([_tree_node_for_version("v2")])

    assert len(container.items) == 3  # one stable body, two version memberships
    assert container.upsert_count == 3
    assert (await second.get_node("tree-1", "node-1")).summary == "summary"


@pytest.mark.asyncio
async def test_cosmos_tree_node_store_overwrites_stable_body_when_content_changes() -> None:
    container = FakeCosmosContainer()
    first = CosmosTreeNodeStore(container, tenant_id="tenant-1", user_id="user-1", forest_version="v1")
    second = CosmosTreeNodeStore(container, tenant_id="tenant-1", user_id="user-1", forest_version="v2")
    await first.upsert_nodes([_tree_node_for_version("v1", summary="old")])
    await second.upsert_nodes([_tree_node_for_version("v2", summary="new")])

    assert len(container.items) == 4
    assert container.upsert_count == 4
    assert (await first.get_node("tree-1", "node-1")).summary == "old"
    assert (await second.get_node("tree-1", "node-1")).summary == "new"


@pytest.mark.asyncio
async def test_cosmos_tree_node_store_keeps_removed_nodes_out_of_new_version() -> None:
    container = FakeCosmosContainer()
    first = CosmosTreeNodeStore(container, tenant_id="tenant-1", user_id="user-1", forest_version="v1")
    second = CosmosTreeNodeStore(container, tenant_id="tenant-1", user_id="user-1", forest_version="v2")
    await first.upsert_nodes([_tree_node_for_version("v1", "root"), _tree_node_for_version("v1", "removed")])
    await second.upsert_nodes([_tree_node_for_version("v2", "root")])

    assert {node.node_id for node in await first.list_nodes_in_tree("tree-1")} == {"root", "removed"}
    assert {node.node_id for node in await second.list_nodes_in_tree("tree-1")} == {"root"}


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
async def test_cosmos_fact_location_index_upserts_with_bounded_overlap() -> None:
    container = AsyncTrackingContainer()
    index = CosmosFactLocationIndex(container, tenant_id="tenant-1", user_id="user-1", forest_version="v1")

    await index.upsert_locations([_fact_location(f"fact-{item}") for item in range(4)])

    assert len(container.items) == 4
    assert container.max_active > 1


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
