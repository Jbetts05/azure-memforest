from __future__ import annotations

import asyncio

import pytest

from src.cloud.cosmos_browse_embeddings import BrowseEmbeddingRecord, CosmosBrowseEmbeddingStore


class NotFoundError(Exception):
    status_code = 404


class FakeContainer:
    def __init__(self):
        self.items = {}
        self.upsert_count = 0
        self.read_count = 0

    def upsert_item(self, item):
        self.upsert_count += 1
        self.items[(item["tenant_user_key"], item["id"])] = dict(item)
        return item

    def read_item(self, *, item, partition_key):
        self.read_count += 1
        key = (partition_key, item)
        if key not in self.items:
            raise NotFoundError()
        return dict(self.items[key])

    def query_items(self, *, query, parameters, partition_key):
        params = {param["name"]: param["value"] for param in parameters}
        return [
            dict(item)
            for (pk, _), item in self.items.items()
            if pk == partition_key
            and item.get("tree_id") == params.get("@tree_id")
            and item.get("embedding_model_version") == params.get("@embedding_model_version")
        ]


class AsyncTrackingContainer(FakeContainer):
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


def _store(container=None, *, forest_version="v1", model_version="embed-v1"):
    return CosmosBrowseEmbeddingStore(
        container or FakeContainer(),
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version=forest_version,
        embedding_model_version=model_version,
    )


def _store_for_user(container, user_id: str, *, forest_version="v1", model_version="embed-v1"):
    return CosmosBrowseEmbeddingStore(
        container,
        tenant_id="tenant-1",
        user_id=user_id,
        forest_version=forest_version,
        embedding_model_version=model_version,
    )


@pytest.mark.asyncio
async def test_browse_embedding_store_round_trips_scoped_embeddings() -> None:
    store = _store()

    await store.upsert_records(
        [BrowseEmbeddingRecord(node_id="node-1", tree_id="tree-1", tree_type="entity", embedding=(0.1, 0.2))],
        model_version="embed-v1",
    )

    assert await store.get_embedding("node-1") == [0.1, 0.2]
    assert await store.get_embeddings(["node-1", "missing"]) == {"node-1": [0.1, 0.2]}


@pytest.mark.asyncio
async def test_browse_embedding_store_reuses_unchanged_embedding_across_forest_versions() -> None:
    container = FakeContainer()
    writer = _store(container, forest_version="v1")
    reader = _store(container, forest_version="v2")
    await writer.upsert_records(
        [BrowseEmbeddingRecord(node_id="node-1", tree_id="tree-1", tree_type="entity", embedding=(0.1,))],
        model_version="embed-v1",
    )

    assert await reader.get_embedding("node-1") == [0.1]


@pytest.mark.asyncio
async def test_browse_embedding_store_namespaces_ids_by_tenant_user() -> None:
    container = FakeContainer()
    first = _store_for_user(container, "user-1")
    second = _store_for_user(container, "user-2")
    record = BrowseEmbeddingRecord(node_id="node-1", tree_id="tree-1", tree_type="entity", embedding=(0.1,))

    await first.upsert_records([record], model_version="embed-v1")
    await second.upsert_records([record], model_version="embed-v1")

    assert len(container.items) == 2
    assert len({item["id"] for item in container.items.values()}) == 2
    assert await first.get_embedding("node-1") == [0.1]
    assert await second.get_embedding("node-1") == [0.1]


@pytest.mark.asyncio
async def test_browse_embedding_store_upserts_with_bounded_overlap() -> None:
    container = AsyncTrackingContainer()
    store = _store(container)

    await store.upsert_records(
        [
            BrowseEmbeddingRecord(node_id=f"node-{index}", tree_id="tree-1", tree_type="entity", embedding=(0.1,))
            for index in range(4)
        ],
        model_version="embed-v1",
    )

    assert len(container.items) == 4
    assert container.max_active > 1


@pytest.mark.asyncio
async def test_browse_embedding_store_overwrites_logical_row_for_new_version() -> None:
    container = FakeContainer()
    first = _store(container, forest_version="v1")
    second = _store(container, forest_version="v2")
    await first.upsert_records(
        [BrowseEmbeddingRecord(node_id="node-1", tree_id="tree-1", tree_type="entity", embedding=(0.1,))],
        model_version="embed-v1",
    )
    await second.upsert_records(
        [BrowseEmbeddingRecord(node_id="node-1", tree_id="tree-1", tree_type="entity", embedding=(0.2,))],
        model_version="embed-v1",
    )

    assert len(container.items) == 1
    assert await first.get_embedding("node-1") == [0.2]
    assert await second.get_embedding("node-1") == [0.2]


@pytest.mark.asyncio
async def test_browse_embedding_store_skips_identical_logical_row_for_new_version() -> None:
    container = FakeContainer()
    first = _store(container, forest_version="v1")
    second = _store(container, forest_version="v2")
    await first.upsert_records(
        [BrowseEmbeddingRecord(node_id="node-1", tree_id="tree-1", tree_type="entity", embedding=(0.1,))],
        model_version="embed-v1",
    )
    await second.upsert_records(
        [BrowseEmbeddingRecord(node_id="node-1", tree_id="tree-1", tree_type="entity", embedding=(0.1,))],
        model_version="embed-v1",
    )

    assert container.upsert_count == 1
    assert await second.get_embedding("node-1") == [0.1]


@pytest.mark.asyncio
async def test_browse_embedding_store_tombstones_tree() -> None:
    store = _store()
    await store.upsert_records(
        [BrowseEmbeddingRecord(node_id="node-1", tree_id="tree-1", tree_type="entity", embedding=(0.1,))],
        model_version="embed-v1",
    )

    await store.tombstone_tree("tree-1", reason="delete")

    assert await store.get_embedding("node-1") is None
