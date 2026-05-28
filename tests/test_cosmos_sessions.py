from __future__ import annotations

import pytest

from src.cloud.cosmos_sessions import CosmosSessionStore, CosmosTurnStore


class NotFoundError(Exception):
    status_code = 404


class FakeContainer:
    def __init__(self):
        self.items = {}

    def upsert_item(self, item):
        self.items[(item["tenant_user_key"], item["id"])] = dict(item)

    def read_item(self, *, item, partition_key):
        key = (partition_key, item)
        if key not in self.items:
            raise NotFoundError()
        return dict(self.items[key])

    def query_items(self, *, query, parameters, partition_key):
        params = {param["name"]: param["value"] for param in parameters}
        result = []
        for (pk, _id), item in self.items.items():
            if pk != partition_key:
                continue
            if "@session_id" in params and item.get("session_id") != params["@session_id"]:
                continue
            result.append(dict(item))
        return result


@pytest.mark.asyncio
async def test_cosmos_session_store_upsert_get_tombstone() -> None:
    store = CosmosSessionStore(FakeContainer(), tenant_id="tenant-1", user_id="user-1")

    await store.upsert_session("session-1", cell_ids=["cell-1"], metadata={"source": "test"})
    session = await store.get_session("session-1")
    await store.tombstone_session("session-1", reason="delete", scrub=True)
    deleted = await store.get_session("session-1")

    assert session is not None
    assert session.cell_ids == ("cell-1",)
    assert deleted is None


@pytest.mark.asyncio
async def test_cosmos_session_store_upsert_does_not_resurrect_tombstone() -> None:
    container = FakeContainer()
    store = CosmosSessionStore(container, tenant_id="tenant-1", user_id="user-1")
    await store.upsert_session("session-1", cell_ids=["cell-1"])
    await store.tombstone_session("session-1", reason="delete", scrub=True)

    await store.upsert_session("session-1", cell_ids=["cell-2"])

    stored = next(iter(container.items.values()))
    assert stored["tombstoned"] is True
    assert stored["cell_ids"] == []


@pytest.mark.asyncio
async def test_cosmos_turn_store_preserves_order_and_scrubs_on_tombstone() -> None:
    container = FakeContainer()
    store = CosmosTurnStore(container, tenant_id="tenant-1", user_id="user-1")

    await store.upsert_turns(
        "session-1",
        [
            {"role": "assistant", "content": "second", "content_id": "t2"},
            {"role": "user", "content": "first", "content_id": "t1"},
        ],
    )
    turns = await store.list_turns("session-1")
    await store.tombstone_session_turns("session-1", reason="delete")
    after_delete = await store.list_turns("session-1")

    assert [turn.content for turn in turns] == ["second", "first"]
    assert after_delete == []
    assert all(item["content"] == "" for item in container.items.values())


@pytest.mark.asyncio
async def test_cosmos_turn_store_tombstones_stale_reingest_tail() -> None:
    container = FakeContainer()
    store = CosmosTurnStore(container, tenant_id="tenant-1", user_id="user-1")
    await store.upsert_turns(
        "session-1",
        [
            {"role": "user", "content": "one", "content_id": "t1"},
            {"role": "user", "content": "two", "content_id": "t2"},
        ],
    )

    await store.upsert_turns("session-1", [{"role": "user", "content": "one updated", "content_id": "t1"}])

    turns = await store.list_turns("session-1")
    assert [turn.content for turn in turns] == ["one updated"]
    assert any(item.get("tombstone_reason") == "stale_reingest_tail" for item in container.items.values())


@pytest.mark.asyncio
async def test_cosmos_turn_store_allows_reordered_tail_id_in_same_reingest() -> None:
    container = FakeContainer()
    store = CosmosTurnStore(container, tenant_id="tenant-1", user_id="user-1")
    await store.upsert_turns(
        "session-1",
        [
            {"role": "user", "content": "one", "content_id": "t1"},
            {"role": "user", "content": "two", "content_id": "t2"},
            {"role": "user", "content": "three", "content_id": "t3"},
        ],
    )

    await store.upsert_turns(
        "session-1",
        [
            {"role": "user", "content": "three moved", "content_id": "t3"},
        ],
    )

    turns = await store.list_turns("session-1")
    assert [turn.content for turn in turns] == ["three moved"]


@pytest.mark.asyncio
async def test_cosmos_turn_store_rejects_large_turns() -> None:
    store = CosmosTurnStore(FakeContainer(), tenant_id="tenant-1", user_id="user-1")

    with pytest.raises(ValueError, match="size budget"):
        await store.upsert_turns("session-1", [{"role": "user", "content": "x" * (1024 * 1024 + 1)}])


@pytest.mark.asyncio
async def test_source_data_is_independent_of_forest_version() -> None:
    session_store = CosmosSessionStore(FakeContainer(), tenant_id="tenant-1", user_id="user-1")
    await session_store.upsert_session("session-1")

    session = await session_store.get_session("session-1")

    assert session is not None
    assert not hasattr(session, "forest_version")
