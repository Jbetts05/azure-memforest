from __future__ import annotations

import pytest

from src.cloud.cosmos_summary_cache import CosmosSummaryCache


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
            if "@ids" in params and item["id"] not in params["@ids"]:
                continue
            if item.get("tombstoned"):
                continue
            result.append(dict(item))
        return result


@pytest.mark.asyncio
async def test_cosmos_summary_cache_round_trips_entries() -> None:
    cache = CosmosSummaryCache(FakeContainer(), tenant_id="tenant-1", user_id="user-1")

    await cache.put("hash-1", "summary one", metadata={"tree_id": "tree-1"})

    assert await cache.get("hash-1") == "summary one"
    assert await cache.get_many(["missing", "hash-1"]) == {"hash-1": "summary one"}
    stored = next(iter(cache.container.items.values()))
    assert stored["content_hash"] == "hash-1"
    assert stored["summary_sha256"]
    assert stored["metadata"] == {"tree_id": "tree-1"}


@pytest.mark.asyncio
async def test_cosmos_summary_cache_tombstone_hides_entry() -> None:
    cache = CosmosSummaryCache(FakeContainer(), tenant_id="tenant-1", user_id="user-1")
    await cache.put("hash-1", "summary one")

    await cache.delete("hash-1")

    assert await cache.get("hash-1") is None
    assert await cache.get_many(["hash-1"]) == {}
