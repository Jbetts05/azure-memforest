from __future__ import annotations

from collections.abc import Callable

from src.cloud.contracts import FactLocation


def assert_fact_location_index_contract(factory: Callable[[], object]) -> None:
    index = factory()

    assert index.get_locations(["missing"]).get("missing", []) == []

    original = FactLocation(
        fact_id="fact-1",
        tree_id="tree-a",
        node_id="node-a",
        tree_type="entity",
        level="L0",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
    )
    replacement = FactLocation(
        fact_id="fact-1",
        tree_id="tree-a",
        node_id="node-a",
        tree_type="entity",
        level="L0",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        time_start=10.0,
    )
    other_tenant = FactLocation(
        fact_id="fact-1",
        tree_id="tree-a",
        node_id="node-a",
        tree_type="entity",
        level="L0",
        tenant_id="tenant-2",
        user_id="user-1",
        forest_version="v1",
    )
    other_tree = FactLocation(
        fact_id="fact-1",
        tree_id="tree-b",
        node_id="node-b",
        tree_type="scene",
        level="L0",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
    )

    index.upsert_locations([original, other_tree])
    index.upsert_locations([replacement, other_tenant])

    locations = index.get_locations(["fact-1"])["fact-1"]
    assert len(locations) == 3
    assert any(location.time_start == 10.0 for location in locations)
    assert {location.tenant_id for location in locations} == {"tenant-1", "tenant-2"}

    returned = index.get_locations(["fact-1"])["fact-1"]
    returned.clear()
    assert index.get_locations(["fact-1"])["fact-1"]

    index.delete_locations_for_tree("tree-a")
    remaining = index.get_locations(["fact-1"])["fact-1"]
    assert [location.tree_id for location in remaining] == ["tree-b"]


def assert_summary_cache_contract(factory: Callable[[], object]) -> None:
    cache = factory()

    assert cache.get("missing") is None
    cache.put("hash-1", "summary one", metadata={"model": "test"})
    assert cache.get("hash-1") == "summary one"

    cache.put("hash-1", "summary two")
    assert cache.get("hash-1") == "summary two"

    cache.delete("hash-1")
    assert cache.get("hash-1") is None

    cache.delete("hash-1")
    assert cache.get("hash-1") is None
