"""Async Cosmos source adapters for tree nodes and fact locations."""

from __future__ import annotations

import hashlib
import time
from typing import Any

from src.cloud.contracts import FactLocation, StoredTreeNode
from src.cloud.schema import SCHEMA_VERSION, partition_key


class CosmosTreeNodeStore:
    def __init__(self, container: Any, *, tenant_id: str, user_id: str, forest_version: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.forest_version = forest_version
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def get_node(self, tree_id: str, node_id: str) -> StoredTreeNode | None:
        try:
            item = await _maybe_await(self.container.read_item(item=_tree_node_id(tree_id, node_id, self.forest_version), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id or item.get("forest_version") != self.forest_version:
            return None
        if item.get("tombstoned"):
            return None
        return _stored_tree_node_from_doc(item)

    async def get_nodes(self, tree_id: str, node_ids: list[str]) -> list[StoredTreeNode]:
        nodes = []
        for node_id in node_ids:
            node = await self.get_node(tree_id, node_id)
            if node is not None:
                nodes.append(node)
        return nodes

    async def list_root_nodes(self, *, limit: int = 100) -> list[StoredTreeNode]:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.forest_version=@forest_version"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@forest_version", "value": self.forest_version},
        ]
        roots: list[StoredTreeNode] = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if item.get("tombstoned"):
                continue
            metadata = dict(item.get("metadata", {}))
            if metadata.get("parent_id") in {None, ""}:
                roots.append(_stored_tree_node_from_doc(item))
                if len(roots) >= limit:
                    break
        return roots

    async def list_nodes_in_tree(self, tree_id: str, *, limit: int = 1000) -> list[StoredTreeNode]:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id "
            "AND c.forest_version=@forest_version"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@forest_version", "value": self.forest_version},
        ]
        nodes: list[StoredTreeNode] = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if item.get("tombstoned"):
                continue
            nodes.append(_stored_tree_node_from_doc(item))
            if len(nodes) >= limit:
                break
        return nodes

    async def upsert_nodes(self, nodes: list[StoredTreeNode]) -> None:
        for node in nodes:
            self._validate_node_scope(node)
            await _maybe_await(self.container.upsert_item(_tree_node_doc(node, self.tenant_user_key)))

    async def tombstone_tree(self, tree_id: str, *, reason: str) -> None:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id "
            "AND c.forest_version=@forest_version"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@forest_version", "value": self.forest_version},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            item["tombstoned"] = True
            item["tombstone_reason"] = reason
            item["tombstoned_at_unix"] = time.time()
            await _maybe_await(self.container.upsert_item(item))

    def _validate_node_scope(self, node: StoredTreeNode) -> None:
        if node.tenant_id != self.tenant_id or node.user_id != self.user_id or node.forest_version != self.forest_version:
            raise ValueError("StoredTreeNode scope does not match CosmosTreeNodeStore scope.")


class CosmosFactLocationIndex:
    def __init__(self, container: Any, *, tenant_id: str, user_id: str, forest_version: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.forest_version = forest_version
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def get_locations(self, fact_ids: list[str]) -> dict[str, list[FactLocation]]:
        if not fact_ids:
            return {}
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND ARRAY_CONTAINS(@fact_ids, c.fact_id) "
            "AND c.forest_version=@forest_version"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@fact_ids", "value": fact_ids},
            {"name": "@forest_version", "value": self.forest_version},
        ]
        out: dict[str, list[FactLocation]] = {fact_id: [] for fact_id in fact_ids}
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if item.get("tombstoned"):
                continue
            out.setdefault(item["fact_id"], []).append(_fact_location_from_doc(item))
        return {fact_id: list(locations) for fact_id, locations in out.items()}

    async def upsert_locations(self, locations: list[FactLocation]) -> None:
        for location in locations:
            self._validate_location_scope(location)
            await _maybe_await(self.container.upsert_item(_fact_location_doc(location, self.tenant_user_key)))

    async def delete_locations_for_tree(self, tree_id: str) -> None:
        query = (
            "SELECT c.id FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id "
            "AND c.forest_version=@forest_version"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@forest_version", "value": self.forest_version},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            await _maybe_await(self.container.delete_item(item=item["id"], partition_key=self.tenant_user_key))

    def _validate_location_scope(self, location: FactLocation) -> None:
        if location.tenant_id != self.tenant_id or location.user_id != self.user_id or location.forest_version != self.forest_version:
            raise ValueError("FactLocation scope does not match CosmosFactLocationIndex scope.")


def _tree_node_doc(node: StoredTreeNode, tenant_user_key: str) -> dict[str, Any]:
    return {
        "id": _tree_node_id(node.tree_id, node.node_id, node.forest_version or ""),
        "schema_version": node.schema_version,
        "tenant_id": node.tenant_id,
        "user_id": node.user_id,
        "tenant_user_key": tenant_user_key,
        "forest_version": node.forest_version,
        "tree_id": node.tree_id,
        "node_id": node.node_id,
        "level": node.level,
        "summary": node.summary,
        "child_ids": list(node.child_ids),
        "fact_ids": list(node.fact_ids),
        "metadata": dict(node.metadata),
        "tombstoned": False,
        "updated_at_unix": time.time(),
    }


def _stored_tree_node_from_doc(item: dict[str, Any]) -> StoredTreeNode:
    return StoredTreeNode(
        tree_id=item["tree_id"],
        node_id=item["node_id"],
        level=item["level"],
        summary=item.get("summary", ""),
        child_ids=tuple(item.get("child_ids", ())),
        fact_ids=tuple(item.get("fact_ids", ())),
        tenant_id=item.get("tenant_id"),
        user_id=item.get("user_id"),
        forest_version=item.get("forest_version"),
        schema_version=int(item.get("schema_version", SCHEMA_VERSION)),
        metadata=dict(item.get("metadata", {})),
    )


def _fact_location_doc(location: FactLocation, tenant_user_key: str) -> dict[str, Any]:
    return {
        "id": _fact_location_id(location),
        "schema_version": location.schema_version,
        "tenant_id": location.tenant_id,
        "user_id": location.user_id,
        "tenant_user_key": tenant_user_key,
        "forest_version": location.forest_version,
        "fact_id": location.fact_id,
        "tree_id": location.tree_id,
        "node_id": location.node_id,
        "tree_type": location.tree_type,
        "level": location.level,
        "time_start": location.time_start,
        "time_end": location.time_end,
        "metadata": dict(location.metadata),
        "updated_at_unix": time.time(),
    }


def _fact_location_from_doc(item: dict[str, Any]) -> FactLocation:
    return FactLocation(
        fact_id=item["fact_id"],
        tree_id=item["tree_id"],
        node_id=item["node_id"],
        tree_type=item["tree_type"],
        level=item["level"],
        tenant_id=item.get("tenant_id"),
        user_id=item.get("user_id"),
        forest_version=item.get("forest_version"),
        schema_version=int(item.get("schema_version", SCHEMA_VERSION)),
        time_start=item.get("time_start"),
        time_end=item.get("time_end"),
        metadata=dict(item.get("metadata", {})),
    )


def _tree_node_id(tree_id: str, node_id: str, forest_version: str) -> str:
    return _hash_id("tree_node", tree_id, node_id, forest_version)


def _fact_location_id(location: FactLocation) -> str:
    return _hash_id(
        "fact_location",
        location.fact_id,
        location.tree_id,
        location.node_id,
        location.tree_type,
        location.level,
        location.forest_version or "",
    )


def _hash_id(prefix: str, *parts: str) -> str:
    raw = "\x1f".join(parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{prefix}_{digest}"


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


async def _aiter(values):
    if hasattr(values, "__aiter__"):
        async for item in values:
            yield item
    else:
        for item in values:
            yield item


def _is_not_found(exc: Exception) -> bool:
    if exc.__class__.__name__ == "CosmosResourceNotFoundError":
        return True
    return getattr(exc, "status_code", None) == 404
