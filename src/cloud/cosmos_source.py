"""Async Cosmos source adapters for tree nodes and fact locations."""

from __future__ import annotations

import hashlib
import time
from typing import Any

from src.cloud.async_batch import run_bounded
from src.cloud.contracts import FactLocation, StoredTreeNode
from src.cloud.schema import SCHEMA_VERSION, partition_key


class CosmosTreeNodeStore:
    def __init__(self, container: Any, *, tenant_id: str, user_id: str, forest_version: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.forest_version = forest_version
        self.generation = _generation_from_forest_version(forest_version)
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def get_node(self, tree_id: str, node_id: str) -> StoredTreeNode | None:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id "
            "AND c.node_id=@node_id AND c.doc_kind=@doc_kind"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@node_id", "value": node_id},
            {"name": "@doc_kind", "value": "tree_node_membership"},
        ]
        matches = [
            item
            async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key))
            if self._membership_visible(item)
        ]
        if not matches:
            legacy = await self._get_legacy_node(tree_id, node_id)
            return legacy
        return await self._read_body(_latest_visible_doc(matches))

    async def get_nodes(self, tree_id: str, node_ids: list[str]) -> list[StoredTreeNode]:
        nodes = []
        for node_id in node_ids:
            node = await self.get_node(tree_id, node_id)
            if node is not None:
                nodes.append(node)
        return nodes

    async def list_root_nodes(self, *, limit: int = 100) -> list[StoredTreeNode]:
        query = "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.doc_kind=@doc_kind"
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@doc_kind", "value": "tree_node_membership"},
        ]
        roots: list[StoredTreeNode] = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not self._membership_visible(item):
                continue
            if _node_parent_id(item) in {None, ""}:
                node = await self._node_from_query_item(item)
                if node is None:
                    continue
                roots.append(node)
                if len(roots) >= limit:
                    break
        return roots

    async def list_tree_ids(self, *, limit: int = 10000) -> list[str]:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.doc_kind=@doc_kind"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@doc_kind", "value": "tree_node_membership"},
        ]
        tree_ids: list[str] = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not self._membership_visible(item):
                continue
            value = str(item.get("tree_id") or "").strip()
            if value and value not in tree_ids:
                tree_ids.append(value)
                if len(tree_ids) >= limit:
                    break
        return tree_ids

    async def list_nodes_in_tree(self, tree_id: str, *, limit: int = 1000) -> list[StoredTreeNode]:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id AND c.doc_kind=@doc_kind"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@doc_kind", "value": "tree_node_membership"},
        ]
        nodes: list[StoredTreeNode] = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not self._membership_visible(item):
                continue
            node = await self._node_from_query_item(item)
            if node is None:
                continue
            nodes.append(node)
            if len(nodes) >= limit:
                break
        return nodes

    async def upsert_nodes(self, nodes: list[StoredTreeNode]) -> None:
        for node in nodes:
            self._validate_node_scope(node)
        delete_keys = await self._full_projection_delete_keys(nodes)
        await self.upsert_nodes_delta(nodes, delete_keys=delete_keys)

    async def upsert_nodes_delta(
        self,
        nodes: list[StoredTreeNode],
        *,
        delete_keys: list[tuple[str, str]] | tuple[tuple[str, str], ...] = (),
        unchanged_keys: list[tuple[str, str]] | tuple[tuple[str, str], ...] = (),
    ) -> None:
        for node in nodes:
            self._validate_node_scope(node)

        async def _upsert(node: StoredTreeNode) -> None:
            await self._close_active_memberships(node.tree_id, node.node_id)
            body = _tree_node_body_doc(node, self.tenant_user_key)
            existing = await self._read_body_doc(body["id"])
            if not _same_tree_node_body(existing, body):
                await _maybe_await(self.container.upsert_item(body))
            await _maybe_await(self.container.upsert_item(_tree_node_membership_doc(node, self.tenant_user_key, body)))

        for tree_id, node_id in delete_keys:
            await self._close_active_memberships(str(tree_id), str(node_id))
        for tree_id, node_id in unchanged_keys:
            await self._repair_unchanged_memberships(str(tree_id), str(node_id))
        await self._close_same_generation_orphan_memberships()
        await run_bounded(nodes, _upsert)

    async def tombstone_tree(self, tree_id: str, *, reason: str) -> None:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id "
            "AND c.doc_kind=@doc_kind"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@doc_kind", "value": "tree_node_membership"},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not self._membership_visible(item):
                continue
            if "valid_from_generation" in item:
                item["valid_to_generation"] = self.generation
                item["valid_to_forest_version"] = self.forest_version
                item["close_reason"] = reason
            else:
                item["tombstoned"] = True
                item["tombstone_reason"] = reason
                item["tombstoned_at_unix"] = time.time()
            await _maybe_await(self.container.upsert_item(item))

    def _validate_node_scope(self, node: StoredTreeNode) -> None:
        if node.tenant_id != self.tenant_id or node.user_id != self.user_id or node.forest_version != self.forest_version:
            raise ValueError("StoredTreeNode scope does not match CosmosTreeNodeStore scope.")

    async def _node_from_query_item(self, item: dict[str, Any]) -> StoredTreeNode | None:
        if item.get("doc_kind") != "tree_node_membership":
            return _stored_tree_node_from_doc(item)
        return await self._read_body(item)

    async def _get_legacy_node(self, tree_id: str, node_id: str) -> StoredTreeNode | None:
        try:
            item = await _maybe_await(self.container.read_item(item=_tree_node_id(tree_id, node_id, self.forest_version), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id or item.get("forest_version") != self.forest_version:
            return None
        if "valid_from_generation" in item:
            return None
        if item.get("tombstoned"):
            return None
        if item.get("doc_kind") != "tree_node_membership":
            return _stored_tree_node_from_doc(item)
        return await self._read_body(item)

    async def _read_body(self, membership: dict[str, Any]) -> StoredTreeNode | None:
        body_id = str(membership.get("body_id") or "")
        if not body_id:
            return None
        try:
            body = await _maybe_await(self.container.read_item(item=body_id, partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if body.get("tenant_id") != self.tenant_id or body.get("user_id") != self.user_id or body.get("tombstoned"):
            return None
        node = _stored_tree_node_from_doc(body)
        return StoredTreeNode(
            tree_id=node.tree_id,
            node_id=node.node_id,
            level=node.level,
            summary=node.summary,
            child_ids=node.child_ids,
            fact_ids=node.fact_ids,
            tenant_id=node.tenant_id,
            user_id=node.user_id,
            forest_version=self.forest_version,
            schema_version=node.schema_version,
            metadata={**dict(membership.get("metadata") or {}), **dict(node.metadata)},
        )

    async def _read_body_doc(self, body_id: str) -> dict[str, Any] | None:
        try:
            body = await _maybe_await(self.container.read_item(item=body_id, partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if body.get("tenant_id") != self.tenant_id or body.get("user_id") != self.user_id or body.get("tombstoned"):
            return None
        return body

    def _membership_visible(self, item: dict[str, Any]) -> bool:
        return _doc_visible_for_version(item, tenant_id=self.tenant_id, user_id=self.user_id, forest_version=self.forest_version, generation=self.generation)

    async def _close_active_memberships(self, tree_id: str, node_id: str) -> None:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id "
            "AND c.node_id=@node_id AND c.doc_kind=@doc_kind"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@node_id", "value": node_id},
            {"name": "@doc_kind", "value": "tree_node_membership"},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not self._membership_visible(item) and not _doc_from_same_generation_other_version(item, generation=self.generation, forest_version=self.forest_version):
                continue
            item["valid_to_generation"] = self.generation
            item["valid_to_forest_version"] = self.forest_version
            item["updated_at_unix"] = time.time()
            await _maybe_await(self.container.upsert_item(item))

    async def _repair_unchanged_memberships(self, tree_id: str, node_id: str) -> None:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id "
            "AND c.node_id=@node_id AND c.doc_kind=@doc_kind"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@node_id", "value": node_id},
            {"name": "@doc_kind", "value": "tree_node_membership"},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if _doc_from_same_generation_other_version(item, generation=self.generation, forest_version=self.forest_version):
                item["valid_to_generation"] = self.generation
                item["valid_to_forest_version"] = self.forest_version
                item["updated_at_unix"] = time.time()
                await _maybe_await(self.container.upsert_item(item))
            elif _doc_closed_by_same_generation_other_version(item, generation=self.generation, forest_version=self.forest_version):
                item["valid_to_generation"] = None
                item["valid_to_forest_version"] = None
                item["updated_at_unix"] = time.time()
                await _maybe_await(self.container.upsert_item(item))

    async def _close_same_generation_orphan_memberships(self) -> None:
        query = "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.doc_kind=@doc_kind"
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@doc_kind", "value": "tree_node_membership"},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not _doc_from_same_generation_other_version(item, generation=self.generation, forest_version=self.forest_version):
                continue
            item["valid_to_generation"] = self.generation
            item["valid_to_forest_version"] = self.forest_version
            item["updated_at_unix"] = time.time()
            await _maybe_await(self.container.upsert_item(item))

    async def _full_projection_delete_keys(self, nodes: list[StoredTreeNode]) -> list[tuple[str, str]]:
        if not nodes:
            return []
        projected = {(node.tree_id, node.node_id) for node in nodes}
        tree_ids = {node.tree_id for node in nodes}
        delete_keys: list[tuple[str, str]] = []
        for tree_id in tree_ids:
            query = "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id AND c.doc_kind=@doc_kind"
            params = [
                {"name": "@pk", "value": self.tenant_user_key},
                {"name": "@tree_id", "value": tree_id},
                {"name": "@doc_kind", "value": "tree_node_membership"},
            ]
            async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
                key = (str(item.get("tree_id") or ""), str(item.get("node_id") or ""))
                if self._membership_visible(item) and key not in projected:
                    delete_keys.append(key)
        return delete_keys


class CosmosFactLocationIndex:
    def __init__(self, container: Any, *, tenant_id: str, user_id: str, forest_version: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.forest_version = forest_version
        self.generation = _generation_from_forest_version(forest_version)
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def get_locations(self, fact_ids: list[str]) -> dict[str, list[FactLocation]]:
        if not fact_ids:
            return {}
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND ARRAY_CONTAINS(@fact_ids, c.fact_id) "
            "AND c.doc_kind=@doc_kind"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@fact_ids", "value": fact_ids},
            {"name": "@doc_kind", "value": "fact_location"},
        ]
        out: dict[str, list[FactLocation]] = {fact_id: [] for fact_id in fact_ids}
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not self._location_visible(item):
                continue
            out.setdefault(item["fact_id"], []).append(_fact_location_from_doc(item))
        return {fact_id: list(locations) for fact_id, locations in out.items()}

    async def upsert_locations(self, locations: list[FactLocation]) -> None:
        for location in locations:
            self._validate_location_scope(location)
        delete_keys = await self._full_projection_delete_keys(locations)
        await self.upsert_locations_delta(locations, delete_keys=delete_keys)

    async def upsert_locations_delta(
        self,
        locations: list[FactLocation],
        *,
        delete_keys: list[str] | tuple[str, ...] = (),
        unchanged_keys: list[str] | tuple[str, ...] = (),
    ) -> None:
        for location in locations:
            self._validate_location_scope(location)

        async def _upsert(location: FactLocation) -> None:
            await self._close_active_location(_fact_location_logical_id(location))
            await _maybe_await(self.container.upsert_item(_fact_location_doc(location, self.tenant_user_key)))

        for logical_id in delete_keys:
            await self._close_active_location(str(logical_id))
        for logical_id in unchanged_keys:
            await self._repair_unchanged_location(str(logical_id))
        await self._close_same_generation_orphan_locations()
        await run_bounded(locations, _upsert)

    async def delete_locations_for_tree(self, tree_id: str) -> None:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id "
            "AND c.doc_kind=@doc_kind"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@doc_kind", "value": "fact_location"},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not self._location_visible(item) and not _doc_from_same_generation_other_version(item, generation=self.generation, forest_version=self.forest_version):
                continue
            item["valid_to_generation"] = self.generation
            item["valid_to_forest_version"] = self.forest_version
            item["updated_at_unix"] = time.time()
            await _maybe_await(self.container.upsert_item(item))

    def _validate_location_scope(self, location: FactLocation) -> None:
        if location.tenant_id != self.tenant_id or location.user_id != self.user_id or location.forest_version != self.forest_version:
            raise ValueError("FactLocation scope does not match CosmosFactLocationIndex scope.")

    def _location_visible(self, item: dict[str, Any]) -> bool:
        return _doc_visible_for_version(item, tenant_id=self.tenant_id, user_id=self.user_id, forest_version=self.forest_version, generation=self.generation)

    async def _close_active_location(self, logical_id: str) -> None:
        query = "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.logical_id=@logical_id AND c.doc_kind=@doc_kind"
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@logical_id", "value": logical_id},
            {"name": "@doc_kind", "value": "fact_location"},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not self._location_visible(item) and not _doc_from_same_generation_other_version(item, generation=self.generation, forest_version=self.forest_version):
                continue
            item["valid_to_generation"] = self.generation
            item["valid_to_forest_version"] = self.forest_version
            item["updated_at_unix"] = time.time()
            await _maybe_await(self.container.upsert_item(item))

    async def _repair_unchanged_location(self, logical_id: str) -> None:
        query = "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.logical_id=@logical_id AND c.doc_kind=@doc_kind"
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@logical_id", "value": logical_id},
            {"name": "@doc_kind", "value": "fact_location"},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if _doc_from_same_generation_other_version(item, generation=self.generation, forest_version=self.forest_version):
                item["valid_to_generation"] = self.generation
                item["valid_to_forest_version"] = self.forest_version
                item["updated_at_unix"] = time.time()
                await _maybe_await(self.container.upsert_item(item))
            elif _doc_closed_by_same_generation_other_version(item, generation=self.generation, forest_version=self.forest_version):
                item["valid_to_generation"] = None
                item["valid_to_forest_version"] = None
                item["updated_at_unix"] = time.time()
                await _maybe_await(self.container.upsert_item(item))

    async def _close_same_generation_orphan_locations(self) -> None:
        query = "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.doc_kind=@doc_kind"
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@doc_kind", "value": "fact_location"},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if not _doc_from_same_generation_other_version(item, generation=self.generation, forest_version=self.forest_version):
                continue
            item["valid_to_generation"] = self.generation
            item["valid_to_forest_version"] = self.forest_version
            item["updated_at_unix"] = time.time()
            await _maybe_await(self.container.upsert_item(item))

    async def _full_projection_delete_keys(self, locations: list[FactLocation]) -> list[str]:
        projected = {_fact_location_logical_id(location) for location in locations}
        delete_keys: list[str] = []
        query = "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.doc_kind=@doc_kind"
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@doc_kind", "value": "fact_location"},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            logical_id = str(item.get("logical_id") or _fact_location_logical_id_from_doc(item))
            if self._location_visible(item) and logical_id not in projected:
                delete_keys.append(logical_id)
        return delete_keys


def _tree_node_body_doc(node: StoredTreeNode, tenant_user_key: str) -> dict[str, Any]:
    content = _tree_node_content(node)
    content_hash = _stable_hash(content)
    return {
        "id": _tree_node_body_id(tenant_user_key, node.tree_id, node.node_id, content_hash),
        "doc_kind": "tree_node_body",
        "schema_version": node.schema_version,
        "tenant_id": node.tenant_id,
        "user_id": node.user_id,
        "tenant_user_key": tenant_user_key,
        "content_sha256": content_hash,
        **content,
        "tombstoned": False,
        "updated_at_unix": time.time(),
    }


def _tree_node_membership_doc(node: StoredTreeNode, tenant_user_key: str, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _tree_node_id(node.tree_id, node.node_id, node.forest_version or ""),
        "doc_kind": "tree_node_membership",
        "schema_version": node.schema_version,
        "tenant_id": node.tenant_id,
        "user_id": node.user_id,
        "tenant_user_key": tenant_user_key,
        "forest_version": node.forest_version,
        "logical_id": _tree_node_logical_id(node.tree_id, node.node_id),
        "valid_from_generation": _generation_from_forest_version(node.forest_version or ""),
        "valid_from_forest_version": node.forest_version,
        "valid_to_generation": None,
        "valid_to_forest_version": None,
        "tree_id": node.tree_id,
        "node_id": node.node_id,
        "body_id": body["id"],
        "content_sha256": body["content_sha256"],
        "metadata": {
            "parent_id": node.metadata.get("parent_id"),
            "tree_type": node.metadata.get("tree_type"),
            "generation": node.metadata.get("generation"),
        },
        "tombstoned": False,
        "updated_at_unix": time.time(),
    }


def _tree_node_content(node: StoredTreeNode) -> dict[str, Any]:
    metadata = {
        key: value
        for key, value in dict(node.metadata).items()
        if key not in {"generation", "forest_version"}
    }
    return {
        "level": node.level,
        "summary": node.summary,
        "child_ids": list(node.child_ids),
        "fact_ids": list(node.fact_ids),
        "tree_id": node.tree_id,
        "node_id": node.node_id,
        "metadata": metadata,
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
    content = _fact_location_content(location)
    return {
        "id": _fact_location_id(location),
        "doc_kind": "fact_location",
        "schema_version": location.schema_version,
        "tenant_id": location.tenant_id,
        "user_id": location.user_id,
        "tenant_user_key": tenant_user_key,
        "forest_version": location.forest_version,
        "logical_id": _fact_location_logical_id(location),
        "valid_from_generation": _generation_from_forest_version(location.forest_version or ""),
        "valid_from_forest_version": location.forest_version,
        "valid_to_generation": None,
        "valid_to_forest_version": None,
        "content_sha256": _stable_hash(content),
        **content,
        "updated_at_unix": time.time(),
    }


def _fact_location_content(location: FactLocation) -> dict[str, Any]:
    metadata = {
        key: value
        for key, value in dict(location.metadata).items()
        if key not in {"generation", "forest_version"}
    }
    return {
        "fact_id": location.fact_id,
        "tree_id": location.tree_id,
        "node_id": location.node_id,
        "tree_type": location.tree_type,
        "level": location.level,
        "time_start": location.time_start,
        "time_end": location.time_end,
        "metadata": metadata,
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


def _tree_node_body_id(tenant_user_key: str, tree_id: str, node_id: str, content_sha256: str) -> str:
    return _hash_id("tree_node_body", tenant_user_key, tree_id, node_id, content_sha256)


def _tree_node_logical_id(tree_id: str, node_id: str) -> str:
    return f"{tree_id}/{node_id}"


def _node_parent_id(item: dict[str, Any]) -> Any:
    return dict(item.get("metadata") or {}).get("parent_id")


def _same_tree_node_body(existing: dict[str, Any] | None, document: dict[str, Any]) -> bool:
    return existing is not None and existing.get("content_sha256") == document.get("content_sha256")


def _stable_hash(value: Any) -> str:
    payload = json_dumps(value)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


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


def _fact_location_logical_id(location: FactLocation) -> str:
    return "/".join((location.fact_id, location.tree_id, location.node_id, location.tree_type, location.level))


def _fact_location_logical_id_from_doc(item: dict[str, Any]) -> str:
    return "/".join(
        (
            str(item.get("fact_id") or ""),
            str(item.get("tree_id") or ""),
            str(item.get("node_id") or ""),
            str(item.get("tree_type") or ""),
            str(item.get("level") or ""),
        )
    )


def _doc_visible_for_version(item: dict[str, Any], *, tenant_id: str, user_id: str, forest_version: str, generation: int) -> bool:
    if item.get("tombstoned"):
        return False
    if item.get("tenant_id") != tenant_id or item.get("user_id") != user_id:
        return False
    if "valid_from_generation" not in item:
        return item.get("forest_version") == forest_version
    from_generation = int(item.get("valid_from_generation") or 0)
    from_version = str(item.get("valid_from_forest_version") or item.get("forest_version") or "")
    if from_generation > generation:
        return False
    if from_generation == generation and from_version != forest_version:
        return False
    to_generation_raw = item.get("valid_to_generation")
    if to_generation_raw is None:
        return True
    to_generation = int(to_generation_raw)
    to_version = str(item.get("valid_to_forest_version") or "")
    if to_generation > generation:
        return True
    if to_generation < generation:
        return False
    return to_version != forest_version


def _latest_visible_doc(items: list[dict[str, Any]]) -> dict[str, Any]:
    return max(
        items,
        key=lambda item: (
            int(item.get("valid_from_generation") or 0),
            str(item.get("valid_from_forest_version") or item.get("forest_version") or ""),
        ),
    )


def _doc_from_same_generation_other_version(item: dict[str, Any], *, generation: int, forest_version: str) -> bool:
    if generation <= 0:
        return False
    if "valid_from_generation" not in item:
        return False
    return int(item.get("valid_from_generation") or 0) == generation and str(item.get("valid_from_forest_version") or item.get("forest_version") or "") != forest_version


def _doc_closed_by_same_generation_other_version(item: dict[str, Any], *, generation: int, forest_version: str) -> bool:
    if generation <= 0:
        return False
    to_generation = item.get("valid_to_generation")
    if to_generation is None:
        return False
    return int(to_generation) == generation and str(item.get("valid_to_forest_version") or "") != forest_version


def _generation_from_forest_version(forest_version: str) -> int:
    value = str(forest_version or "")
    if value.startswith("g"):
        digits = []
        for char in value[1:]:
            if not char.isdigit():
                break
            digits.append(char)
        if digits:
            return int("".join(digits))
    return 0


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
