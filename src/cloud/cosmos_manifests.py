"""Cosmos manifest and deletion log adapters."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Literal

from src.cloud.schema import SCHEMA_VERSION, partition_key

ManifestStatus = Literal["pending", "committed", "failed"]
DeletionStatus = Literal["tombstoned", "purged"]


class ManifestNotFound(Exception):
    pass


class ManifestStateError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class ForestManifest:
    manifest_id: str
    tenant_id: str
    user_id: str
    forest_version: str
    generation: int
    status: ManifestStatus
    created_at_unix: float
    committed_at_unix: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class DeletionRecord:
    deletion_id: str
    tenant_id: str
    user_id: str
    deletion_type: str
    target_id: str
    reason: str
    created_at_unix: float
    status: DeletionStatus = "tombstoned"
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION


class CosmosManifestStore:
    def __init__(self, container: Any, *, tenant_id: str, user_id: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def create_pending(self, *, forest_version: str, generation: int, metadata: dict[str, Any] | None = None) -> ForestManifest:
        manifest = ForestManifest(
            manifest_id=_manifest_id(self.tenant_id, self.user_id, generation, forest_version),
            tenant_id=self.tenant_id,
            user_id=self.user_id,
            forest_version=forest_version,
            generation=generation,
            status="pending",
            created_at_unix=time.time(),
            metadata=dict(metadata or {}),
        )
        document = _manifest_to_doc(manifest, self.tenant_user_key)
        try:
            await _maybe_await(self.container.create_item(document, partition_key=self.tenant_id))
        except Exception as exc:
            if _is_conflict(exc):
                return await self.get_manifest(manifest.manifest_id)
            raise
        return manifest

    async def get_manifest(self, manifest_id: str) -> ForestManifest:
        try:
            item = await _maybe_await(self.container.read_item(item=manifest_id, partition_key=self.tenant_id))
        except Exception as exc:
            if _is_not_found(exc):
                raise ManifestNotFound() from exc
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id:
            raise ManifestNotFound()
        return _manifest_from_doc(item)

    async def commit_manifest(self, manifest_id: str) -> ForestManifest:
        item = await self._read_manifest_item(manifest_id)
        if item["status"] != "pending":
            raise ManifestStateError(f"Cannot commit manifest in state {item['status']!r}.")
        etag = item.get("_etag")
        item["status"] = "committed"
        item["committed_at_unix"] = time.time()
        try:
            await _conditional_replace(self.container, item, partition_key=self.tenant_id, etag=etag)
        except Exception as exc:
            if _is_precondition_failed(exc):
                raise ManifestStateError("Manifest changed before commit.") from exc
            raise
        return _manifest_from_doc(item)

    async def latest_committed(self) -> ForestManifest | None:
        query = (
            "SELECT * FROM c WHERE c.tenant_id=@tenant_id AND c.user_id=@user_id "
            "AND c.status='committed' ORDER BY c.generation DESC"
        )
        params = [
            {"name": "@tenant_id", "value": self.tenant_id},
            {"name": "@user_id", "value": self.user_id},
        ]
        matches = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_id)):
            if item.get("tenant_id") == self.tenant_id and item.get("user_id") == self.user_id:
                matches.append(_manifest_from_doc(item))
        return matches[0] if matches else None

    async def _read_manifest_item(self, manifest_id: str) -> dict[str, Any]:
        try:
            item = await _maybe_await(self.container.read_item(item=manifest_id, partition_key=self.tenant_id))
        except Exception as exc:
            if _is_not_found(exc):
                raise ManifestNotFound() from exc
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id:
            raise ManifestNotFound()
        return item


class CosmosDeletionLog:
    def __init__(self, container: Any, *, tenant_id: str) -> None:
        self.container = container
        self.tenant_id = tenant_id

    async def record_deletion(
        self,
        *,
        user_id: str,
        deletion_type: str,
        target_id: str,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> DeletionRecord:
        record = DeletionRecord(
            deletion_id=_deletion_id(self.tenant_id, user_id, deletion_type, target_id),
            tenant_id=self.tenant_id,
            user_id=user_id,
            deletion_type=deletion_type,
            target_id=target_id,
            reason=reason,
            created_at_unix=time.time(),
            metadata=dict(metadata or {}),
        )
        try:
            await _maybe_await(self.container.create_item(_deletion_to_doc(record), partition_key=self.tenant_id))
        except Exception as exc:
            if _is_conflict(exc):
                return await self.get_deletion(record.deletion_id)
            raise
        return record

    async def get_deletion(self, deletion_id: str) -> DeletionRecord:
        try:
            item = await _maybe_await(self.container.read_item(item=deletion_id, partition_key=self.tenant_id))
        except Exception as exc:
            if _is_not_found(exc):
                raise
            raise
        if item.get("tenant_id") != self.tenant_id:
            raise KeyError(deletion_id)
        return _deletion_from_doc(item)

    async def list_deletions(self, *, user_id: str) -> list[DeletionRecord]:
        query = "SELECT * FROM c WHERE c.tenant_id=@tenant_id AND c.user_id=@user_id"
        params = [
            {"name": "@tenant_id", "value": self.tenant_id},
            {"name": "@user_id", "value": user_id},
        ]
        records = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_id)):
            records.append(_deletion_from_doc(item))
        return records


def _manifest_to_doc(manifest: ForestManifest, tenant_user_key: str) -> dict[str, Any]:
    return {
        "id": manifest.manifest_id,
        "schema_version": manifest.schema_version,
        "tenant_id": manifest.tenant_id,
        "user_id": manifest.user_id,
        "tenant_user_key": tenant_user_key,
        "forest_version": manifest.forest_version,
        "generation": manifest.generation,
        "status": manifest.status,
        "created_at_unix": manifest.created_at_unix,
        "committed_at_unix": manifest.committed_at_unix,
        "metadata": dict(manifest.metadata),
    }


def _manifest_from_doc(item: dict[str, Any]) -> ForestManifest:
    return ForestManifest(
        manifest_id=item["id"],
        tenant_id=item["tenant_id"],
        user_id=item["user_id"],
        forest_version=item["forest_version"],
        generation=int(item["generation"]),
        status=item["status"],
        created_at_unix=float(item["created_at_unix"]),
        committed_at_unix=item.get("committed_at_unix"),
        metadata=dict(item.get("metadata", {})),
        schema_version=int(item.get("schema_version", SCHEMA_VERSION)),
    )


def _deletion_to_doc(record: DeletionRecord) -> dict[str, Any]:
    return {
        "id": record.deletion_id,
        "schema_version": record.schema_version,
        "tenant_id": record.tenant_id,
        "user_id": record.user_id,
        "tenant_user_key": partition_key(tenant_id=record.tenant_id, user_id=record.user_id),
        "deletion_id": record.deletion_id,
        "deletion_type": record.deletion_type,
        "target_id": record.target_id,
        "reason": record.reason,
        "created_at_unix": record.created_at_unix,
        "status": record.status,
        "metadata": dict(record.metadata),
    }


def _deletion_from_doc(item: dict[str, Any]) -> DeletionRecord:
    return DeletionRecord(
        deletion_id=item["deletion_id"],
        tenant_id=item["tenant_id"],
        user_id=item["user_id"],
        deletion_type=item["deletion_type"],
        target_id=item["target_id"],
        reason=item["reason"],
        created_at_unix=float(item["created_at_unix"]),
        status=item.get("status", "tombstoned"),
        metadata=dict(item.get("metadata", {})),
        schema_version=int(item.get("schema_version", SCHEMA_VERSION)),
    )


def _manifest_id(tenant_id: str, user_id: str, generation: int, forest_version: str) -> str:
    return "manifest_" + _hash(tenant_id, user_id, str(generation), forest_version)


def _deletion_id(tenant_id: str, user_id: str, deletion_type: str, target_id: str) -> str:
    return "deletion_" + _hash(tenant_id, user_id, deletion_type, target_id)


def _hash(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


async def _conditional_replace(container: Any, item: dict[str, Any], *, partition_key: str, etag: str | None):
    if hasattr(container, "replace_item"):
        from azure.core import MatchConditions

        return await _maybe_await(
            container.replace_item(
                item=item["id"],
                body=item,
                partition_key=partition_key,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        )
    return await _maybe_await(container.upsert_item(item))


async def _aiter(values):
    if hasattr(values, "__aiter__"):
        async for item in values:
            yield item
    else:
        for item in values:
            yield item


def _is_conflict(exc: Exception) -> bool:
    return exc.__class__.__name__ == "CosmosResourceExistsError" or getattr(exc, "status_code", None) == 409


def _is_not_found(exc: Exception) -> bool:
    return exc.__class__.__name__ == "CosmosResourceNotFoundError" or getattr(exc, "status_code", None) == 404


def _is_precondition_failed(exc: Exception) -> bool:
    return getattr(exc, "status_code", None) == 412 or exc.__class__.__name__ == "CosmosAccessConditionFailedError"
