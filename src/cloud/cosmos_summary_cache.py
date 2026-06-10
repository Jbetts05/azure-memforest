"""Async Cosmos content-hash summary cache."""

from __future__ import annotations

import hashlib
import time
from typing import Any

from src.cloud.schema import SCHEMA_VERSION, partition_key


class CosmosSummaryCache:
    """Content-addressed summaries scoped by tenant/user."""

    def __init__(self, container: Any, *, tenant_id: str, user_id: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def get(self, content_hash: str) -> str | None:
        try:
            item = await _maybe_await(self.container.read_item(item=_summary_doc_id(content_hash), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id or item.get("tombstoned"):
            return None
        return str(item.get("summary") or "")

    async def get_many(self, content_hashes: list[str]) -> dict[str, str]:
        if not content_hashes:
            return {}
        ids = [_summary_doc_id(content_hash) for content_hash in content_hashes]
        found: dict[str, str] = {}
        for chunk in _chunks(ids, 100):
            query = (
                "SELECT * FROM c WHERE c.tenant_user_key=@pk AND ARRAY_CONTAINS(@ids, c.id) "
                "AND (NOT IS_DEFINED(c.tombstoned) OR c.tombstoned=false)"
            )
            params = [
                {"name": "@pk", "value": self.tenant_user_key},
                {"name": "@ids", "value": chunk},
            ]
            async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
                if item.get("tenant_id") == self.tenant_id and item.get("user_id") == self.user_id:
                    found[str(item["content_hash"])] = str(item.get("summary") or "")
        return {content_hash: found[content_hash] for content_hash in content_hashes if content_hash in found}

    async def put(self, content_hash: str, summary: str, *, metadata: dict[str, Any] | None = None) -> None:
        doc = {
            "id": _summary_doc_id(content_hash),
            "schema_version": SCHEMA_VERSION,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "tenant_user_key": self.tenant_user_key,
            "content_hash": str(content_hash),
            "summary": str(summary),
            "summary_sha256": hashlib.sha256(str(summary).encode("utf-8")).hexdigest(),
            "metadata": dict(metadata or {}),
            "tombstoned": False,
            "updated_at_unix": time.time(),
        }
        await _maybe_await(self.container.upsert_item(doc))

    async def delete(self, content_hash: str) -> None:
        try:
            item = await _maybe_await(self.container.read_item(item=_summary_doc_id(content_hash), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id:
            return
        item["tombstoned"] = True
        item["tombstoned_at_unix"] = time.time()
        await _maybe_await(self.container.upsert_item(item))


def _summary_doc_id(content_hash: str) -> str:
    digest = hashlib.sha256(str(content_hash).encode("utf-8")).hexdigest()
    return f"summary_{digest}"


def _chunks(values: list[str], size: int):
    for index in range(0, len(values), size):
        yield values[index : index + size]


async def _aiter(value):
    if hasattr(value, "__aiter__"):
        async for item in value:
            yield item
    else:
        for item in value:
            yield item


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _is_not_found(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None)
    if status == 404:
        return True
    return exc.__class__.__name__ in {"ResourceNotFoundError", "CosmosResourceNotFoundError"}
