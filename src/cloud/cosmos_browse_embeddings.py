"""Cosmos-backed browse embedding store."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any

from src.cloud.async_batch import run_bounded
from src.cloud.schema import SCHEMA_VERSION, partition_key


@dataclass(frozen=True, slots=True)
class BrowseEmbeddingRecord:
    node_id: str
    tree_id: str
    tree_type: str
    embedding: tuple[float, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "embedding", tuple(float(value) for value in self.embedding))


class CosmosBrowseEmbeddingStore:
    """Browse embeddings scoped to one tenant/user/forest/model version."""

    def __init__(
        self,
        container: Any,
        *,
        tenant_id: str,
        user_id: str,
        forest_version: str,
        embedding_model_version: str,
    ) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.forest_version = forest_version
        self.embedding_model_version = embedding_model_version
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def get_embedding(self, node_id: str) -> list[float] | None:
        try:
            item = await _maybe_await(self.container.read_item(item=_embedding_id(node_id, self.embedding_model_version), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if not self._in_scope(item) or item.get("tombstoned"):
            return None
        return [float(value) for value in item.get("embedding", [])]

    async def get_embeddings(self, node_ids: list[str]) -> dict[str, list[float]]:
        embeddings: dict[str, list[float]] = {}
        for node_id in node_ids:
            embedding = await self.get_embedding(node_id)
            if embedding is not None:
                embeddings[node_id] = embedding
        return embeddings

    async def upsert_embeddings(self, embeddings: dict[str, list[float]], *, model_version: str) -> None:
        records = [
            BrowseEmbeddingRecord(node_id=node_id, tree_id="", tree_type="", embedding=tuple(embedding))
            for node_id, embedding in embeddings.items()
        ]
        await self.upsert_records(records, model_version=model_version)

    async def upsert_records(self, records: list[BrowseEmbeddingRecord], *, model_version: str) -> None:
        if model_version != self.embedding_model_version:
            raise ValueError("model_version does not match CosmosBrowseEmbeddingStore scope.")

        async def _upsert(record: BrowseEmbeddingRecord) -> None:
            await _maybe_await(self.container.upsert_item(_record_to_doc(record, self)))

        await run_bounded(records, _upsert)

    async def tombstone_tree(self, tree_id: str, *, reason: str) -> None:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.tree_id=@tree_id "
            "AND c.embedding_model_version=@embedding_model_version"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@tree_id", "value": tree_id},
            {"name": "@embedding_model_version", "value": self.embedding_model_version},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            item["tombstoned"] = True
            item["tombstone_reason"] = reason
            item["tombstoned_at_unix"] = time.time()
            await _maybe_await(self.container.upsert_item(item))

    def _in_scope(self, item: dict[str, Any]) -> bool:
        return (
            item.get("tenant_id") == self.tenant_id
            and item.get("user_id") == self.user_id
            and item.get("forest_version") == self.forest_version
            and item.get("embedding_model_version") == self.embedding_model_version
        )


def _record_to_doc(record: BrowseEmbeddingRecord, store: CosmosBrowseEmbeddingStore) -> dict[str, Any]:
    return {
        "id": _embedding_id(record.node_id, store.embedding_model_version),
        "schema_version": SCHEMA_VERSION,
        "tenant_id": store.tenant_id,
        "user_id": store.user_id,
        "tenant_user_key": store.tenant_user_key,
        "forest_version": store.forest_version,
        "embedding_model_version": store.embedding_model_version,
        "node_id": record.node_id,
        "tree_id": record.tree_id,
        "tree_type": record.tree_type,
        "embedding": list(record.embedding),
        "tombstoned": False,
        "updated_at_unix": time.time(),
    }


def _embedding_id(node_id: str, embedding_model_version: str) -> str:
    digest = hashlib.sha256(f"{node_id}\x1f{embedding_model_version}".encode("utf-8")).hexdigest()
    return f"browse_embedding_{digest}"


async def _maybe_await(value: Any):
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
    return exc.__class__.__name__ == "CosmosResourceNotFoundError" or getattr(exc, "status_code", None) == 404
