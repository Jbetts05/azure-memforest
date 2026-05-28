"""Async Cosmos adapters for raw source sessions and turns."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any

from src.cloud.schema import SCHEMA_VERSION, partition_key

MAX_TURN_CONTENT_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class SourceSessionDocument:
    session_id: str
    tenant_id: str
    user_id: str
    cell_ids: tuple[str, ...] = field(default_factory=tuple)
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION
    tombstoned: bool = False


@dataclass(frozen=True, slots=True)
class SourceTurnDocument:
    turn_id: str
    session_id: str
    turn_index: int
    role: str
    content: str
    tenant_id: str
    user_id: str
    timestamp: str | None = None
    content_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION
    tombstoned: bool = False


class CosmosSessionStore:
    """Durable source sessions scoped by tenant/user, independent of forest version."""

    def __init__(self, container: Any, *, tenant_id: str, user_id: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def upsert_session(self, session_id: str, *, cell_ids: list[str] | None = None, metadata: dict[str, Any] | None = None) -> SourceSessionDocument:
        existing = await self._read_raw_session(session_id)
        if existing is not None and existing.get("tombstoned"):
            return _session_from_doc(existing)
        doc = SourceSessionDocument(
            session_id=session_id,
            tenant_id=self.tenant_id,
            user_id=self.user_id,
            cell_ids=tuple(cell_ids or ()),
            metadata=dict(metadata or {}),
        )
        await _maybe_await(self.container.upsert_item(_session_to_doc(doc, self.tenant_user_key)))
        return doc

    async def get_session(self, session_id: str) -> SourceSessionDocument | None:
        try:
            item = await _maybe_await(self.container.read_item(item=_session_id(session_id), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id or item.get("tombstoned"):
            return None
        return _session_from_doc(item)

    async def _read_raw_session(self, session_id: str) -> dict[str, Any] | None:
        try:
            item = await _maybe_await(self.container.read_item(item=_session_id(session_id), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id:
            return None
        return item

    async def tombstone_session(self, session_id: str, *, reason: str, scrub: bool = False) -> None:
        """Tombstone the session record.

        Callers handling user deletion must also call
        `CosmosTurnStore.tombstone_session_turns` to scrub raw turn content.
        """
        try:
            item = await _maybe_await(self.container.read_item(item=_session_id(session_id), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return
            raise
        item["tombstoned"] = True
        item["tombstone_reason"] = reason
        item["tombstoned_at_unix"] = time.time()
        if scrub:
            item["metadata"] = {}
            item["cell_ids"] = []
        await _maybe_await(self.container.upsert_item(item))


class CosmosTurnStore:
    """Durable raw turns scoped by tenant/user, independent of forest version."""

    def __init__(self, container: Any, *, tenant_id: str, user_id: str) -> None:
        self.container = container
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.tenant_user_key = partition_key(tenant_id=tenant_id, user_id=user_id)

    async def upsert_turns(self, session_id: str, turns: list[dict[str, Any]]) -> list[SourceTurnDocument]:
        new_turn_ids = {
            str(turn.get("turn_id") or turn.get("content_id") or f"{session_id}:{index}")
            for index, turn in enumerate(turns)
        }
        stale_tail_ids = await self._tombstone_stale_tail(
            session_id,
            new_count=len(turns),
            new_turn_ids=new_turn_ids,
        )
        docs: list[SourceTurnDocument] = []
        for turn_index, turn in enumerate(turns):
            content = str(turn.get("content", ""))
            if len(content.encode("utf-8")) > MAX_TURN_CONTENT_BYTES:
                raise ValueError("turn content exceeds Cosmos source turn size budget.")
            turn_id = str(turn.get("turn_id") or turn.get("content_id") or f"{session_id}:{turn_index}")
            doc = SourceTurnDocument(
                turn_id=turn_id,
                session_id=session_id,
                turn_index=turn_index,
                role=str(turn.get("role", "")),
                content=content,
                timestamp=turn.get("timestamp"),
                content_id=turn.get("content_id"),
                metadata=dict(turn.get("metadata") or {}),
                tenant_id=self.tenant_id,
                user_id=self.user_id,
            )
            existing = await self._read_raw_turn(session_id, turn_id)
            if existing is not None and existing.get("tombstoned") and existing["id"] not in stale_tail_ids:
                continue
            await _maybe_await(self.container.upsert_item(_turn_to_doc(doc, self.tenant_user_key)))
            docs.append(doc)
        return docs

    async def list_turns(self, session_id: str) -> list[SourceTurnDocument]:
        query = (
            "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.session_id=@session_id "
            "ORDER BY c.turn_index ASC"
        )
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@session_id", "value": session_id},
        ]
        turns = []
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            if item.get("tenant_id") == self.tenant_id and item.get("user_id") == self.user_id and not item.get("tombstoned"):
                turns.append(_turn_from_doc(item))
        return sorted(turns, key=lambda turn: turn.turn_index)

    async def tombstone_session_turns(self, session_id: str, *, reason: str, scrub_content: bool = True) -> None:
        query = "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.session_id=@session_id"
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@session_id", "value": session_id},
        ]
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            item["tombstoned"] = True
            item["tombstone_reason"] = reason
            item["tombstoned_at_unix"] = time.time()
            if scrub_content:
                item["content"] = ""
                item["metadata"] = {}
            await _maybe_await(self.container.upsert_item(item))

    async def _tombstone_stale_tail(self, session_id: str, *, new_count: int, new_turn_ids: set[str]) -> set[str]:
        query = "SELECT * FROM c WHERE c.tenant_user_key=@pk AND c.session_id=@session_id"
        params = [
            {"name": "@pk", "value": self.tenant_user_key},
            {"name": "@session_id", "value": session_id},
        ]
        tombstoned_ids: set[str] = set()
        async for item in _aiter(self.container.query_items(query=query, parameters=params, partition_key=self.tenant_user_key)):
            stale_by_index = int(item.get("turn_index", -1)) >= new_count
            stale_by_id = str(item.get("turn_id", "")) not in new_turn_ids
            if (stale_by_index or stale_by_id) and not item.get("tombstoned"):
                item["tombstoned"] = True
                item["tombstone_reason"] = "stale_reingest_tail"
                item["tombstoned_at_unix"] = time.time()
                item["content"] = ""
                item["metadata"] = {}
                await _maybe_await(self.container.upsert_item(item))
                tombstoned_ids.add(item["id"])
        return tombstoned_ids

    async def _read_raw_turn(self, session_id: str, turn_id: str) -> dict[str, Any] | None:
        try:
            item = await _maybe_await(self.container.read_item(item=_turn_id(session_id, turn_id), partition_key=self.tenant_user_key))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        if item.get("tenant_id") != self.tenant_id or item.get("user_id") != self.user_id:
            return None
        return item


def _session_to_doc(doc: SourceSessionDocument, tenant_user_key: str) -> dict[str, Any]:
    return {
        "id": _session_id(doc.session_id),
        "schema_version": doc.schema_version,
        "tenant_id": doc.tenant_id,
        "user_id": doc.user_id,
        "tenant_user_key": tenant_user_key,
        "session_id": doc.session_id,
        "cell_ids": list(doc.cell_ids),
        "metadata": dict(doc.metadata),
        "tombstoned": doc.tombstoned,
        "updated_at_unix": time.time(),
    }


def _turn_to_doc(doc: SourceTurnDocument, tenant_user_key: str) -> dict[str, Any]:
    return {
        "id": _turn_id(doc.session_id, doc.turn_id),
        "schema_version": doc.schema_version,
        "tenant_id": doc.tenant_id,
        "user_id": doc.user_id,
        "tenant_user_key": tenant_user_key,
        "turn_id": doc.turn_id,
        "session_id": doc.session_id,
        "turn_index": doc.turn_index,
        "role": doc.role,
        "content": doc.content,
        "timestamp": doc.timestamp,
        "content_id": doc.content_id,
        "metadata": dict(doc.metadata),
        "tombstoned": doc.tombstoned,
        "updated_at_unix": time.time(),
    }


def _session_from_doc(item: dict[str, Any]) -> SourceSessionDocument:
    return SourceSessionDocument(
        session_id=item["session_id"],
        tenant_id=item["tenant_id"],
        user_id=item["user_id"],
        cell_ids=tuple(item.get("cell_ids", ())),
        metadata=dict(item.get("metadata", {})),
        schema_version=int(item.get("schema_version", SCHEMA_VERSION)),
        tombstoned=bool(item.get("tombstoned", False)),
    )


def _turn_from_doc(item: dict[str, Any]) -> SourceTurnDocument:
    return SourceTurnDocument(
        turn_id=item["turn_id"],
        session_id=item["session_id"],
        turn_index=int(item["turn_index"]),
        role=item["role"],
        content=item.get("content", ""),
        timestamp=item.get("timestamp"),
        content_id=item.get("content_id"),
        metadata=dict(item.get("metadata", {})),
        tenant_id=item["tenant_id"],
        user_id=item["user_id"],
        schema_version=int(item.get("schema_version", SCHEMA_VERSION)),
        tombstoned=bool(item.get("tombstoned", False)),
    )


def _session_id(session_id: str) -> str:
    return _hash_id("session", session_id)


def _turn_id(session_id: str, turn_id: str) -> str:
    return _hash_id("turn", session_id, turn_id)


def _hash_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
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
    return exc.__class__.__name__ == "CosmosResourceNotFoundError" or getattr(exc, "status_code", None) == 404
