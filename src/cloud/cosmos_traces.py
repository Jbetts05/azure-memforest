"""Cosmos-backed query trace recorder."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from threading import RLock
from typing import Any

from src.cloud.schema import SCHEMA_VERSION
from src.observability.tracing import TraceEvent, TraceSink
from src.service.auth import ActorContext
from src.service.schemas import QueryTraceResponse

MAX_ATTRIBUTE_STRING_LENGTH = 256
MAX_EVENT_BYTES = 4096
MAX_TRACE_BYTES = 1024 * 1024
FORBIDDEN_ATTRIBUTE_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "embedding",
        "embedding_vector",
        "exception_args",
        "exception_message",
        "fact_text",
        "password",
        "prompt",
        "raw_prompt",
        "raw_query",
        "raw_request",
        "raw_response",
        "response",
        "stack_trace",
        "token",
        "turn_content",
    }
)
log = logging.getLogger(__name__)


class TraceNotFound(Exception):
    pass


class BufferingTraceSink(TraceSink):
    """Synchronous TraceSink that buffers events for async persistence."""

    def __init__(self, recorder: "CosmosTraceRecorder | None" = None, *, max_events_per_query: int = 500) -> None:
        self._recorder = recorder
        self._max_events_per_query = max_events_per_query
        self._events_by_query: dict[str, list[TraceEvent]] = {}
        self._truncated_queries: set[str] = set()
        self._lock = RLock()

    def emit(self, event: TraceEvent) -> None:
        with self._lock:
            events = self._events_by_query.setdefault(event.query_id, [])
            if len(events) >= self._max_events_per_query:
                self._truncated_queries.add(event.query_id)
                return
            events.append(event)

    def events(self, query_id: str) -> list[TraceEvent]:
        with self._lock:
            return list(self._events_by_query.get(query_id, ()))

    async def flush(
        self,
        *,
        query_id: str,
        tenant_id: str,
        user_id: str,
        forest_version: str,
    ) -> None:
        if self._recorder is None:
            return
        with self._lock:
            events = list(self._events_by_query.pop(query_id, ()))
            buffer_truncated = query_id in self._truncated_queries
            self._truncated_queries.discard(query_id)
        if events:
            try:
                await self._recorder.record_events(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    query_id=query_id,
                    forest_version=forest_version,
                    events=events,
                    force_truncated=buffer_truncated,
                )
            except Exception:
                log.exception("Failed to persist trace query_id=%s", query_id)


@dataclass(frozen=True, slots=True)
class CosmosTraceRecorder:
    container: Any
    max_events_per_query: int = 500
    max_trace_bytes: int = MAX_TRACE_BYTES

    async def record_events(
        self,
        *,
        tenant_id: str,
        user_id: str,
        query_id: str,
        forest_version: str,
        events: list[TraceEvent],
        force_truncated: bool = False,
    ) -> dict[str, Any]:
        if not (tenant_id and user_id and query_id and forest_version):
            raise ValueError("tenant_id, user_id, query_id, and forest_version are required for persisted traces.")
        sorted_events = sorted(events, key=lambda event: (event.timestamp_unix, event.event_id))
        serialized_events = []
        trace_truncated = force_truncated
        dropped_attribute_count = 0
        for event in sorted_events:
            if len(serialized_events) >= self.max_events_per_query:
                trace_truncated = True
                break
            payload, dropped = _event_to_payload(event)
            dropped_attribute_count += dropped
            if _json_size(payload) > MAX_EVENT_BYTES:
                payload["attributes"] = {}
                payload["attribute_truncated"] = True
                dropped_attribute_count += 1
            serialized_events.append(payload)
            candidate_doc = _trace_document(
                tenant_id=tenant_id,
                user_id=user_id,
                query_id=query_id,
                forest_version=forest_version,
                events=serialized_events,
                trace_truncated=trace_truncated,
                dropped_attribute_count=dropped_attribute_count,
            )
            if _json_size(candidate_doc) > self.max_trace_bytes:
                serialized_events.pop()
                trace_truncated = True
                break

        document = _trace_document(
            tenant_id=tenant_id,
            user_id=user_id,
            query_id=query_id,
            forest_version=forest_version,
            events=serialized_events,
            trace_truncated=trace_truncated,
            dropped_attribute_count=dropped_attribute_count,
        )
        await _maybe_await(_upsert_item(self.container, document, partition_key=tenant_id))
        return document

    async def get_trace(self, *, tenant_id: str, user_id: str, query_id: str) -> dict[str, Any]:
        try:
            item = await _maybe_await(
                self.container.read_item(
                    item=_trace_id(query_id),
                    partition_key=tenant_id,
                )
            )
        except Exception as exc:
            if _is_cosmos_not_found(exc):
                raise TraceNotFound() from exc
            raise
        if item.get("tenant_id") != tenant_id or item.get("user_id") != user_id:
            raise TraceNotFound()
        return item


def trace_recorder_from_database(database_client: Any) -> CosmosTraceRecorder:
    return CosmosTraceRecorder(container=database_client.get_container_client("query_traces"))


class CosmosQueryTraceService:
    def __init__(self, recorder: CosmosTraceRecorder) -> None:
        self._recorder = recorder

    async def get_trace(self, actor: ActorContext, *, user_id: str, query_id: str) -> QueryTraceResponse:
        item = await self._recorder.get_trace(
            tenant_id=actor.tenant.tenant_id,
            user_id=user_id,
            query_id=query_id,
        )
        return QueryTraceResponse(
            query_id=item["query_id"],
            tenant_id=item["tenant_id"],
            user_id=item["user_id"],
            forest_version=item["forest_version"],
            event_count=int(item.get("event_count", 0)),
            trace_truncated=bool(item.get("trace_truncated", False)),
            dropped_attribute_count=int(item.get("dropped_attribute_count", 0)),
            events=list(item.get("events", [])),
        )


def _trace_document(
    *,
    tenant_id: str,
    user_id: str,
    query_id: str,
    forest_version: str,
    events: list[dict[str, Any]],
    trace_truncated: bool,
    dropped_attribute_count: int,
) -> dict[str, Any]:
    now = time.time()
    return {
        "id": _trace_id(query_id),
        "schema_version": SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "query_id": query_id,
        "forest_version": forest_version,
        "event_count": len(events),
        "trace_truncated": trace_truncated,
        "dropped_attribute_count": dropped_attribute_count,
        "created_at_unix": now,
        "updated_at_unix": now,
        "events": events,
    }


def _event_to_payload(event: TraceEvent) -> tuple[dict[str, Any], int]:
    attributes, dropped = _safe_attributes(event.attributes)
    payload = {
        "schema_version": event.schema_version,
        "event_id": event.event_id,
        "event_type": str(event.event_type),
        "timestamp_unix": event.timestamp_unix,
        "tree_id": event.tree_id,
        "node_id": event.node_id,
        "fact_id": event.fact_id,
        "score": event.score,
        "service_name": event.service_name,
        "duration_ms": event.duration_ms,
        "cache_hit": event.cache_hit,
        "attributes": attributes,
    }
    return {key: value for key, value in payload.items() if value is not None}, dropped


def _safe_attributes(attributes: Any) -> tuple[dict[str, Any], int]:
    safe: dict[str, Any] = {}
    dropped = 0
    if not isinstance(attributes, Mapping):
        return safe, 1
    for key, value in attributes.items():
        if not isinstance(key, str):
            dropped += 1
            continue
        if key.strip().lower() in FORBIDDEN_ATTRIBUTE_KEYS:
            dropped += 1
            continue
        clean = _safe_scalar(value)
        if clean is None and value is not None:
            dropped += 1
            continue
        safe[key] = clean
    return safe, dropped


def _safe_scalar(value: Any) -> Any | None:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value if len(value) <= MAX_ATTRIBUTE_STRING_LENGTH else value[:MAX_ATTRIBUTE_STRING_LENGTH]
    return None


def _trace_id(query_id: str) -> str:
    return f"query_{query_id}"


def _json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


def _upsert_item(container: Any, document: dict[str, Any], *, partition_key: str):
    try:
        return container.upsert_item(document)
    except TypeError:
        return container.upsert_item(document, partition_key=partition_key)


def _is_cosmos_not_found(exc: Exception) -> bool:
    if exc.__class__.__name__ == "CosmosResourceNotFoundError":
        return True
    return getattr(exc, "status_code", None) == 404
