"""Structured retrieval tracing for explainable Azure-MemForest queries."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from threading import RLock
from types import MappingProxyType
from typing import Any, Mapping


class TraceEventType(StrEnum):
    QUERY_STARTED = "query_started"
    EMBEDDING_REQUESTED = "embedding_requested"
    RECALL_SEARCH_STARTED = "recall_search_started"
    RECALL_CANDIDATE_RETURNED = "recall_candidate_returned"
    RECALL_TREE_SELECTED = "recall_tree_selected"
    PLANNER_RESULT = "planner_result"
    BROWSE_TREE_STARTED = "browse_tree_started"
    BROWSE_LEVEL_EXPANDED = "browse_level_expanded"
    BROWSE_NODE_SCORED = "browse_node_scored"
    BROWSE_NODE_SELECTED = "browse_node_selected"
    BROWSE_NODE_PRUNED = "browse_node_pruned"
    LEAF_IDS_RETURNED = "leaf_ids_returned"
    FACTS_LOADED = "facts_loaded"
    FACTS_RERANKED = "facts_reranked"
    ANSWER_GENERATED = "answer_generated"
    QUERY_COMPLETED = "query_completed"
    SERVICE_CALL = "service_call"


@dataclass(frozen=True)
class TraceEvent:
    """One safe-by-default explainability event.

    Raw prompts and model responses are intentionally not first-class fields.
    If payload capture is ever required, it should be added behind an explicit
    tenant-approved policy and retention control.
    """

    query_id: str
    event_type: TraceEventType
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_unix: float = field(default_factory=time.time)
    tenant_id: str | None = None
    user_id: str | None = None
    tree_id: str | None = None
    node_id: str | None = None
    fact_id: str | None = None
    score: float | None = None
    service_name: str | None = None
    duration_ms: float | None = None
    cache_hit: bool | None = None
    attributes: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "attributes", MappingProxyType(dict(self.attributes)))


class InMemoryTraceSink:
    """Thread-safe trace sink for local tests and development diagnostics."""

    def __init__(self) -> None:
        self._events: list[TraceEvent] = []
        self._lock = RLock()

    def emit(self, event: TraceEvent) -> None:
        with self._lock:
            self._events.append(event)

    def events(self, *, query_id: str | None = None) -> list[TraceEvent]:
        with self._lock:
            if query_id is None:
                return list(self._events)
            return [event for event in self._events if event.query_id == query_id]
