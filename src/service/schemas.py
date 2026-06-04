"""HTTP API schemas for Azure-MemForest."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


JobStatus = Literal["accepted", "running", "succeeded", "failed", "cancelled"]


class TurnInput(BaseModel):
    role: str = Field(min_length=1, max_length=64)
    content: str = Field(min_length=1)
    timestamp: str | None = None
    content_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestSessionRequest(BaseModel):
    turns: list[TurnInput] = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestSessionResponse(BaseModel):
    job_id: str
    status: JobStatus = "accepted"
    tenant_id: str
    user_id: str
    session_id: str
    progress_budget: dict[str, Any] = Field(default_factory=dict)


class QueryRequest(BaseModel):
    question: str = Field(min_length=1)
    query_time: float | None = None
    top_k: int | None = Field(default=None, ge=1)
    max_facts: int | None = Field(default=None, ge=0)
    trace_requested: bool = False


class MemoryFactResponse(BaseModel):
    fact_id: str
    text: str
    time_start: float | None = None
    time_end: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class QueryResponse(BaseModel):
    query_id: str
    status: Literal["succeeded"] = "succeeded"
    answer: str | None = None
    reason: str | None = None
    facts: list[MemoryFactResponse] = Field(default_factory=list)


class ChatRequest(BaseModel):
    message: str = Field(min_length=1)
    turns: list[TurnInput] = Field(default_factory=list)
    max_facts: int | None = Field(default=8, ge=0)
    trace_requested: bool = False


class ChatResponse(BaseModel):
    status: Literal["succeeded"] = "succeeded"
    reply: str
    query_id: str | None = None
    reason: str | None = None
    memory_status: Literal["grounded", "no_forest", "no_facts", "unavailable"] = "unavailable"
    facts: list[MemoryFactResponse] = Field(default_factory=list)


class DeleteSessionResponse(BaseModel):
    job_id: str
    status: JobStatus = "accepted"
    tenant_id: str
    user_id: str
    session_id: str


class ClearMemoryResponse(BaseModel):
    job_id: str
    status: JobStatus = "accepted"
    tenant_id: str
    user_id: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    tenant_id: str
    user_id: str
    submitted_at: float | None = None
    completed_at: float | None = None
    error: str | None = None
    stage: str | None = None
    stage_started_at: float | None = None
    stage_history: list[dict[str, Any]] = Field(default_factory=list)
    telemetry: dict[str, Any] = Field(default_factory=dict)
    progress_signature: dict[str, Any] = Field(default_factory=dict)
    progress_budget: dict[str, Any] = Field(default_factory=dict)
    last_progress_at: float | None = None


class JobEventResponse(BaseModel):
    event_type: str
    timestamp_unix: float | None = None
    status: str
    detail: str | None = None


class QueryTraceResponse(BaseModel):
    query_id: str
    tenant_id: str
    user_id: str
    forest_version: str
    event_count: int
    trace_truncated: bool = False
    dropped_attribute_count: int = 0
    events: list[dict[str, Any]] = Field(default_factory=list)
    event_summaries: list[dict[str, Any]] = Field(default_factory=list)


class SourceSessionResponse(BaseModel):
    session_id: str
    cell_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceTurnResponse(BaseModel):
    turn_id: str
    session_id: str
    turn_index: int
    role: str
    content: str
    timestamp: str | None = None
    content_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ForestManifestResponse(BaseModel):
    manifest_id: str
    forest_version: str
    generation: int
    status: str
    created_at_unix: float
    committed_at_unix: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TreeNodeResponse(BaseModel):
    tree_id: str
    node_id: str
    level: str
    summary: str
    child_ids: list[str] = Field(default_factory=list)
    fact_ids: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class TreeListResponse(BaseModel):
    forest_version: str
    roots: list[TreeNodeResponse] = Field(default_factory=list)


class SubtreeResponse(BaseModel):
    forest_version: str
    tree_id: str
    root_node_id: str
    truncated: bool = False
    nodes: list[TreeNodeResponse] = Field(default_factory=list)


class FactDetailResponse(BaseModel):
    fact_id: str
    text: str
    time_start: float | None = None
    time_end: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    locations: list[dict[str, Any]] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    status: Literal["error"] = "error"
    error: str
    request_id: str = ""
