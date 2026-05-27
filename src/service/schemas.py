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
    facts: list[MemoryFactResponse] = Field(default_factory=list)


class DeleteSessionResponse(BaseModel):
    job_id: str
    status: JobStatus = "accepted"
    tenant_id: str
    user_id: str
    session_id: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    tenant_id: str
    user_id: str
    submitted_at: float | None = None
    completed_at: float | None = None
    error: str | None = None


class ErrorResponse(BaseModel):
    status: Literal["error"] = "error"
    error: str
    request_id: str = ""
