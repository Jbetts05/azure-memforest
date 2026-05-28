"""Service-layer ports for memory API route handlers."""

from __future__ import annotations

from typing import Protocol

from src.service.auth import ActorContext
from src.service.schemas import (
    DeleteSessionResponse,
    IngestSessionRequest,
    IngestSessionResponse,
    JobStatusResponse,
    QueryRequest,
    QueryResponse,
    QueryTraceResponse,
)


class MemoryCommandService(Protocol):
    """Write-side memory API port.

    Idempotency keys are supplied by the `Idempotency-Key` HTTP header only.
    Implementations must scope idempotency by tenant and user.
    """

    async def submit_ingest_session(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        session_id: str,
        request: IngestSessionRequest,
        idempotency_key: str,
    ) -> IngestSessionResponse:
        ...

    async def submit_delete_session(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        session_id: str,
        idempotency_key: str,
    ) -> DeleteSessionResponse:
        ...


class MemoryQueryService(Protocol):
    async def query(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        request: QueryRequest,
    ) -> QueryResponse:
        ...


class JobStatusService(Protocol):
    """Job status API port.

    Implementations must filter by ActorContext.tenant and user_id and should
    return not-found semantics rather than revealing cross-tenant existence.
    """

    async def get_job(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        job_id: str,
    ) -> JobStatusResponse:
        ...


class QueryTraceService(Protocol):
    async def get_trace(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        query_id: str,
    ) -> QueryTraceResponse:
        ...
