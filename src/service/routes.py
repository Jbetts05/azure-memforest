"""HTTP routes for Azure-MemForest memory APIs."""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse

from src.service.auth import ActorContext, Forbidden, get_actor_context
from src.cloud.cosmos_jobs import JobNotFound
from src.cloud.cosmos_traces import TraceNotFound
from src.cloud.query_prerequisites import CloudQueryNotReady
from src.service.middleware import REQUEST_ID_HEADER
from src.service.schemas import (
    DeleteSessionResponse,
    IngestSessionRequest,
    IngestSessionResponse,
    JobStatusResponse,
    QueryRequest,
    QueryResponse,
    QueryTraceResponse,
)

IDEMPOTENCY_HEADER = "Idempotency-Key"
_IDEMPOTENCY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,255}$")

router = APIRouter(prefix="/v1", tags=["memory"])


@router.post(
    "/users/{user_id}/sessions/{session_id}:ingest",
    response_model=IngestSessionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_session(
    user_id: str,
    session_id: str,
    payload: IngestSessionRequest,
    request: Request,
    actor: ActorContext = Depends(get_actor_context),
    idempotency_key: str | None = Header(default=None, alias=IDEMPOTENCY_HEADER),
):
    _enforce_user_access(actor, user_id)
    key = _require_idempotency_key(idempotency_key, request)
    service = _required_service(request, "memory_command_service")
    return await service.submit_ingest_session(
        actor,
        user_id=user_id,
        session_id=session_id,
        request=payload,
        idempotency_key=key,
    )


@router.post("/users/{user_id}/query", response_model=QueryResponse)
async def query_memory(
    user_id: str,
    payload: QueryRequest,
    request: Request,
    actor: ActorContext = Depends(get_actor_context),
):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "memory_query_service")
    try:
        return await service.query(actor, user_id=user_id, request=payload)
    except CloudQueryNotReady as exc:
        return _error_response(
            request,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error=f"cloud_query_not_ready:{exc.reason}",
        )


@router.post(
    "/users/{user_id}/sessions/{session_id}:delete",
    response_model=DeleteSessionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def delete_session(
    user_id: str,
    session_id: str,
    request: Request,
    actor: ActorContext = Depends(get_actor_context),
    idempotency_key: str | None = Header(default=None, alias=IDEMPOTENCY_HEADER),
):
    _enforce_user_access(actor, user_id)
    key = _require_idempotency_key(idempotency_key, request)
    service = _required_service(request, "memory_command_service")
    return await service.submit_delete_session(
        actor,
        user_id=user_id,
        session_id=session_id,
        idempotency_key=key,
    )


@router.get("/users/{user_id}/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    user_id: str,
    job_id: str,
    request: Request,
    actor: ActorContext = Depends(get_actor_context),
):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "job_status_service")
    try:
        return await service.get_job(actor, user_id=user_id, job_id=job_id)
    except JobNotFound:
        return _error_response(
            request,
            status_code=status.HTTP_404_NOT_FOUND,
            error="job_not_found",
        )


@router.get("/users/{user_id}/queries/{query_id}/trace", response_model=QueryTraceResponse)
async def get_query_trace(
    user_id: str,
    query_id: str,
    request: Request,
    actor: ActorContext = Depends(get_actor_context),
):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "query_trace_service")
    try:
        return await service.get_trace(actor, user_id=user_id, query_id=query_id)
    except TraceNotFound:
        return _error_response(
            request,
            status_code=status.HTTP_404_NOT_FOUND,
            error="query_trace_not_found",
        )


def _enforce_user_access(actor: ActorContext, user_id: str) -> None:
    if actor.actor_id == user_id or "memory.admin" in actor.roles:
        return
    raise Forbidden()


def _require_idempotency_key(value: str | None, request: Request) -> str:
    key = str(value or "").strip()
    if _IDEMPOTENCY_PATTERN.fullmatch(key):
        return key
    return _error_response(
        request,
        status_code=status.HTTP_400_BAD_REQUEST,
        error="idempotency_key_required",
    )


def _required_service(request: Request, name: str):
    service = getattr(request.app.state, name, None)
    if service is not None:
        return service
    return _error_response(
        request,
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        error="service_not_configured",
    )


def _error_response(request: Request, *, status_code: int, error: str):
    request_id = getattr(request.state, "request_id", "")
    raise _RouteError(status_code=status_code, error=error, request_id=request_id)


class _RouteError(Exception):
    def __init__(self, *, status_code: int, error: str, request_id: str) -> None:
        self.status_code = status_code
        self.error = error
        self.request_id = request_id


async def route_error_handler(_request: Request, exc: _RouteError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        headers={REQUEST_ID_HEADER: exc.request_id} if exc.request_id else None,
        content={
            "status": "error",
            "error": exc.error,
            "request_id": exc.request_id,
        },
    )
