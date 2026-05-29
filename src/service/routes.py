"""HTTP routes for Azure-MemForest memory APIs."""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, Header, Request, status
from fastapi.responses import JSONResponse

from src.service.auth import ActorContext, Forbidden, get_actor_context
from src.cloud.cosmos_jobs import JobNotFound
from src.cloud.cosmos_traces import TraceNotFound
from src.cloud.query_prerequisites import CloudQueryNotReady
from src.cloud.ui_read_service import UiReadNotFound
from src.service.middleware import REQUEST_ID_HEADER
from src.service.schemas import (
    ChatRequest,
    ChatResponse,
    DeleteSessionResponse,
    IngestSessionRequest,
    IngestSessionResponse,
    JobStatusResponse,
    JobEventResponse,
    QueryRequest,
    QueryResponse,
    QueryTraceResponse,
    SourceSessionResponse,
    SourceTurnResponse,
    ForestManifestResponse,
    TreeListResponse,
    TreeNodeResponse,
    SubtreeResponse,
    FactDetailResponse,
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


@router.post("/users/{user_id}/chat", response_model=ChatResponse)
async def chat_memory(
    user_id: str,
    payload: ChatRequest,
    request: Request,
    actor: ActorContext = Depends(get_actor_context),
):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "memory_chat_service")
    return await service.chat(actor, user_id=user_id, request=payload)


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


@router.get("/users/{user_id}/jobs/{job_id}/events", response_model=list[JobEventResponse])
async def get_job_events(
    user_id: str,
    job_id: str,
    request: Request,
    actor: ActorContext = Depends(get_actor_context),
):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "job_status_service")
    try:
        job = await service.get_job(actor, user_id=user_id, job_id=job_id)
    except JobNotFound:
        return _error_response(
            request,
            status_code=status.HTTP_404_NOT_FOUND,
            error="job_not_found",
        )
    status_value = _field(job, "status")
    events = [
        JobEventResponse(event_type="accepted", timestamp_unix=_field(job, "submitted_at"), status="accepted"),
    ]
    if status_value in {"running", "succeeded", "failed", "cancelled"}:
        events.append(JobEventResponse(event_type="running", timestamp_unix=None, status="running", detail="Job has been claimed by a worker."))
    if status_value in {"succeeded", "failed", "cancelled"}:
        events.append(JobEventResponse(event_type=status_value, timestamp_unix=_field(job, "completed_at"), status=status_value, detail=_field(job, "error")))
    return events


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


@router.get("/users/{user_id}/sessions", response_model=list[SourceSessionResponse])
async def list_sessions(user_id: str, request: Request, limit: int = 100, actor: ActorContext = Depends(get_actor_context)):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "ui_read_service")
    return await service.list_sessions(actor, user_id=user_id, limit=limit)


@router.get("/users/{user_id}/sessions/{session_id}", response_model=SourceSessionResponse)
async def get_session(user_id: str, session_id: str, request: Request, actor: ActorContext = Depends(get_actor_context)):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "ui_read_service")
    try:
        return await service.get_session(actor, user_id=user_id, session_id=session_id)
    except UiReadNotFound:
        return _error_response(request, status_code=status.HTTP_404_NOT_FOUND, error="session_not_found")


@router.get("/users/{user_id}/sessions/{session_id}/turns", response_model=list[SourceTurnResponse])
async def list_session_turns(user_id: str, session_id: str, request: Request, limit: int = 100, actor: ActorContext = Depends(get_actor_context)):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "ui_read_service")
    return await service.list_turns(actor, user_id=user_id, session_id=session_id, limit=limit)


@router.get("/users/{user_id}/forest/latest", response_model=ForestManifestResponse)
async def get_latest_forest(user_id: str, request: Request, actor: ActorContext = Depends(get_actor_context)):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "ui_read_service")
    try:
        return await service.latest_manifest(actor, user_id=user_id)
    except UiReadNotFound:
        return _error_response(request, status_code=status.HTTP_404_NOT_FOUND, error="forest_not_found")


@router.get("/users/{user_id}/forest/{forest_version}/trees", response_model=TreeListResponse)
async def list_tree_roots(user_id: str, forest_version: str, request: Request, limit: int = 100, actor: ActorContext = Depends(get_actor_context)):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "ui_read_service")
    try:
        return await service.list_tree_roots(actor, user_id=user_id, forest_version=forest_version, limit=limit)
    except UiReadNotFound:
        return _error_response(request, status_code=status.HTTP_404_NOT_FOUND, error="forest_not_found")


@router.get("/users/{user_id}/forest/{forest_version}/trees/{tree_id}/nodes/{node_id}", response_model=TreeNodeResponse)
async def get_tree_node(user_id: str, forest_version: str, tree_id: str, node_id: str, request: Request, actor: ActorContext = Depends(get_actor_context)):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "ui_read_service")
    try:
        return await service.get_tree_node(actor, user_id=user_id, forest_version=forest_version, tree_id=tree_id, node_id=node_id)
    except UiReadNotFound:
        return _error_response(request, status_code=status.HTTP_404_NOT_FOUND, error="tree_node_not_found")


@router.get("/users/{user_id}/forest/{forest_version}/trees/{tree_id}/subtree", response_model=SubtreeResponse)
async def get_subtree(user_id: str, forest_version: str, tree_id: str, request: Request, root_node_id: str, max_depth: int = 2, max_nodes: int = 250, actor: ActorContext = Depends(get_actor_context)):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "ui_read_service")
    try:
        return await service.get_subtree(actor, user_id=user_id, forest_version=forest_version, tree_id=tree_id, root_node_id=root_node_id, max_depth=max_depth, max_nodes=max_nodes)
    except UiReadNotFound:
        return _error_response(request, status_code=status.HTTP_404_NOT_FOUND, error="subtree_not_found")


@router.get("/users/{user_id}/forest/{forest_version}/facts/{fact_id}", response_model=FactDetailResponse)
async def get_fact_detail(user_id: str, forest_version: str, fact_id: str, request: Request, actor: ActorContext = Depends(get_actor_context)):
    _enforce_user_access(actor, user_id)
    service = _required_service(request, "ui_read_service")
    try:
        return await service.get_fact(actor, user_id=user_id, forest_version=forest_version, fact_id=fact_id)
    except UiReadNotFound:
        return _error_response(request, status_code=status.HTTP_404_NOT_FOUND, error="fact_not_found")


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


def _field(value, name: str):
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


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
