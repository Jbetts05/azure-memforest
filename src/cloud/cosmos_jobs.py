"""Cosmos-backed job status store for write orchestration."""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.cloud.schema import SCHEMA_VERSION, partition_key
from src.service.jobs import WriteJobEnvelope
from src.service.ports import JobStatusService
from src.service.schemas import JobStatusResponse
from src.worker.progress import budget_for_envelope, classify_job_progress

if TYPE_CHECKING:
    from src.service.auth import ActorContext

log = logging.getLogger(__name__)


class JobNotFound(Exception):
    pass


class JobClaimError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class CosmosJobStore:
    """Job status store.

    The API creates accepted records after publishing messages. Workers own
    running/terminal state transitions and should use ETags when updating.
    """

    container: Any

    async def create_accepted_job(self, envelope: WriteJobEnvelope) -> JobStatusResponse:
        document = _job_document(envelope)
        try:
            await _maybe_await(
                _create_item(self.container, document, partition_key=envelope.tenant_id)
            )
        except Exception as exc:
            if _is_cosmos_conflict(exc):
                return await self.get_job_by_ids(
                    tenant_id=envelope.tenant_id,
                    user_id=envelope.user_id,
                    job_id=envelope.job_id,
                )
            raise
        return _response_from_document(document)

    async def get_job_by_ids(self, *, tenant_id: str, user_id: str, job_id: str) -> JobStatusResponse:
        try:
            item = await _maybe_await(
                self.container.read_item(
                    item=job_id,
                    partition_key=tenant_id,
                )
            )
        except Exception as exc:
            if _is_cosmos_not_found(exc):
                raise JobNotFound() from exc
            raise
        if str(item.get("tenant_id", "")) != tenant_id or str(item.get("user_id", "")) != user_id:
            raise JobNotFound()
        return _response_from_document(item)

    async def claim_job(self, *, tenant_id: str, user_id: str, job_id: str, owner_id: str, ttl_seconds: int) -> dict[str, Any]:
        item = await self._read_job_item(tenant_id=tenant_id, user_id=user_id, job_id=job_id)
        if item["status"] not in {"accepted", "running"}:
            raise JobClaimError(f"Cannot claim job in state {item['status']!r}.")
        now = time.time()
        expires_at = float(item.get("lease_expires_at_unix") or 0.0)
        current_owner = str(item.get("lease_owner_id") or "")
        if item["status"] == "running" and current_owner and current_owner != owner_id and expires_at > now:
            raise JobClaimError("Job is leased by another owner.")
        previous_token = int(item.get("fencing_token") or 0)
        same_owner_renewal = item["status"] == "running" and current_owner == owner_id
        item["status"] = "running"
        item.setdefault("started_at", now)
        item["lease_owner_id"] = owner_id
        item["lease_expires_at_unix"] = now + ttl_seconds
        item["fencing_token"] = previous_token if same_owner_renewal else previous_token + 1
        item.setdefault("last_progress_at", now)
        item["updated_at"] = now
        try:
            await _conditional_replace(self.container, item, partition_key=tenant_id, etag=item.get("_etag"))
        except Exception as exc:
            if _is_precondition_failed(exc):
                raise JobClaimError("Job changed before claim.") from exc
            raise
        return dict(item)

    async def complete_job(self, *, tenant_id: str, user_id: str, job_id: str, owner_id: str, fencing_token: int) -> JobStatusResponse:
        item = await self._read_job_item(tenant_id=tenant_id, user_id=user_id, job_id=job_id)
        if item.get("status") != "running":
            raise JobClaimError(f"Cannot complete job in state {item.get('status')!r}.")
        _assert_lease(item, owner_id=owner_id, fencing_token=fencing_token)
        item["status"] = "succeeded"
        item["completed_at"] = time.time()
        item["updated_at"] = item["completed_at"]
        await _conditional_replace(self.container, item, partition_key=tenant_id, etag=item.get("_etag"))
        return _response_from_document(item)

    async def fail_job(self, *, tenant_id: str, user_id: str, job_id: str, owner_id: str, fencing_token: int, error: str) -> JobStatusResponse:
        item = await self._read_job_item(tenant_id=tenant_id, user_id=user_id, job_id=job_id)
        if item.get("status") != "running":
            raise JobClaimError(f"Cannot fail job in state {item.get('status')!r}.")
        _assert_lease(item, owner_id=owner_id, fencing_token=fencing_token)
        item["status"] = "failed"
        item["error"] = error
        item["completed_at"] = time.time()
        item["updated_at"] = item["completed_at"]
        await _conditional_replace(self.container, item, partition_key=tenant_id, etag=item.get("_etag"))
        return _response_from_document(item)

    async def update_stage(
        self,
        *,
        tenant_id: str,
        user_id: str,
        job_id: str,
        owner_id: str,
        fencing_token: int,
        stage: str,
        detail: str = "",
        telemetry: dict[str, Any] | None = None,
    ) -> JobStatusResponse:
        item = await self._read_job_item(tenant_id=tenant_id, user_id=user_id, job_id=job_id)
        if item.get("status") != "running":
            raise JobClaimError(f"Cannot update job stage in state {item.get('status')!r}.")
        _assert_lease(item, owner_id=owner_id, fencing_token=fencing_token)
        now = time.time()
        previous_stage = item.get("stage")
        if previous_stage and previous_stage != stage:
            history = list(item.get("stage_history") or [])
            history.append(
                {
                    "stage": previous_stage,
                    "started_at": item.get("stage_started_at"),
                    "completed_at": now,
                    "detail": item.get("stage_detail") or "",
                }
            )
            item["stage_history"] = history[-50:]
        item["stage"] = stage
        item["stage_started_at"] = item.get("stage_started_at") if previous_stage == stage else now
        item["stage_detail"] = detail
        if telemetry is not None:
            item["telemetry"] = telemetry
        progress_signature = _merge_progress_signature(
            previous=item.get("progress_signature") or {},
            current=_progress_signature(stage=stage, telemetry=item.get("telemetry") or {}),
        )
        if progress_signature != item.get("progress_signature"):
            item["progress_signature"] = progress_signature
            item["last_progress_at"] = now
        item["updated_at"] = now
        await _conditional_replace(self.container, item, partition_key=tenant_id, etag=item.get("_etag"))
        return _response_from_document(item)

    async def fail_stalled_jobs(
        self,
        *,
        now: float | None = None,
        limit: int = 25,
        scan_limit: int = 200,
    ) -> list[JobStatusResponse]:
        now = time.time() if now is None else now
        failed: list[JobStatusResponse] = []
        scanned = 0
        async for item in _aiter(
            _query_items(
                self.container,
                query="SELECT * FROM c WHERE c.status = @status ORDER BY c.lease_expires_at_unix ASC",
                parameters=[{"name": "@status", "value": "running"}],
            )
        ):
            scanned += 1
            if scanned > scan_limit:
                break
            if len(failed) >= limit:
                break
            classification = classify_job_progress(item, now=now)
            if classification.state == "ok":
                continue
            lease_expires_at = float(item.get("lease_expires_at_unix") or 0.0)
            if lease_expires_at > now:
                continue
            item["status"] = "failed"
            item["error"] = (
                f"{classification.state}: {classification.reason}; "
                f"seconds_since_progress={classification.seconds_since_progress:.1f}"
            )
            item["completed_at"] = now
            item["updated_at"] = now
            item["reaped_at_unix"] = now
            item["reap_reason"] = classification.state
            item["lease_owner_id"] = ""
            item["lease_expires_at_unix"] = 0.0
            try:
                await _conditional_replace(self.container, item, partition_key=str(item["tenant_id"]), etag=item.get("_etag"))
            except Exception as exc:
                if _is_precondition_failed(exc):
                    continue
                raise
            log.warning(
                "job_reaped_after_stall",
                extra={
                    "job_id": item.get("job_id"),
                    "tenant_id": item.get("tenant_id"),
                    "user_id": item.get("user_id"),
                    "reap_reason": classification.state,
                    "seconds_since_progress": classification.seconds_since_progress,
                },
            )
            failed.append(_response_from_document(item))
        return failed

    async def _read_job_item(self, *, tenant_id: str, user_id: str, job_id: str) -> dict[str, Any]:
        try:
            item = await _maybe_await(self.container.read_item(item=job_id, partition_key=tenant_id))
        except Exception as exc:
            if _is_cosmos_not_found(exc):
                raise JobNotFound() from exc
            raise
        if str(item.get("tenant_id", "")) != tenant_id or str(item.get("user_id", "")) != user_id:
            raise JobNotFound()
        return item


class CosmosJobStatusService(JobStatusService):
    def __init__(self, job_store: CosmosJobStore) -> None:
        self._job_store = job_store

    async def get_job(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        job_id: str,
    ) -> JobStatusResponse:
        return await self._job_store.get_job_by_ids(
            tenant_id=actor.tenant.tenant_id,
            user_id=user_id,
            job_id=job_id,
        )


def job_store_from_database(database_client: Any) -> CosmosJobStore:
    return CosmosJobStore(container=database_client.get_container_client("jobs"))


def job_store_from_env(environ: dict[str, str]) -> CosmosJobStore | None:
    endpoint = str(environ.get("AZURE_COSMOS_ENDPOINT", "")).strip()
    database_name = str(environ.get("AZURE_COSMOS_DATABASE_NAME", "")).strip()
    client_id = str(environ.get("AZURE_CLIENT_ID", "")).strip()
    if not (endpoint and database_name and client_id):
        return None
    from azure.cosmos.aio import CosmosClient
    from azure.identity.aio import ManagedIdentityCredential

    credential = ManagedIdentityCredential(client_id=client_id)
    client = CosmosClient(endpoint, credential=credential)
    return job_store_from_database(client.get_database_client(database_name))


def _job_document(envelope: WriteJobEnvelope) -> dict[str, Any]:
    now = time.time()
    payload_json = envelope.to_json()
    budget = budget_for_envelope(envelope)
    return {
        "id": envelope.job_id,
        "schema_version": SCHEMA_VERSION,
        "tenant_id": envelope.tenant_id,
        "user_id": envelope.user_id,
        "tenant_user_key": partition_key(tenant_id=envelope.tenant_id, user_id=envelope.user_id),
        "job_id": envelope.job_id,
        "job_type": envelope.job_type,
        "session_id": envelope.session_id,
        "status": "accepted",
        "fencing_token": 0,
        "idempotency_key_hash": _idempotency_hash(envelope.tenant_id, envelope.idempotency_key),
        "payload_digest": hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        "payload_size_bytes": len(payload_json.encode("utf-8")),
        "progress_budget": {
            "job_type": budget.job_type,
            "stall_timeout_seconds": budget.stall_timeout_seconds,
            "max_completed_model_calls": budget.max_completed_model_calls,
            "max_retries": budget.max_retries,
        },
        "submitted_at": envelope.submitted_at_unix,
        "updated_at": now,
    }


def _response_from_document(document: dict[str, Any]) -> JobStatusResponse:
    return JobStatusResponse(
        job_id=str(document["job_id"]),
        status=document["status"],
        tenant_id=str(document["tenant_id"]),
        user_id=str(document["user_id"]),
        submitted_at=document.get("submitted_at"),
        completed_at=document.get("completed_at"),
        error=document.get("error"),
        stage=document.get("stage"),
        stage_started_at=document.get("stage_started_at"),
        stage_history=list(document.get("stage_history") or []),
        telemetry=dict(document.get("telemetry") or {}),
        progress_signature=dict(document.get("progress_signature") or {}),
        last_progress_at=document.get("last_progress_at"),
    )


def _idempotency_hash(tenant_id: str, idempotency_key: str) -> str:
    return hashlib.sha256(f"{tenant_id}:{idempotency_key}".encode("utf-8")).hexdigest()


def _progress_signature(*, stage: str, telemetry: dict[str, Any]) -> dict[str, Any]:
    calls = telemetry.get("calls") if isinstance(telemetry, dict) else {}
    embedded_items = 0
    if isinstance(calls, dict):
        for entry in calls.values():
            if isinstance(entry, dict) and entry.get("operation") == "embedding":
                embedded_items += int(entry.get("item_count") or 0)
    return {
        "stage": stage,
        "completed_model_calls": int((telemetry or {}).get("total_calls") or 0),
        "embedded_items": embedded_items,
        "durable_writes": int((telemetry or {}).get("durable_write_count") or 0),
    }


def _merge_progress_signature(*, previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    if not previous:
        return current
    return {
        "stage": current.get("stage") or previous.get("stage"),
        "completed_model_calls": max(int(previous.get("completed_model_calls") or 0), int(current.get("completed_model_calls") or 0)),
        "embedded_items": max(int(previous.get("embedded_items") or 0), int(current.get("embedded_items") or 0)),
        "durable_writes": max(int(previous.get("durable_writes") or 0), int(current.get("durable_writes") or 0)),
    }


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value


async def _aiter(value):
    value = await _maybe_await(value)
    if hasattr(value, "__aiter__"):
        async for item in value:
            yield item
        return
    for item in value:
        yield item


def _create_item(container: Any, document: dict[str, Any], *, partition_key: str):
    try:
        return container.create_item(document)
    except TypeError:
        return container.create_item(document, partition_key=partition_key)


def _query_items(container: Any, *, query: str, parameters: list[dict[str, Any]]):
    try:
        return container.query_items(query=query, parameters=parameters, enable_cross_partition_query=True)
    except TypeError:
        return container.query_items(query=query, parameters=parameters)


async def _conditional_replace(container: Any, item: dict[str, Any], *, partition_key: str, etag: str | None):
    if hasattr(container, "replace_item"):
        from azure.core import MatchConditions

        try:
            return await _maybe_await(
                container.replace_item(
                    item=item["id"],
                    body=item,
                    etag=etag,
                    match_condition=MatchConditions.IfNotModified,
                )
            )
        except TypeError:
            return await _maybe_await(
                container.replace_item(
                    item=item["id"],
                    body=item,
                    partition_key=partition_key,
                    etag=etag,
                    match_condition=MatchConditions.IfNotModified,
                )
            )
    return await _maybe_await(container.upsert_item(item))


def _assert_lease(item: dict[str, Any], *, owner_id: str, fencing_token: int) -> None:
    if item.get("lease_owner_id") != owner_id or int(item.get("fencing_token") or 0) != int(fencing_token):
        raise JobClaimError("Job lease fencing token mismatch.")


def _is_cosmos_conflict(exc: Exception) -> bool:
    if exc.__class__.__name__ == "CosmosResourceExistsError":
        return True
    return getattr(exc, "status_code", None) == 409


def _is_cosmos_not_found(exc: Exception) -> bool:
    if exc.__class__.__name__ == "CosmosResourceNotFoundError":
        return True
    return getattr(exc, "status_code", None) == 404


def _is_precondition_failed(exc: Exception) -> bool:
    return getattr(exc, "status_code", None) == 412 or exc.__class__.__name__ == "CosmosAccessConditionFailedError"
