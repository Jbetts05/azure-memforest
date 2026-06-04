"""Durable Functions activity runner for write materialization."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.cloud.cosmos_jobs import CosmosJobStore, JobClaimError, JobNotFound
from src.worker.consumer import (
    PermanentHandlerError,
    TerminalHandlerError,
    TransientHandlerError,
    WriteJobHandler,
    parse_write_job_envelope,
)

if TYPE_CHECKING:
    from src.service.jobs import WriteJobEnvelope


@dataclass(frozen=True, slots=True)
class DurableActivityResult:
    job_id: str
    job_type: str
    tenant_id: str
    user_id: str
    session_id: str
    status: str
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "status": self.status,
            "reason": self.reason,
        }


class DurableWriteActivityRunner:
    def __init__(
        self,
        *,
        job_store: CosmosJobStore,
        handler: WriteJobHandler,
        owner_id: str,
        lease_ttl_seconds: int = 300,
    ) -> None:
        self._job_store = job_store
        self._handler = handler
        self._owner_id = owner_id
        self._lease_ttl_seconds = lease_ttl_seconds

    async def run(self, payload: dict[str, Any] | str | bytes) -> dict[str, Any]:
        envelope = _parse_payload(payload)
        try:
            claimed = await self._job_store.claim_job(
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                job_id=envelope.job_id,
                owner_id=self._owner_id,
                ttl_seconds=self._lease_ttl_seconds,
            )
        except JobClaimError:
            terminal = await self._terminal_result(envelope)
            if terminal is not None:
                return terminal
            raise
        fencing_token = int(claimed["fencing_token"])
        if envelope.job_type != "ingest_session":
            await self._fail_if_running(envelope, fencing_token=fencing_token, error="unsupported_job_type")
            return _result(envelope, status="failed", reason="unsupported_job_type")
        try:
            await self._handler.handle(envelope, claimed)
        except TerminalHandlerError as exc:
            await self._fail_if_running(envelope, fencing_token=fencing_token, error=f"{exc.reason}: {exc}")
            return _result(envelope, status="failed", reason=exc.reason)
        except PermanentHandlerError as exc:
            await self._fail_if_running(envelope, fencing_token=fencing_token, error=str(exc))
            return _result(envelope, status="failed", reason="permanent_handler_error")
        except TransientHandlerError as exc:
            await self._fail_if_running(envelope, fencing_token=fencing_token, error=str(exc))
            return _result(envelope, status="failed", reason="transient_handler_error")
        await self._job_store.complete_job(
            tenant_id=envelope.tenant_id,
            user_id=envelope.user_id,
            job_id=envelope.job_id,
            owner_id=self._owner_id,
            fencing_token=fencing_token,
        )
        return _result(envelope, status="succeeded")

    async def fail_after_retries(self, payload: dict[str, Any] | str | bytes, *, reason: str) -> dict[str, Any]:
        envelope = _parse_payload(payload)
        try:
            claimed = await self._job_store.claim_job(
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                job_id=envelope.job_id,
                owner_id=self._owner_id,
                ttl_seconds=self._lease_ttl_seconds,
            )
        except (JobClaimError, JobNotFound):
            terminal = await self._terminal_result(envelope)
            if terminal is not None:
                return terminal
            return _result(envelope, status="failed", reason=reason)
        await self._fail_if_running(envelope, fencing_token=int(claimed["fencing_token"]), error=reason)
        return _result(envelope, status="failed", reason=reason)

    async def _fail_if_running(self, envelope: WriteJobEnvelope, *, fencing_token: int, error: str) -> None:
        try:
            await self._job_store.fail_job(
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                job_id=envelope.job_id,
                owner_id=self._owner_id,
                fencing_token=fencing_token,
                error=error,
            )
        except JobClaimError:
            return

    async def _terminal_result(self, envelope: WriteJobEnvelope) -> dict[str, Any] | None:
        get_job = getattr(self._job_store, "get_job_by_ids", None)
        if get_job is None:
            return None
        try:
            job = await get_job(tenant_id=envelope.tenant_id, user_id=envelope.user_id, job_id=envelope.job_id)
        except JobNotFound:
            return None
        status = str(_job_value(job, "status") or "")
        if status == "succeeded":
            return _result(envelope, status="succeeded")
        if status == "failed":
            return _result(envelope, status="failed", reason=str(_job_value(job, "error") or "failed"))
        return None


async def materialize_write_job_from_env(activity_input: dict[str, Any]) -> dict[str, Any]:
    from src.worker.factory import build_scoped_materialization_handler_from_env

    envelope_payload = activity_input.get("envelope", activity_input)
    owner_id = _owner_id(activity_input)
    runtime = build_scoped_materialization_handler_from_env(os.environ, owner_id=owner_id)
    runner = DurableWriteActivityRunner(
        job_store=runtime.job_store,
        handler=runtime.handler,
        owner_id=owner_id,
        lease_ttl_seconds=int(os.getenv("AZURE_MEMFOREST_DURABLE_JOB_LEASE_TTL_SECONDS", "300")),
    )
    try:
        return await runner.run(envelope_payload)
    finally:
        await _close_runtime(runtime)


async def fail_write_job_from_env(activity_input: dict[str, Any]) -> dict[str, Any]:
    from src.worker.factory import build_scoped_materialization_handler_from_env

    envelope_payload = activity_input.get("envelope", activity_input)
    owner_id = _owner_id(activity_input)
    reason = str(activity_input.get("reason") or "durable_activity_failed")
    runtime = build_scoped_materialization_handler_from_env(os.environ, owner_id=owner_id)
    runner = DurableWriteActivityRunner(
        job_store=runtime.job_store,
        handler=runtime.handler,
        owner_id=owner_id,
        lease_ttl_seconds=int(os.getenv("AZURE_MEMFOREST_DURABLE_JOB_LEASE_TTL_SECONDS", "300")),
    )
    try:
        return await runner.fail_after_retries(envelope_payload, reason=reason)
    finally:
        await _close_runtime(runtime)


def _parse_payload(payload: dict[str, Any] | str | bytes) -> WriteJobEnvelope:
    if isinstance(payload, dict):
        return parse_write_job_envelope(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return parse_write_job_envelope(payload)


def _owner_id(activity_input: dict[str, Any]) -> str:
    owner_id = str(activity_input.get("owner_id") or activity_input.get("instance_id") or "").strip()
    if not owner_id:
        raise ValueError("Durable activity input requires owner_id or instance_id")
    return owner_id


def _result(envelope: WriteJobEnvelope, *, status: str, reason: str = "") -> dict[str, Any]:
    return DurableActivityResult(
        job_id=envelope.job_id,
        job_type=envelope.job_type,
        tenant_id=envelope.tenant_id,
        user_id=envelope.user_id,
        session_id=envelope.session_id,
        status=status,
        reason=reason,
    ).as_dict()


def _job_value(job: Any, key: str) -> Any:
    if isinstance(job, dict):
        return job.get(key)
    return getattr(job, key, None)


async def _close_runtime(runtime: Any) -> None:
    for closeable in runtime.closeables:
        close = getattr(closeable, "close", None)
        if close is not None:
            value = close()
            if hasattr(value, "__await__"):
                await value
