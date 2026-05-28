"""Service Bus write-job consumer foundation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from src.cloud.cosmos_jobs import CosmosJobStore, JobClaimError, JobNotFound
from src.cloud.schema import SCHEMA_VERSION
from src.service.jobs import WriteJobEnvelope


class PermanentHandlerError(Exception):
    pass


class TransientHandlerError(Exception):
    pass


class WriteJobHandler(Protocol):
    async def handle(self, envelope: WriteJobEnvelope, claimed_job: dict[str, Any]) -> None:
        ...


class NotImplementedJobHandler:
    async def handle(self, envelope: WriteJobEnvelope, claimed_job: dict[str, Any]) -> None:
        raise PermanentHandlerError("write job handler is not configured")


@dataclass(frozen=True)
class WorkerOutcome:
    action: str
    reason: str = ""


class ServiceBusWriteWorker:
    """Processes individual write-job messages.

    The receive loop, session lock renewal, and Container Apps worker hosting
    are intentionally not wired yet. This class owns the idempotent
    message-level semantics used by the future receiver.
    """

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

    async def process_message(self, message: Any) -> WorkerOutcome:
        try:
            envelope = parse_write_job_envelope(_message_body(message))
        except ValueError as exc:
            await _dead_letter(message, reason="invalid_envelope", description=str(exc))
            return WorkerOutcome(action="dead_letter", reason="invalid_envelope")

        try:
            claimed = await self._job_store.claim_job(
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                job_id=envelope.job_id,
                owner_id=self._owner_id,
                ttl_seconds=self._lease_ttl_seconds,
            )
        except JobNotFound:
            await _dead_letter(message, reason="job_not_found", description="job status record not found")
            return WorkerOutcome(action="dead_letter", reason="job_not_found")
        except JobClaimError as exc:
            if "state" in str(exc):
                await _complete(message)
                return WorkerOutcome(action="complete", reason="terminal_or_invalid_state")
            await _abandon(message)
            return WorkerOutcome(action="abandon", reason="claim_failed")

        try:
            await self._handler.handle(envelope, claimed)
        except PermanentHandlerError as exc:
            try:
                await self._job_store.fail_job(
                    tenant_id=envelope.tenant_id,
                    user_id=envelope.user_id,
                    job_id=envelope.job_id,
                    owner_id=self._owner_id,
                    fencing_token=int(claimed["fencing_token"]),
                    error=str(exc),
                )
                await _dead_letter(message, reason="permanent_handler_error", description=str(exc))
                return WorkerOutcome(action="dead_letter", reason="permanent_handler_error")
            except Exception:
                await _abandon(message)
                return WorkerOutcome(action="abandon", reason="fail_job_failed")
        except TransientHandlerError:
            await _abandon(message)
            return WorkerOutcome(action="abandon", reason="transient_handler_error")
        except Exception:
            await _abandon(message)
            return WorkerOutcome(action="abandon", reason="unhandled_handler_error")

        try:
            await self._job_store.complete_job(
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                job_id=envelope.job_id,
                owner_id=self._owner_id,
                fencing_token=int(claimed["fencing_token"]),
            )
            await _complete(message)
            return WorkerOutcome(action="complete", reason="succeeded")
        except Exception:
            await _abandon(message)
            return WorkerOutcome(action="abandon", reason="complete_job_failed")


def parse_write_job_envelope(body: bytes | str) -> WriteJobEnvelope:
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else str(body))
    required = {
        "schema_version",
        "job_id",
        "job_type",
        "tenant_id",
        "user_id",
        "session_id",
        "idempotency_key",
        "submitted_at_unix",
        "payload",
    }
    missing = sorted(required - set(payload))
    if missing:
        raise ValueError(f"missing fields: {missing}")
    if int(payload["schema_version"]) != SCHEMA_VERSION:
        raise ValueError("unsupported schema_version")
    if payload["job_type"] not in {"ingest_session", "delete_session"}:
        raise ValueError("unsupported job_type")
    return WriteJobEnvelope(
        schema_version=int(payload["schema_version"]),
        job_id=str(payload["job_id"]),
        job_type=payload["job_type"],
        tenant_id=str(payload["tenant_id"]),
        user_id=str(payload["user_id"]),
        session_id=str(payload["session_id"]),
        idempotency_key=str(payload["idempotency_key"]),
        submitted_at_unix=float(payload["submitted_at_unix"]),
        payload=dict(payload["payload"]),
    )


def _message_body(message: Any) -> bytes:
    if hasattr(message, "body"):
        body = message.body
        if callable(body):
            body = body()
        if isinstance(body, bytes):
            return body
        if isinstance(body, str):
            return body.encode("utf-8")
        try:
            return b"".join(bytes(chunk) for chunk in body)
        except TypeError:
            pass
    return bytes(message)


async def _complete(message: Any) -> None:
    if hasattr(message, "complete"):
        await _maybe_await(message.complete())


async def _abandon(message: Any) -> None:
    if hasattr(message, "abandon"):
        await _maybe_await(message.abandon())


async def _dead_letter(message: Any, *, reason: str, description: str) -> None:
    if hasattr(message, "dead_letter"):
        await _maybe_await(
            message.dead_letter(
                reason=reason,
                error_description=description,
            )
        )


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value
