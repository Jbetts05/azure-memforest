"""Service Bus write-job consumer foundation."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Protocol

from src.cloud.cosmos_jobs import CosmosJobStore, JobClaimError, JobNotFound
from src.cloud.schema import SCHEMA_VERSION
from src.service.jobs import WriteJobEnvelope

log = logging.getLogger(__name__)
FINAL_DELIVERY_COUNT = int(os.getenv("AZURE_MEMFOREST_FINAL_DELIVERY_COUNT", "4"))


class PermanentHandlerError(Exception):
    pass


class TransientHandlerError(Exception):
    def __init__(self, message: str, *, exit_code: int | None = None) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class TerminalHandlerError(Exception):
    def __init__(self, message: str, *, reason: str = "terminal_handler_error") -> None:
        super().__init__(message)
        self.reason = reason


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
        settle_before_handle: bool = False,
    ) -> None:
        self._job_store = job_store
        self._handler = handler
        self._owner_id = owner_id
        self._lease_ttl_seconds = lease_ttl_seconds
        self._settle_before_handle = settle_before_handle

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

        message_settled = False
        if self._settle_before_handle:
            await _complete(message)
            message_settled = True

        try:
            await self._handler.handle(envelope, claimed)
        except TerminalHandlerError as exc:
            log.warning(
                "worker_terminal_handler_error",
                extra={"job_id": envelope.job_id, "reason": exc.reason, "delivery_count": getattr(message, "delivery_count", None)},
            )
            try:
                await self._job_store.fail_job(
                    tenant_id=envelope.tenant_id,
                    user_id=envelope.user_id,
                    job_id=envelope.job_id,
                    owner_id=self._owner_id,
                    fencing_token=int(claimed["fencing_token"]),
                    error=f"{exc.reason}: {exc}",
                )
                if not message_settled:
                    await _complete(message)
                return WorkerOutcome(action="complete", reason=exc.reason)
            except Exception:
                if not message_settled:
                    await _abandon(message)
                return WorkerOutcome(action="abandon", reason="fail_terminal_job_failed")
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
                if not message_settled:
                    await _dead_letter(message, reason="permanent_handler_error", description=str(exc))
                return WorkerOutcome(action="dead_letter", reason="permanent_handler_error")
            except Exception as exc:
                log.exception(
                    "worker_fail_job_failed",
                    extra={"job_id": envelope.job_id, "delivery_count": getattr(message, "delivery_count", None)},
                )
                if _is_final_delivery(message):
                    if not message_settled:
                        await _dead_letter(message, reason="fail_job_failed", description=f"{type(exc).__name__}: {exc}")
                    return WorkerOutcome(action="dead_letter", reason="fail_job_failed")
                if not message_settled:
                    await _abandon(message)
                return WorkerOutcome(action="abandon", reason="fail_job_failed")
        except TransientHandlerError as exc:
            log.warning(
                "worker_transient_handler_error",
                exc_info=True,
                extra={"job_id": envelope.job_id, "delivery_count": getattr(message, "delivery_count", None)},
            )
            if message_settled:
                await self._fail_settled_job(envelope, claimed, reason="transient_handler_error", error=str(exc))
                if exc.exit_code is not None:
                    log.critical(
                        "worker_exiting_after_settled_transient_error",
                        extra={"job_id": envelope.job_id, "exit_code": exc.exit_code},
                    )
                    _exit_process(exc.exit_code)
                return WorkerOutcome(action="complete", reason="transient_handler_error")
            if _is_final_delivery(message):
                if not message_settled:
                    await self._fail_and_dead_letter_final(message, envelope, claimed, reason="transient_handler_error", error=str(exc))
                return WorkerOutcome(action="dead_letter", reason="transient_handler_error")
            if not message_settled:
                await _abandon(message)
            return WorkerOutcome(action="abandon", reason="transient_handler_error")
        except Exception as exc:
            log.exception(
                "worker_unhandled_handler_error",
                extra={"job_id": envelope.job_id, "delivery_count": getattr(message, "delivery_count", None)},
            )
            if message_settled:
                await self._fail_settled_job(
                    envelope,
                    claimed,
                    reason="unhandled_handler_error",
                    error=f"{type(exc).__name__}: {exc}",
                )
                return WorkerOutcome(action="complete", reason="unhandled_handler_error")
            if _is_final_delivery(message):
                if not message_settled:
                    await self._fail_and_dead_letter_final(message, envelope, claimed, reason="unhandled_handler_error", error=f"{type(exc).__name__}: {exc}")
                return WorkerOutcome(action="dead_letter", reason="unhandled_handler_error")
            if not message_settled:
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
            if not message_settled:
                await _complete(message)
            return WorkerOutcome(action="complete", reason="succeeded")
        except Exception as exc:
            log.exception(
                "worker_complete_job_failed",
                extra={"job_id": envelope.job_id, "delivery_count": getattr(message, "delivery_count", None)},
            )
            if _is_final_delivery(message):
                if not message_settled:
                    await _dead_letter(message, reason="complete_job_failed", description=f"{type(exc).__name__}: {exc}")
                return WorkerOutcome(action="dead_letter", reason="complete_job_failed")
            if not message_settled:
                await _abandon(message)
            return WorkerOutcome(action="abandon", reason="complete_job_failed")

    async def reap_stalled_jobs(self) -> int:
        failed = await self._job_store.fail_stalled_jobs()
        return len(failed)

    async def _fail_and_dead_letter_final(
        self,
        message: Any,
        envelope: WriteJobEnvelope,
        claimed: dict[str, Any],
        *,
        reason: str,
        error: str,
    ) -> None:
        try:
            await self._job_store.fail_job(
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                job_id=envelope.job_id,
                owner_id=self._owner_id,
                fencing_token=int(claimed["fencing_token"]),
                error=error or reason,
            )
        except Exception:
            log.exception(
                "worker_fail_job_failed_on_final_delivery",
                extra={"job_id": envelope.job_id, "delivery_count": getattr(message, "delivery_count", None)},
            )
        await _dead_letter(message, reason=reason, description=error or reason)

    async def _fail_settled_job(
        self,
        envelope: WriteJobEnvelope,
        claimed: dict[str, Any],
        *,
        reason: str,
        error: str,
    ) -> None:
        try:
            await self._job_store.fail_job(
                tenant_id=envelope.tenant_id,
                user_id=envelope.user_id,
                job_id=envelope.job_id,
                owner_id=self._owner_id,
                fencing_token=int(claimed["fencing_token"]),
                error=error or reason,
            )
        except Exception:
            log.exception(
                "worker_fail_settled_job_failed",
                extra={"job_id": envelope.job_id, "reason": reason},
            )


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
    if payload["job_type"] not in {"ingest_session", "delete_session", "clear_memory"}:
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


def _is_final_delivery(message: Any) -> bool:
    try:
        return int(getattr(message, "delivery_count", 0)) >= FINAL_DELIVERY_COUNT
    except (TypeError, ValueError):
        return False


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


def _exit_process(exit_code: int) -> None:
    os._exit(exit_code)


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value
