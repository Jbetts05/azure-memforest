"""Session-aware Service Bus receive loop for write workers."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from src.worker.consumer import ServiceBusWriteWorker, WorkerOutcome

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WorkerReceiverConfig:
    queue_name: str
    session_accept_wait_seconds: float = 5.0
    message_wait_seconds: float = 5.0
    max_messages_per_session: int = 100
    session_lock_renewal_seconds: float = 300.0
    idle_sleep_seconds: float = 1.0


@dataclass(frozen=True, slots=True)
class ReceiverLoopOutcome:
    action: str
    processed_count: int = 0
    reason: str = ""


class SessionMessageAdapter:
    """Adds receiver-backed settlement methods to Azure Service Bus messages."""

    def __init__(self, *, receiver: Any, message: Any) -> None:
        self._receiver = receiver
        self._message = message

    @property
    def body(self) -> Any:
        return getattr(self._message, "body", b"")

    @property
    def message_id(self) -> str:
        return str(getattr(self._message, "message_id", ""))

    @property
    def delivery_count(self) -> int | None:
        value = getattr(self._message, "delivery_count", None)
        return int(value) if value is not None else None

    async def complete(self) -> None:
        await _maybe_await(self._receiver.complete_message(self._message))

    async def abandon(self) -> None:
        await _maybe_await(self._receiver.abandon_message(self._message))

    async def dead_letter(self, *, reason: str, error_description: str) -> None:
        await _maybe_await(
            self._receiver.dead_letter_message(
                self._message,
                reason=reason,
                error_description=error_description,
            )
        )


class ServiceBusSessionReceiverLoop:
    def __init__(
        self,
        *,
        servicebus_client: Any,
        worker: ServiceBusWriteWorker,
        config: WorkerReceiverConfig,
        lock_renewer: Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if config.session_lock_renewal_seconds <= config.session_accept_wait_seconds:
            raise ValueError("session_lock_renewal_seconds must exceed session_accept_wait_seconds.")
        if config.message_wait_seconds <= 0:
            raise ValueError("message_wait_seconds must be positive.")
        if config.max_messages_per_session <= 0:
            raise ValueError("max_messages_per_session must be positive.")
        self._client = servicebus_client
        self._worker = worker
        self._config = config
        self._lock_renewer = lock_renewer
        self._log = logger or log

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        while stop_event is None or not stop_event.is_set():
            outcome = await self.process_next_session(stop_event=stop_event)
            if outcome.action in {"idle", "accept_error"}:
                await _sleep_or_stop(self._config.idle_sleep_seconds, stop_event)

    async def process_next_session(self, *, stop_event: asyncio.Event | None = None) -> ReceiverLoopOutcome:
        try:
            receiver = await _maybe_await(
                self._client.accept_next_session(
                    queue_name=self._config.queue_name,
                    max_wait_time=self._config.session_accept_wait_seconds,
                )
            )
        except Exception as exc:
            if _is_operation_timeout(exc):
                return ReceiverLoopOutcome(action="idle", reason="no_session_available")
            self._log.warning("worker_session_accept_failed", extra={"error_class": exc.__class__.__name__})
            return ReceiverLoopOutcome(action="accept_error", reason=exc.__class__.__name__)

        if receiver is None:
            return ReceiverLoopOutcome(action="idle", reason="no_session_available")

        processed = 0
        async with receiver:
            await self._register_session_lock_renewal(receiver)
            while processed < self._config.max_messages_per_session:
                if stop_event is not None and stop_event.is_set():
                    break
                messages = await _maybe_await(
                    receiver.receive_messages(
                        max_message_count=1,
                        max_wait_time=self._config.message_wait_seconds,
                    )
                )
                if not messages:
                    break
                message = messages[0]
                outcome = await self._process_message(receiver, message)
                processed += 1
                if outcome.action == "abandon":
                    break
        return ReceiverLoopOutcome(action="processed", processed_count=processed)

    async def close(self) -> None:
        if self._lock_renewer is not None and hasattr(self._lock_renewer, "close"):
            await _maybe_await(self._lock_renewer.close())
        if hasattr(self._client, "close"):
            await _maybe_await(self._client.close())

    async def _register_session_lock_renewal(self, receiver: Any) -> None:
        if self._lock_renewer is None:
            return
        session = getattr(receiver, "session", None)
        if session is None:
            return
        await _maybe_await(
            self._lock_renewer.register(
                receiver,
                session,
                max_lock_renewal_duration=self._config.session_lock_renewal_seconds,
            )
        )

    async def _process_message(self, receiver: Any, message: Any) -> WorkerOutcome:
        started = time.perf_counter()
        adapter = SessionMessageAdapter(receiver=receiver, message=message)
        outcome = await self._worker.process_message(adapter)
        self._log.info(
            "worker_message_processed",
            extra={
                "session_id": _safe_str(getattr(getattr(receiver, "session", None), "session_id", "")),
                "message_id": adapter.message_id,
                "delivery_count": adapter.delivery_count,
                "worker_action": outcome.action,
                "worker_reason": outcome.reason,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            },
        )
        return outcome


async def create_azure_lock_renewer() -> Any:
    from azure.servicebus.aio import AutoLockRenewer

    return AutoLockRenewer()


async def _sleep_or_stop(seconds: float, stop_event: asyncio.Event | None) -> None:
    if stop_event is None:
        await asyncio.sleep(seconds)
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        return


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _is_operation_timeout(exc: Exception) -> bool:
    if exc.__class__.__name__ == "OperationTimeoutError":
        return True
    text = f"{exc.__class__.__name__}: {exc}".lower()
    if "operationtimeout" in text or "operation timeout" in text or "timed out" in text or "timeout" in text:
        return True
    inner = getattr(exc, "__cause__", None)
    if inner is None:
        return False
    inner_text = f"{inner.__class__.__name__}: {inner}".lower()
    return "operationtimeout" in inner_text or "operation timeout" in inner_text or "timed out" in inner_text or "timeout" in inner_text


def _safe_str(value: Any) -> str:
    return str(value or "")
