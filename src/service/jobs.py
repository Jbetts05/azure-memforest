"""Service Bus write job publishing for Azure-MemForest."""

from __future__ import annotations

import hashlib
import json
import time
import asyncio
from dataclasses import asdict, dataclass
from typing import Any, Literal

from src.cloud.schema import SCHEMA_VERSION, partition_key
from src.service.ports import MemoryCommandService
from src.service.schemas import (
    ClearMemoryResponse,
    DeleteSessionResponse,
    IngestSessionRequest,
    IngestSessionResponse,
)

JobType = Literal["ingest_session", "delete_session", "clear_memory"]
MAX_SERVICE_BUS_MESSAGE_BYTES = 240 * 1024
MAX_DURABLE_START_PAYLOAD_BYTES = 60 * 1024
DURABLE_START_TIMEOUT_SECONDS = 60
DURABLE_START_ATTEMPTS = 3


@dataclass(frozen=True, slots=True)
class WriteJobEnvelope:
    schema_version: int
    job_id: str
    job_type: JobType
    tenant_id: str
    user_id: str
    session_id: str
    idempotency_key: str
    submitted_at_unix: float
    payload: dict[str, Any]

    @property
    def session_key(self) -> str:
        return partition_key(tenant_id=self.tenant_id, user_id=self.user_id)

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))


class ServiceBusJobPublisher:
    def __init__(
        self,
        *,
        fully_qualified_namespace: str,
        queue_name: str,
        client_id: str,
        servicebus_client: Any | None = None,
    ) -> None:
        self.fully_qualified_namespace = fully_qualified_namespace
        self.queue_name = queue_name
        self.client_id = client_id
        self._client = servicebus_client
        self._sender = None
        self._credential = None
        self._lock = asyncio.Lock()

    async def submit_job(self, envelope: WriteJobEnvelope) -> None:
        body = envelope.to_json()
        if len(body.encode("utf-8")) > MAX_SERVICE_BUS_MESSAGE_BYTES:
            raise ValueError("write job envelope exceeds Service Bus Standard message size budget.")
        sender = await self._sender_for_queue()
        from azure.servicebus import ServiceBusMessage

        message = ServiceBusMessage(
            body,
            content_type="application/json",
            session_id=envelope.session_key,
            message_id=envelope.job_id,
            application_properties={
                "schema_version": envelope.schema_version,
                "job_id": envelope.job_id,
                "tenant_id": envelope.tenant_id,
                "user_id": envelope.user_id,
                "job_type": envelope.job_type,
                "idempotency_key": envelope.idempotency_key,
            },
        )
        await sender.send_messages(message)

    async def close(self) -> None:
        if self._sender is not None:
            await self._sender.close()
        if self._client is not None and hasattr(self._client, "close"):
            await self._client.close()
        if self._credential is not None and hasattr(self._credential, "close"):
            await self._credential.close()

    async def _sender_for_queue(self):
        if self._sender is not None:
            return self._sender
        async with self._lock:
            if self._sender is None:
                self._sender = self._client_for_namespace().get_queue_sender(queue_name=self.queue_name)
            return self._sender

    def _client_for_namespace(self):
        if self._client is not None:
            return self._client
        from azure.identity.aio import ManagedIdentityCredential
        from azure.servicebus.aio import ServiceBusClient

        credential = ManagedIdentityCredential(client_id=self.client_id)
        self._credential = credential
        self._client = ServiceBusClient(
            fully_qualified_namespace=self.fully_qualified_namespace,
            credential=credential,
        )
        return self._client


class ServiceBusMemoryCommandService(MemoryCommandService):
    def __init__(self, publisher: ServiceBusJobPublisher) -> None:
        self._publisher = publisher

    async def close(self) -> None:
        await self._publisher.close()

    async def submit_ingest_session(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        session_id: str,
        request: IngestSessionRequest,
        idempotency_key: str,
    ) -> IngestSessionResponse:
        envelope = build_write_job_envelope(
            actor,
            user_id=user_id,
            session_id=session_id,
            job_type="ingest_session",
            idempotency_key=idempotency_key,
            payload=request.model_dump(mode="json"),
        )
        await self._publisher.submit_job(envelope)
        return IngestSessionResponse(
            job_id=envelope.job_id,
            tenant_id=actor.tenant.tenant_id,
            user_id=user_id,
            session_id=session_id,
        )

    async def submit_delete_session(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        session_id: str,
        idempotency_key: str,
    ) -> DeleteSessionResponse:
        envelope = build_write_job_envelope(
            actor,
            user_id=user_id,
            session_id=session_id,
            job_type="delete_session",
            idempotency_key=idempotency_key,
            payload={},
        )
        await self._publisher.submit_job(envelope)
        return DeleteSessionResponse(
            job_id=envelope.job_id,
            tenant_id=actor.tenant.tenant_id,
            user_id=user_id,
            session_id=session_id,
        )

    async def submit_clear_memory(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        idempotency_key: str,
    ) -> ClearMemoryResponse:
        envelope = build_write_job_envelope(
            actor,
            user_id=user_id,
            session_id="__all__",
            job_type="clear_memory",
            idempotency_key=idempotency_key,
            payload={},
        )
        await self._publisher.submit_job(envelope)
        return ClearMemoryResponse(
            job_id=envelope.job_id,
            tenant_id=actor.tenant.tenant_id,
            user_id=user_id,
        )


class DurableJobStarter:
        def __init__(self, *, start_url: str, bearer_token: str = "", http_client: Any | None = None) -> None:
            self.start_url = start_url
            self.bearer_token = bearer_token
            self._http_client = http_client

        async def submit_job(self, envelope: WriteJobEnvelope) -> None:
            payload = json.loads(envelope.to_json())
            payload["instance_id"] = envelope.job_id
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            if len(body.encode("utf-8")) > MAX_DURABLE_START_PAYLOAD_BYTES:
                raise ValueError("write job envelope exceeds Durable start payload size budget.")
            headers = {"content-type": "application/json"}
            headers["x-ms-durable-instance-id"] = envelope.job_id
            if self.bearer_token:
                headers["x-functions-key"] = self.bearer_token
            if self._http_client is not None:
                response = await self._post_with_retries(self._http_client, payload, headers)
                _raise_for_status(response)
                return
            import httpx

            async with httpx.AsyncClient(timeout=DURABLE_START_TIMEOUT_SECONDS) as client:
                response = await self._post_with_retries(client, payload, headers)
                _raise_for_status(response)

        async def _post_with_retries(self, client: Any, payload: dict[str, Any], headers: dict[str, str]) -> Any:
            last_exc: Exception | None = None
            for attempt in range(1, DURABLE_START_ATTEMPTS + 1):
                try:
                    return await _maybe_await(
                        client.post(
                            self.start_url,
                            json=payload,
                            headers=headers,
                            timeout=DURABLE_START_TIMEOUT_SECONDS,
                        )
                    )
                except Exception as exc:
                    if not _is_transient_durable_start_error(exc) or attempt >= DURABLE_START_ATTEMPTS:
                        raise
                    last_exc = exc
                    await asyncio.sleep(min(2 ** (attempt - 1), 5))
            if last_exc is not None:
                raise last_exc
            raise RuntimeError("durable job start failed")

        async def close(self) -> None:
            close = getattr(self._http_client, "aclose", None) or getattr(self._http_client, "close", None)
            if close is not None:
                await _maybe_await(close())


class DurableMemoryCommandService(MemoryCommandService):
        def __init__(self, starter: DurableJobStarter) -> None:
            self._starter = starter

        async def close(self) -> None:
            await self._starter.close()

        async def submit_ingest_session(
            self,
            actor: ActorContext,
            *,
            user_id: str,
            session_id: str,
            request: IngestSessionRequest,
            idempotency_key: str,
        ) -> IngestSessionResponse:
            envelope = build_write_job_envelope(
                actor,
                user_id=user_id,
                session_id=session_id,
                job_type="ingest_session",
                idempotency_key=idempotency_key,
                payload=request.model_dump(mode="json"),
            )
            await self._starter.submit_job(envelope)
            return IngestSessionResponse(job_id=envelope.job_id, tenant_id=actor.tenant.tenant_id, user_id=user_id, session_id=session_id)

        async def submit_delete_session(
            self,
            actor: ActorContext,
            *,
            user_id: str,
            session_id: str,
            idempotency_key: str,
        ) -> DeleteSessionResponse:
            envelope = build_write_job_envelope(
                actor,
                user_id=user_id,
                session_id=session_id,
                job_type="delete_session",
                idempotency_key=idempotency_key,
                payload={},
            )
            await self._starter.submit_job(envelope)
            return DeleteSessionResponse(job_id=envelope.job_id, tenant_id=actor.tenant.tenant_id, user_id=user_id, session_id=session_id)

        async def submit_clear_memory(
            self,
            actor: ActorContext,
            *,
            user_id: str,
            idempotency_key: str,
        ) -> ClearMemoryResponse:
            envelope = build_write_job_envelope(
                actor,
                user_id=user_id,
                session_id="__all__",
                job_type="clear_memory",
                idempotency_key=idempotency_key,
                payload={},
            )
            await self._starter.submit_job(envelope)
            return ClearMemoryResponse(job_id=envelope.job_id, tenant_id=actor.tenant.tenant_id, user_id=user_id)


class DurableIngestMemoryCommandService(MemoryCommandService):
        def __init__(self, durable: DurableMemoryCommandService, fallback: MemoryCommandService) -> None:
            self._durable = durable
            self._fallback = fallback

        async def close(self) -> None:
            await self._durable.close()
            close = getattr(self._fallback, "close", None)
            if close is not None:
                await _maybe_await(close())

        async def submit_ingest_session(
            self,
            actor: ActorContext,
            *,
            user_id: str,
            session_id: str,
            request: IngestSessionRequest,
            idempotency_key: str,
        ) -> IngestSessionResponse:
            return await self._durable.submit_ingest_session(
                actor,
                user_id=user_id,
                session_id=session_id,
                request=request,
                idempotency_key=idempotency_key,
            )

        async def submit_delete_session(
            self,
            actor: ActorContext,
            *,
            user_id: str,
            session_id: str,
            idempotency_key: str,
        ) -> DeleteSessionResponse:
            return await self._fallback.submit_delete_session(
                actor,
                user_id=user_id,
                session_id=session_id,
                idempotency_key=idempotency_key,
            )

        async def submit_clear_memory(
            self,
            actor: ActorContext,
            *,
            user_id: str,
            idempotency_key: str,
        ) -> ClearMemoryResponse:
            return await self._fallback.submit_clear_memory(actor, user_id=user_id, idempotency_key=idempotency_key)


def build_write_job_envelope(
    actor: ActorContext,
    *,
    user_id: str,
    session_id: str,
    job_type: JobType,
    idempotency_key: str,
    payload: dict[str, Any],
) -> WriteJobEnvelope:
    job_id = _job_id(
        tenant_id=actor.tenant.tenant_id,
        user_id=user_id,
        session_id=session_id,
        job_type=job_type,
        idempotency_key=idempotency_key,
    )
    return WriteJobEnvelope(
        schema_version=SCHEMA_VERSION,
        job_id=job_id,
        job_type=job_type,
        tenant_id=actor.tenant.tenant_id,
        user_id=user_id,
        session_id=session_id,
        idempotency_key=idempotency_key,
        submitted_at_unix=time.time(),
        payload=payload,
    )


def service_bus_command_service_from_env(environ: dict[str, str], *, servicebus_client: Any | None = None) -> ServiceBusMemoryCommandService | None:
    namespace = str(environ.get("AZURE_SERVICEBUS_FULLY_QUALIFIED_NAMESPACE", "")).strip()
    queue = str(environ.get("AZURE_SERVICEBUS_WRITES_QUEUE_NAME", "")).strip()
    client_id = str(environ.get("AZURE_CLIENT_ID", "")).strip()
    if not (namespace and queue and client_id):
        return None
    return ServiceBusMemoryCommandService(
        ServiceBusJobPublisher(
            fully_qualified_namespace=namespace,
            queue_name=queue,
            client_id=client_id,
            servicebus_client=servicebus_client,
        )
    )


def durable_command_service_from_env(environ: dict[str, str], *, http_client: Any | None = None) -> DurableMemoryCommandService | None:
    start_url = str(environ.get("AZURE_MEMFOREST_DURABLE_START_URL", "")).strip()
    if not start_url:
        return None
    return DurableMemoryCommandService(
        DurableJobStarter(
            start_url=start_url,
            bearer_token=str(environ.get("AZURE_MEMFOREST_DURABLE_START_TOKEN", "")).strip(),
            http_client=http_client,
        )
    )


def _job_id(
    *,
    tenant_id: str,
    user_id: str,
    session_id: str,
    job_type: str,
    idempotency_key: str,
) -> str:
    raw = json.dumps(
        {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "job_type": job_type,
            "idempotency_key": idempotency_key,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "job_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _raise_for_status(response: Any) -> None:
    status_code = int(getattr(response, "status_code", 200) or 200)
    if status_code == 409:
        return
    if 400 <= status_code:
        raise RuntimeError(f"durable job start failed with status {status_code}")
    raise_for_status = getattr(response, "raise_for_status", None)
    if raise_for_status is not None:
        try:
            raise_for_status()
        except Exception as exc:
            raise RuntimeError("durable job start failed") from None


def _is_transient_durable_start_error(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    return "timeout" in name or isinstance(exc, TimeoutError)


class JobRecordingCommandService(MemoryCommandService):
    """Decorator that records accepted job status after publish succeeds."""

    def __init__(self, inner: MemoryCommandService, job_store: Any) -> None:
        self._inner = inner
        self._job_store = job_store

    async def submit_ingest_session(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        session_id: str,
        request: IngestSessionRequest,
        idempotency_key: str,
    ) -> IngestSessionResponse:
        envelope = build_write_job_envelope(
            actor,
            user_id=user_id,
            session_id=session_id,
            job_type="ingest_session",
            idempotency_key=idempotency_key,
            payload=request.model_dump(mode="json"),
        )
        recorded = await self._job_store.create_accepted_job(envelope)
        if recorded.status != "accepted":
            return IngestSessionResponse(
                job_id=recorded.job_id,
                status=recorded.status,
                tenant_id=recorded.tenant_id,
                user_id=recorded.user_id,
                session_id=session_id,
                progress_budget=recorded.progress_budget,
            )
        await self._inner.submit_ingest_session(
            actor,
            user_id=user_id,
            session_id=session_id,
            request=request,
            idempotency_key=idempotency_key,
        )
        return IngestSessionResponse(
            job_id=recorded.job_id,
            status=recorded.status,
            tenant_id=recorded.tenant_id,
            user_id=recorded.user_id,
            session_id=session_id,
            progress_budget=recorded.progress_budget,
        )

    async def submit_delete_session(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        session_id: str,
        idempotency_key: str,
    ) -> DeleteSessionResponse:
        envelope = build_write_job_envelope(
            actor,
            user_id=user_id,
            session_id=session_id,
            job_type="delete_session",
            idempotency_key=idempotency_key,
            payload={},
        )
        recorded = await self._job_store.create_accepted_job(envelope)
        if recorded.status != "accepted":
            return DeleteSessionResponse(
                job_id=recorded.job_id,
                status=recorded.status,
                tenant_id=recorded.tenant_id,
                user_id=recorded.user_id,
                session_id=session_id,
            )
        await self._inner.submit_delete_session(
            actor,
            user_id=user_id,
            session_id=session_id,
            idempotency_key=idempotency_key,
        )
        return DeleteSessionResponse(
            job_id=recorded.job_id,
            status=recorded.status,
            tenant_id=recorded.tenant_id,
            user_id=recorded.user_id,
            session_id=session_id,
        )

    async def submit_clear_memory(
        self,
        actor: ActorContext,
        *,
        user_id: str,
        idempotency_key: str,
    ) -> ClearMemoryResponse:
        envelope = build_write_job_envelope(
            actor,
            user_id=user_id,
            session_id="__all__",
            job_type="clear_memory",
            idempotency_key=idempotency_key,
            payload={},
        )
        recorded = await self._job_store.create_accepted_job(envelope)
        if recorded.status != "accepted":
            return ClearMemoryResponse(
                job_id=recorded.job_id,
                status=recorded.status,
                tenant_id=recorded.tenant_id,
                user_id=recorded.user_id,
            )
        await self._inner.submit_clear_memory(
            actor,
            user_id=user_id,
            idempotency_key=idempotency_key,
        )
        return ClearMemoryResponse(
            job_id=recorded.job_id,
            status=recorded.status,
            tenant_id=recorded.tenant_id,
            user_id=recorded.user_id,
        )

    async def close(self) -> None:
        close = getattr(self._inner, "close", None)
        if close is not None:
            await close()
