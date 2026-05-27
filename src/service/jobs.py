"""Service Bus write job publishing for Azure-MemForest."""

from __future__ import annotations

import hashlib
import json
import time
import asyncio
from dataclasses import asdict, dataclass
from typing import Any, Literal

from src.cloud.schema import SCHEMA_VERSION, partition_key
from src.service.auth import ActorContext
from src.service.ports import MemoryCommandService
from src.service.schemas import (
    DeleteSessionResponse,
    IngestSessionRequest,
    IngestSessionResponse,
)

JobType = Literal["ingest_session", "delete_session"]
MAX_SERVICE_BUS_MESSAGE_BYTES = 240 * 1024


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
