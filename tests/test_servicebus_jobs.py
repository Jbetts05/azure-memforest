from __future__ import annotations

import pytest

from src.service.auth import ActorContext, TenantContext
from src.service.jobs import (
    DurableJobStarter,
    DurableIngestMemoryCommandService,
    DurableMemoryCommandService,
    ServiceBusJobPublisher,
    ServiceBusMemoryCommandService,
    build_write_job_envelope,
    durable_command_service_from_env,
    service_bus_command_service_from_env,
)
from src.service.schemas import IngestSessionRequest, TurnInput


class FakeSender:
    def __init__(self) -> None:
        self.messages = []
        self.closed = False

    async def send_messages(self, message) -> None:
        self.messages.append(message)

    async def close(self) -> None:
        self.closed = True


class FakeServiceBusClient:
    def __init__(self) -> None:
        self.sender = FakeSender()
        self.closed = False

    def get_queue_sender(self, *, queue_name: str):
        self.queue_name = queue_name
        return self.sender

    async def close(self) -> None:
        self.closed = True


class FakeResponse:
    def __init__(self, status_code=200) -> None:
        self.status_code = status_code
        self.raise_for_status_called = False

    def raise_for_status(self) -> None:
        self.raise_for_status_called = True
        if self.status_code >= 400:
            raise RuntimeError("raw-url https://functions.example/start?code=secret")


class FakeHttpClient:
    def __init__(self) -> None:
        self.posts = []
        self.closed = False
        self.response = FakeResponse()

    async def post(self, url, *, json, headers, timeout):
        self.posts.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return self.response

    async def aclose(self):
        self.closed = True


def _actor() -> ActorContext:
    return ActorContext(
        actor_id="user-1",
        tenant=TenantContext(tenant_id="tenant-1"),
        auth_scheme="test",
    )


def test_write_job_envelope_has_deterministic_id_and_session_key() -> None:
    first = build_write_job_envelope(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": []},
    )
    second = build_write_job_envelope(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": [{"content": "different"}]},
    )

    assert first.job_id == second.job_id
    assert first.session_key == "tenant-1:user-1"
    assert first.schema_version == 1


@pytest.mark.asyncio
async def test_servicebus_publisher_sends_json_message_with_metadata() -> None:
    fake_client = FakeServiceBusClient()
    publisher = ServiceBusJobPublisher(
        fully_qualified_namespace="example.servicebus.windows.net",
        queue_name="memforest-writes",
        client_id="client-id",
        servicebus_client=fake_client,
    )
    envelope = build_write_job_envelope(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        job_type="delete_session",
        idempotency_key="idem-1",
        payload={},
    )

    await publisher.submit_job(envelope)

    assert fake_client.queue_name == "memforest-writes"
    message = fake_client.sender.messages[0]
    assert message.session_id == "tenant-1:user-1"
    assert message.message_id == envelope.job_id
    assert message.content_type == "application/json"
    assert message.application_properties["job_type"] == "delete_session"
    assert message.application_properties["job_id"] == envelope.job_id
    assert message.application_properties["idempotency_key"] == envelope.idempotency_key


@pytest.mark.asyncio
async def test_memory_command_service_returns_accepted_after_publish() -> None:
    fake_client = FakeServiceBusClient()
    service = ServiceBusMemoryCommandService(
        ServiceBusJobPublisher(
            fully_qualified_namespace="example.servicebus.windows.net",
            queue_name="memforest-writes",
            client_id="client-id",
            servicebus_client=fake_client,
        )
    )

    response = await service.submit_ingest_session(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        request=IngestSessionRequest(
            turns=[
                TurnInput(role="user", content="hello"),
            ]
        ),
        idempotency_key="idem-1",
    )

    assert response.status == "accepted"
    assert response.tenant_id == "tenant-1"
    assert fake_client.sender.messages


@pytest.mark.asyncio
async def test_publisher_rejects_oversized_messages() -> None:
    publisher = ServiceBusJobPublisher(
        fully_qualified_namespace="example.servicebus.windows.net",
        queue_name="memforest-writes",
        client_id="client-id",
        servicebus_client=FakeServiceBusClient(),
    )
    envelope = build_write_job_envelope(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"text": "x" * (250 * 1024)},
    )

    with pytest.raises(ValueError, match="message size"):
        await publisher.submit_job(envelope)


def test_service_bus_command_service_from_env_requires_all_settings() -> None:
    assert service_bus_command_service_from_env({}) is None
    assert service_bus_command_service_from_env(
        {
            "AZURE_SERVICEBUS_FULLY_QUALIFIED_NAMESPACE": "example.servicebus.windows.net",
            "AZURE_SERVICEBUS_WRITES_QUEUE_NAME": "memforest-writes",
            "AZURE_CLIENT_ID": "client-id",
        },
        servicebus_client=FakeServiceBusClient(),
    ) is not None


@pytest.mark.asyncio
async def test_durable_starter_posts_write_envelope() -> None:
    http_client = FakeHttpClient()
    starter = DurableJobStarter(
        start_url="https://functions.example/start",
        bearer_token="token",
        http_client=http_client,
    )
    envelope = build_write_job_envelope(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": []},
    )

    await starter.submit_job(envelope)
    await starter.close()

    post = http_client.posts[0]
    assert post["url"] == "https://functions.example/start"
    assert post["json"]["job_id"] == envelope.job_id
    assert post["json"]["instance_id"] == envelope.job_id
    assert post["headers"]["x-ms-durable-instance-id"] == envelope.job_id
    assert post["headers"]["x-functions-key"] == "token"
    assert http_client.response.raise_for_status_called is True
    assert http_client.closed is True


def test_durable_command_service_from_env_requires_start_url() -> None:
    assert durable_command_service_from_env({}) is None
    service = durable_command_service_from_env(
        {"AZURE_MEMFOREST_DURABLE_START_URL": "https://functions.example/start"},
        http_client=FakeHttpClient(),
    )
    assert isinstance(service, DurableMemoryCommandService)


@pytest.mark.asyncio
async def test_durable_starter_tolerates_duplicate_instance_conflict() -> None:
    http_client = FakeHttpClient()
    http_client.response = FakeResponse(status_code=409)
    starter = DurableJobStarter(start_url="https://functions.example/start", http_client=http_client)
    envelope = build_write_job_envelope(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": []},
    )

    await starter.submit_job(envelope)

    assert http_client.posts[0]["json"]["instance_id"] == envelope.job_id


@pytest.mark.asyncio
async def test_durable_starter_sanitizes_start_failures() -> None:
    http_client = FakeHttpClient()
    http_client.response = FakeResponse(status_code=500)
    starter = DurableJobStarter(start_url="https://functions.example/start?code=secret", http_client=http_client)
    envelope = build_write_job_envelope(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": []},
    )

    with pytest.raises(RuntimeError, match="status 500") as exc_info:
        await starter.submit_job(envelope)

    assert "secret" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_durable_starter_rejects_oversized_payloads() -> None:
    http_client = FakeHttpClient()
    starter = DurableJobStarter(start_url="https://functions.example/start", http_client=http_client)
    envelope = build_write_job_envelope(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": [{"role": "user", "content": "x" * (70 * 1024)}]},
    )

    with pytest.raises(ValueError, match="Durable start payload"):
        await starter.submit_job(envelope)

    assert http_client.posts == []


@pytest.mark.asyncio
async def test_durable_ingest_service_routes_delete_and_clear_to_fallback() -> None:
    http_client = FakeHttpClient()
    servicebus_client = FakeServiceBusClient()
    durable = DurableMemoryCommandService(DurableJobStarter(start_url="https://functions.example/start", http_client=http_client))
    fallback = ServiceBusMemoryCommandService(
        ServiceBusJobPublisher(
            fully_qualified_namespace="example.servicebus.windows.net",
            queue_name="memforest-writes",
            client_id="client-id",
            servicebus_client=servicebus_client,
        )
    )
    service = DurableIngestMemoryCommandService(durable, fallback)

    await service.submit_ingest_session(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        request=IngestSessionRequest(turns=[TurnInput(role="user", content="hello")]),
        idempotency_key="idem-ingest",
    )
    await service.submit_delete_session(_actor(), user_id="user-1", session_id="session-1", idempotency_key="idem-delete")
    await service.submit_clear_memory(_actor(), user_id="user-1", idempotency_key="idem-clear")

    assert http_client.posts[0]["json"]["job_type"] == "ingest_session"
    assert [message.application_properties["job_type"] for message in servicebus_client.sender.messages] == [
        "delete_session",
        "clear_memory",
    ]


@pytest.mark.asyncio
async def test_memory_command_service_closes_publisher_resources() -> None:
    fake_client = FakeServiceBusClient()
    service = ServiceBusMemoryCommandService(
        ServiceBusJobPublisher(
            fully_qualified_namespace="example.servicebus.windows.net",
            queue_name="memforest-writes",
            client_id="client-id",
            servicebus_client=fake_client,
        )
    )
    await service.submit_delete_session(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        idempotency_key="idem-1",
    )

    await service.close()

    assert fake_client.sender.closed is True
    assert fake_client.closed is True
