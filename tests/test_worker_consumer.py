from __future__ import annotations

import pytest

from src.cloud.cosmos_jobs import CosmosJobStore
from src.service.auth import ActorContext, TenantContext
from src.service.jobs import build_write_job_envelope
from src.worker.consumer import (
    PermanentHandlerError,
    ServiceBusWriteWorker,
    TerminalHandlerError,
    TransientHandlerError,
    parse_write_job_envelope,
)
from src.worker.receiver import ServiceBusSessionReceiverLoop, WorkerReceiverConfig
from tests.test_cosmos_jobs import FakeJobsContainer


class FakeMessage:
    def __init__(self, body: bytes, *, delivery_count: int = 1):
        self.body = body
        self.delivery_count = delivery_count
        self.completed = False
        self.abandoned = False
        self.dead_lettered = False
        self.dead_letter_reason = ""

    async def complete(self):
        self.completed = True

    async def abandon(self):
        self.abandoned = True

    async def dead_letter(self, *, reason, error_description):
        self.dead_lettered = True
        self.dead_letter_reason = reason


class FakeAzureMessage:
    def __init__(self, body: bytes, *, message_id: str = "message-1", delivery_count: int = 1):
        self.body = body
        self.message_id = message_id
        self.delivery_count = delivery_count
        self.completed = False
        self.abandoned = False
        self.dead_lettered = False


class FakeSession:
    def __init__(self, session_id="tenant-1:user-1"):
        self.session_id = session_id


class FakeSessionReceiver:
    def __init__(self, messages):
        self.messages = list(messages)
        self.session = FakeSession()
        self.completed = []
        self.abandoned = []
        self.dead_lettered = []
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited = True

    async def receive_messages(self, *, max_message_count, max_wait_time):
        if not self.messages:
            return []
        return [self.messages.pop(0)]

    async def complete_message(self, message):
        message.completed = True
        self.completed.append(message.message_id)

    async def abandon_message(self, message):
        message.abandoned = True
        self.abandoned.append(message.message_id)

    async def dead_letter_message(self, message, *, reason, error_description):
        message.dead_lettered = True
        self.dead_lettered.append((message.message_id, reason))


class FakeServiceBusClient:
    def __init__(self, receiver=None, error=None):
        self.receiver = receiver
        self.error = error
        self.closed = False

    async def accept_next_session(self, *, queue_name, max_wait_time):
        if self.error is not None:
            raise self.error
        return self.receiver

    async def close(self):
        self.closed = True


class FakeRealSdkServiceBusClient:
    def __init__(self, receiver):
        self.receiver = receiver
        self.calls = []

    def get_queue_receiver(self, *, queue_name, session_id, max_wait_time):
        self.calls.append((queue_name, session_id, max_wait_time))
        return self.receiver


class FakeOperationTimeoutError(Exception):
    pass


FakeOperationTimeoutError.__name__ = "OperationTimeoutError"


class FakeLockRenewer:
    def __init__(self):
        self.registered = []
        self.closed = False

    async def register(self, receiver, session, *, max_lock_renewal_duration):
        self.registered.append((receiver, session, max_lock_renewal_duration))

    async def close(self):
        self.closed = True


class RecordingHandler:
    def __init__(self, error=None):
        self.error = error
        self.called = False

    async def handle(self, envelope, claimed_job):
        self.called = True
        if self.error:
            raise self.error


class BrokenTransitionStore:
    def __init__(self, container, *, fail_complete=False, fail_fail=False):
        self._inner = CosmosJobStore(container)
        self.fail_complete = fail_complete
        self.fail_fail = fail_fail

    async def create_accepted_job(self, *args, **kwargs):
        return await self._inner.create_accepted_job(*args, **kwargs)

    async def get_job_by_ids(self, *args, **kwargs):
        return await self._inner.get_job_by_ids(*args, **kwargs)

    async def claim_job(self, *args, **kwargs):
        return await self._inner.claim_job(*args, **kwargs)

    async def complete_job(self, **kwargs):
        if self.fail_complete:
            raise RuntimeError("complete failed")
        return await self._inner.complete_job(**kwargs)

    async def fail_job(self, **kwargs):
        if self.fail_fail:
            raise RuntimeError("fail failed")
        return await self._inner.fail_job(**kwargs)


def _envelope():
    actor = ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1"))
    return build_write_job_envelope(
        actor,
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": []},
    )


async def _store_with_job(envelope):
    store = CosmosJobStore(FakeJobsContainer())
    await store.create_accepted_job(envelope)
    return store


def test_parse_write_job_envelope_validates_schema_and_type() -> None:
    envelope = _envelope()
    parsed = parse_write_job_envelope(envelope.to_json())

    assert parsed.job_id == envelope.job_id
    with pytest.raises(ValueError, match="unsupported job_type"):
        parse_write_job_envelope(envelope.to_json().replace("ingest_session", "bad_type"))


@pytest.mark.asyncio
async def test_worker_process_message_completes_on_success() -> None:
    envelope = _envelope()
    store = await _store_with_job(envelope)
    handler = RecordingHandler()
    worker = ServiceBusWriteWorker(job_store=store, handler=handler, owner_id="worker-1")
    message = FakeMessage(envelope.to_json().encode("utf-8"))

    outcome = await worker.process_message(message)

    assert handler.called is True
    assert message.completed is True
    assert outcome.action == "complete"
    status = await store.get_job_by_ids(tenant_id="tenant-1", user_id="user-1", job_id=envelope.job_id)
    assert status.status == "succeeded"


@pytest.mark.asyncio
async def test_worker_dead_letters_invalid_envelope() -> None:
    worker = ServiceBusWriteWorker(
        job_store=CosmosJobStore(FakeJobsContainer()),
        handler=RecordingHandler(),
        owner_id="worker-1",
    )
    message = FakeMessage(b"{}")

    outcome = await worker.process_message(message)

    assert outcome.action == "dead_letter"
    assert message.dead_lettered is True


@pytest.mark.asyncio
async def test_worker_abandons_transient_handler_error() -> None:
    envelope = _envelope()
    store = await _store_with_job(envelope)
    worker = ServiceBusWriteWorker(
        job_store=store,
        handler=RecordingHandler(TransientHandlerError("try again")),
        owner_id="worker-1",
    )
    message = FakeMessage(envelope.to_json().encode("utf-8"))

    outcome = await worker.process_message(message)

    assert outcome.action == "abandon"
    assert message.abandoned is True


@pytest.mark.asyncio
async def test_worker_fails_and_dead_letters_final_transient_error() -> None:
    envelope = _envelope()
    store = await _store_with_job(envelope)
    worker = ServiceBusWriteWorker(
        job_store=store,
        handler=RecordingHandler(TransientHandlerError("try again")),
        owner_id="worker-1",
    )
    message = FakeMessage(envelope.to_json().encode("utf-8"), delivery_count=4)

    outcome = await worker.process_message(message)

    assert outcome.action == "dead_letter"
    assert outcome.reason == "transient_handler_error"
    assert message.dead_lettered is True
    status = await store.get_job_by_ids(tenant_id="tenant-1", user_id="user-1", job_id=envelope.job_id)
    assert status.status == "failed"
    assert status.error == "try again"


@pytest.mark.asyncio
async def test_worker_fails_and_dead_letters_final_unhandled_error() -> None:
    envelope = _envelope()
    store = await _store_with_job(envelope)
    worker = ServiceBusWriteWorker(
        job_store=store,
        handler=RecordingHandler(RuntimeError("boom")),
        owner_id="worker-1",
    )
    message = FakeMessage(envelope.to_json().encode("utf-8"), delivery_count=4)

    outcome = await worker.process_message(message)

    assert outcome.action == "dead_letter"
    assert outcome.reason == "unhandled_handler_error"
    assert message.dead_lettered is True
    status = await store.get_job_by_ids(tenant_id="tenant-1", user_id="user-1", job_id=envelope.job_id)
    assert status.status == "failed"
    assert "RuntimeError: boom" in status.error


@pytest.mark.asyncio
async def test_worker_marks_failed_and_dead_letters_permanent_error() -> None:
    envelope = _envelope()
    store = await _store_with_job(envelope)
    worker = ServiceBusWriteWorker(
        job_store=store,
        handler=RecordingHandler(PermanentHandlerError("bad payload")),
        owner_id="worker-1",
    )
    message = FakeMessage(envelope.to_json().encode("utf-8"))

    outcome = await worker.process_message(message)

    assert outcome.action == "dead_letter"
    assert message.dead_lettered is True
    status = await store.get_job_by_ids(tenant_id="tenant-1", user_id="user-1", job_id=envelope.job_id)
    assert status.status == "failed"


@pytest.mark.asyncio
async def test_worker_marks_terminal_error_failed_without_dead_lettering() -> None:
    envelope = _envelope()
    store = await _store_with_job(envelope)
    worker = ServiceBusWriteWorker(
        job_store=store,
        handler=RecordingHandler(TerminalHandlerError("filtered", reason="content_filter")),
        owner_id="worker-1",
    )
    message = FakeMessage(envelope.to_json().encode("utf-8"))

    outcome = await worker.process_message(message)

    assert outcome.action == "complete"
    assert outcome.reason == "content_filter"
    assert message.completed is True
    assert message.dead_lettered is False
    status = await store.get_job_by_ids(tenant_id="tenant-1", user_id="user-1", job_id=envelope.job_id)
    assert status.status == "failed"
    assert status.error == "content_filter: filtered"


@pytest.mark.asyncio
async def test_worker_completes_terminal_job_without_handler() -> None:
    envelope = _envelope()
    store = await _store_with_job(envelope)
    claimed = await store.claim_job(
        tenant_id="tenant-1",
        user_id="user-1",
        job_id=envelope.job_id,
        owner_id="worker-1",
        ttl_seconds=60,
    )
    await store.complete_job(
        tenant_id="tenant-1",
        user_id="user-1",
        job_id=envelope.job_id,
        owner_id="worker-1",
        fencing_token=claimed["fencing_token"],
    )
    handler = RecordingHandler()
    worker = ServiceBusWriteWorker(job_store=store, handler=handler, owner_id="worker-2")
    message = FakeMessage(envelope.to_json().encode("utf-8"))

    outcome = await worker.process_message(message)

    assert outcome.action == "complete"
    assert handler.called is False
    assert message.completed is True


@pytest.mark.asyncio
async def test_worker_dead_letters_when_job_record_missing() -> None:
    envelope = _envelope()
    worker = ServiceBusWriteWorker(
        job_store=CosmosJobStore(FakeJobsContainer()),
        handler=RecordingHandler(),
        owner_id="worker-1",
    )
    message = FakeMessage(envelope.to_json().encode("utf-8"))

    outcome = await worker.process_message(message)

    assert outcome.action == "dead_letter"
    assert outcome.reason == "job_not_found"
    assert message.dead_lettered is True


@pytest.mark.asyncio
async def test_worker_abandons_when_complete_transition_fails() -> None:
    envelope = _envelope()
    store = BrokenTransitionStore(FakeJobsContainer(), fail_complete=True)
    await store.create_accepted_job(envelope)
    worker = ServiceBusWriteWorker(job_store=store, handler=RecordingHandler(), owner_id="worker-1")
    message = FakeMessage(envelope.to_json().encode("utf-8"))

    outcome = await worker.process_message(message)

    assert outcome.action == "abandon"
    assert outcome.reason == "complete_job_failed"
    assert message.abandoned is True


@pytest.mark.asyncio
async def test_worker_dead_letters_final_complete_transition_failure_without_marking_failed() -> None:
    envelope = _envelope()
    store = BrokenTransitionStore(FakeJobsContainer(), fail_complete=True)
    await store.create_accepted_job(envelope)
    worker = ServiceBusWriteWorker(job_store=store, handler=RecordingHandler(), owner_id="worker-1")
    message = FakeMessage(envelope.to_json().encode("utf-8"), delivery_count=4)

    outcome = await worker.process_message(message)

    assert outcome.action == "dead_letter"
    assert outcome.reason == "complete_job_failed"
    assert message.dead_lettered is True
    status = await store.get_job_by_ids(tenant_id="tenant-1", user_id="user-1", job_id=envelope.job_id)
    assert status.status == "running"


@pytest.mark.asyncio
async def test_worker_abandons_when_failed_transition_fails() -> None:
    envelope = _envelope()
    store = BrokenTransitionStore(FakeJobsContainer(), fail_fail=True)
    await store.create_accepted_job(envelope)
    worker = ServiceBusWriteWorker(
        job_store=store,
        handler=RecordingHandler(PermanentHandlerError("bad")),
        owner_id="worker-1",
    )
    message = FakeMessage(envelope.to_json().encode("utf-8"))

    outcome = await worker.process_message(message)

    assert outcome.action == "abandon"
    assert outcome.reason == "fail_job_failed"
    assert message.abandoned is True


@pytest.mark.asyncio
async def test_worker_dead_letters_final_failed_transition_failure() -> None:
    envelope = _envelope()
    store = BrokenTransitionStore(FakeJobsContainer(), fail_fail=True)
    await store.create_accepted_job(envelope)
    worker = ServiceBusWriteWorker(
        job_store=store,
        handler=RecordingHandler(PermanentHandlerError("bad")),
        owner_id="worker-1",
    )
    message = FakeMessage(envelope.to_json().encode("utf-8"), delivery_count=4)

    outcome = await worker.process_message(message)

    assert outcome.action == "dead_letter"
    assert outcome.reason == "fail_job_failed"
    assert message.dead_lettered is True


@pytest.mark.asyncio
async def test_receiver_loop_processes_session_messages_in_order() -> None:
    first = _envelope()
    second = build_write_job_envelope(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        session_id="session-2",
        job_type="ingest_session",
        idempotency_key="idem-2",
        payload={"turns": []},
    )
    store = CosmosJobStore(FakeJobsContainer())
    await store.create_accepted_job(first)
    await store.create_accepted_job(second)
    handler = RecordingHandler()
    worker = ServiceBusWriteWorker(job_store=store, handler=handler, owner_id="worker-1")
    receiver = FakeSessionReceiver(
        [
            FakeAzureMessage(first.to_json().encode("utf-8"), message_id=first.job_id),
            FakeAzureMessage(second.to_json().encode("utf-8"), message_id=second.job_id),
        ]
    )
    renewer = FakeLockRenewer()
    loop = ServiceBusSessionReceiverLoop(
        servicebus_client=FakeServiceBusClient(receiver),
        worker=worker,
        config=WorkerReceiverConfig(queue_name="writes", message_wait_seconds=0.1),
        lock_renewer=renewer,
    )

    outcome = await loop.process_next_session()

    assert outcome.action == "processed"
    assert outcome.processed_count == 2
    assert receiver.completed == [first.job_id, second.job_id]
    assert receiver.entered is True
    assert receiver.exited is True
    assert renewer.registered[0][1] is receiver.session


@pytest.mark.asyncio
async def test_receiver_loop_supports_real_sdk_get_queue_receiver_path() -> None:
    envelope = _envelope()
    store = await _store_with_job(envelope)
    worker = ServiceBusWriteWorker(job_store=store, handler=RecordingHandler(), owner_id="worker-1")
    receiver = FakeSessionReceiver([FakeAzureMessage(envelope.to_json().encode("utf-8"), message_id=envelope.job_id)])
    client = FakeRealSdkServiceBusClient(receiver)
    loop = ServiceBusSessionReceiverLoop(
        servicebus_client=client,
        worker=worker,
        config=WorkerReceiverConfig(queue_name="writes", message_wait_seconds=0.1),
    )

    outcome = await loop.process_next_session()

    assert outcome.processed_count == 1
    assert client.calls[0][0] == "writes"
    assert receiver.completed == [envelope.job_id]


@pytest.mark.asyncio
async def test_receiver_loop_treats_accept_timeout_as_idle() -> None:
    worker = ServiceBusWriteWorker(
        job_store=CosmosJobStore(FakeJobsContainer()),
        handler=RecordingHandler(),
        owner_id="worker-1",
    )
    loop = ServiceBusSessionReceiverLoop(
        servicebus_client=FakeServiceBusClient(error=FakeOperationTimeoutError()),
        worker=worker,
        config=WorkerReceiverConfig(queue_name="writes", message_wait_seconds=0.1),
    )

    outcome = await loop.process_next_session()

    assert outcome.action == "idle"
    assert outcome.reason == "no_session_available"


@pytest.mark.asyncio
async def test_receiver_loop_treats_wrapped_timeout_as_idle() -> None:
    worker = ServiceBusWriteWorker(
        job_store=CosmosJobStore(FakeJobsContainer()),
        handler=RecordingHandler(),
        owner_id="worker-1",
    )
    loop = ServiceBusSessionReceiverLoop(
        servicebus_client=FakeServiceBusClient(error=RuntimeError("Service Bus operation timed out")),
        worker=worker,
        config=WorkerReceiverConfig(queue_name="writes", message_wait_seconds=0.1),
    )

    outcome = await loop.process_next_session()

    assert outcome.action == "idle"
    assert outcome.reason == "no_session_available"


@pytest.mark.asyncio
async def test_receiver_loop_releases_session_after_abandon() -> None:
    envelope = _envelope()
    store = await _store_with_job(envelope)
    worker = ServiceBusWriteWorker(
        job_store=store,
        handler=RecordingHandler(TransientHandlerError("try again")),
        owner_id="worker-1",
    )
    message = FakeAzureMessage(envelope.to_json().encode("utf-8"), message_id=envelope.job_id)
    receiver = FakeSessionReceiver([message])
    loop = ServiceBusSessionReceiverLoop(
        servicebus_client=FakeServiceBusClient(receiver),
        worker=worker,
        config=WorkerReceiverConfig(queue_name="writes", message_wait_seconds=0.1),
    )

    outcome = await loop.process_next_session()

    assert outcome.processed_count == 1
    assert message.abandoned is True
    assert receiver.exited is True
