from __future__ import annotations

import pytest

from src.cloud.cosmos_jobs import CosmosJobStore
from src.service.auth import ActorContext, TenantContext
from src.service.jobs import build_write_job_envelope
from src.worker.consumer import (
    PermanentHandlerError,
    ServiceBusWriteWorker,
    TransientHandlerError,
    parse_write_job_envelope,
)
from tests.test_cosmos_jobs import FakeJobsContainer


class FakeMessage:
    def __init__(self, body: bytes):
        self.body = body
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
