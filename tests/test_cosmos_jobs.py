from __future__ import annotations

import pytest

from src.cloud.cosmos_jobs import CosmosJobStatusService, CosmosJobStore, JobClaimError, JobNotFound
from src.service.auth import ActorContext, TenantContext
from src.service.jobs import JobRecordingCommandService, build_write_job_envelope
from src.service.schemas import DeleteSessionResponse, IngestSessionRequest, TurnInput


class ExistsError(Exception):
    status_code = 409


class NotFoundError(Exception):
    status_code = 404


class PreconditionFailed(Exception):
    status_code = 412


class FakeJobsContainer:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict] = {}
        self.fail_next_replace = False

    def create_item(self, item, *, partition_key):
        key = (partition_key, item["id"])
        if key in self.items:
            raise ExistsError()
        stored = dict(item)
        stored["_etag"] = "etag-1"
        self.items[key] = stored
        return self.items[key]

    def read_item(self, *, item, partition_key):
        key = (partition_key, item)
        if key not in self.items:
            raise NotFoundError()
        return dict(self.items[key])

    def replace_item(self, *, item, body, partition_key, etag=None, match_condition=None):
        key = (partition_key, item)
        if key not in self.items:
            raise NotFoundError()
        if self.fail_next_replace:
            self.fail_next_replace = False
            raise PreconditionFailed()
        if etag != self.items[key].get("_etag"):
            raise PreconditionFailed()
        stored = dict(body)
        stored["_etag"] = "etag-2"
        self.items[key] = stored


def _actor() -> ActorContext:
    return ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1"), auth_scheme="test")


def _envelope():
    return build_write_job_envelope(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": [{"role": "user", "content": "hello"}]},
    )


@pytest.mark.asyncio
async def test_cosmos_job_store_creates_and_reads_accepted_job() -> None:
    store = CosmosJobStore(container=FakeJobsContainer())
    envelope = _envelope()

    created = await store.create_accepted_job(envelope)
    read = await store.get_job_by_ids(
        tenant_id="tenant-1",
        user_id="user-1",
        job_id=envelope.job_id,
    )

    assert created.job_id == envelope.job_id
    assert created.status == "accepted"
    assert read.job_id == envelope.job_id
    stored = store.container.items[("tenant-1", envelope.job_id)]
    assert stored["payload_digest"]
    assert stored["payload_size_bytes"] > 0
    assert "idem-1" not in str(stored)


@pytest.mark.asyncio
async def test_cosmos_job_store_create_if_absent_does_not_clobber_existing_state() -> None:
    container = FakeJobsContainer()
    store = CosmosJobStore(container=container)
    envelope = _envelope()
    await store.create_accepted_job(envelope)
    container.items[("tenant-1", envelope.job_id)]["status"] = "succeeded"

    response = await store.create_accepted_job(envelope)

    assert response.status == "succeeded"


@pytest.mark.asyncio
async def test_cosmos_job_store_hides_cross_user_jobs() -> None:
    store = CosmosJobStore(container=FakeJobsContainer())
    envelope = _envelope()
    await store.create_accepted_job(envelope)

    with pytest.raises(JobNotFound):
        await store.get_job_by_ids(
            tenant_id="tenant-1",
            user_id="other-user",
            job_id=envelope.job_id,
        )


class FakeCommandService:
    def __init__(self) -> None:
        self.published = False

    async def submit_ingest_session(self, actor, *, user_id, session_id, request, idempotency_key):
        self.published = True
        return {
            "job_id": "unused",
            "status": "accepted",
            "tenant_id": actor.tenant.tenant_id,
            "user_id": user_id,
            "session_id": session_id,
        }

    async def submit_delete_session(self, actor, *, user_id, session_id, idempotency_key):
        self.published = True
        return DeleteSessionResponse(
            job_id="unused",
            tenant_id=actor.tenant.tenant_id,
            user_id=user_id,
            session_id=session_id,
        )


@pytest.mark.asyncio
async def test_job_recording_command_service_records_after_publish() -> None:
    inner = FakeCommandService()
    store = CosmosJobStore(container=FakeJobsContainer())
    service = JobRecordingCommandService(inner, store)

    response = await service.submit_ingest_session(
        _actor(),
        user_id="user-1",
        session_id="session-1",
        request=IngestSessionRequest(turns=[TurnInput(role="user", content="hello")]),
        idempotency_key="idem-1",
    )

    assert inner.published is True
    assert response.status == "accepted"
    assert response.session_id == "session-1"
    assert ("tenant-1", response.job_id) in store.container.items


@pytest.mark.asyncio
async def test_cosmos_job_status_service_returns_not_found_for_missing_job() -> None:
    service = CosmosJobStatusService(CosmosJobStore(container=FakeJobsContainer()))

    with pytest.raises(JobNotFound):
        await service.get_job(_actor(), user_id="user-1", job_id="missing")


@pytest.mark.asyncio
async def test_cosmos_job_store_claim_complete_and_fail_are_fenced() -> None:
    store = CosmosJobStore(container=FakeJobsContainer())
    envelope = _envelope()
    await store.create_accepted_job(envelope)

    claimed = await store.claim_job(
        tenant_id="tenant-1",
        user_id="user-1",
        job_id=envelope.job_id,
        owner_id="worker-1",
        ttl_seconds=60,
    )
    completed = await store.complete_job(
        tenant_id="tenant-1",
        user_id="user-1",
        job_id=envelope.job_id,
        owner_id="worker-1",
        fencing_token=claimed["fencing_token"],
    )

    assert claimed["status"] == "running"
    assert claimed["fencing_token"] == 1
    assert completed.status == "succeeded"
    with pytest.raises(JobClaimError):
        await store.claim_job(
            tenant_id="tenant-1",
            user_id="user-1",
            job_id=envelope.job_id,
            owner_id="worker-2",
            ttl_seconds=60,
        )


@pytest.mark.asyncio
async def test_cosmos_job_store_claim_uses_etag_guard() -> None:
    container = FakeJobsContainer()
    store = CosmosJobStore(container=container)
    envelope = _envelope()
    await store.create_accepted_job(envelope)
    container.fail_next_replace = True

    with pytest.raises(JobClaimError):
        await store.claim_job(
            tenant_id="tenant-1",
            user_id="user-1",
            job_id=envelope.job_id,
            owner_id="worker-1",
            ttl_seconds=60,
        )


@pytest.mark.asyncio
async def test_cosmos_job_store_terminal_state_not_overwritten() -> None:
    store = CosmosJobStore(container=FakeJobsContainer())
    envelope = _envelope()
    await store.create_accepted_job(envelope)
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

    with pytest.raises(JobClaimError):
        await store.fail_job(
            tenant_id="tenant-1",
            user_id="user-1",
            job_id=envelope.job_id,
            owner_id="worker-1",
            fencing_token=claimed["fencing_token"],
            error="late",
        )


@pytest.mark.asyncio
async def test_cosmos_job_store_rejects_wrong_fencing_token() -> None:
    store = CosmosJobStore(container=FakeJobsContainer())
    envelope = _envelope()
    await store.create_accepted_job(envelope)
    await store.claim_job(
        tenant_id="tenant-1",
        user_id="user-1",
        job_id=envelope.job_id,
        owner_id="worker-1",
        ttl_seconds=60,
    )

    with pytest.raises(JobClaimError):
        await store.fail_job(
            tenant_id="tenant-1",
            user_id="user-1",
            job_id=envelope.job_id,
            owner_id="worker-1",
            fencing_token=999,
            error="boom",
        )
