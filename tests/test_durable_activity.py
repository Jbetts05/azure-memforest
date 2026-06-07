from __future__ import annotations

import pytest
from dataclasses import asdict

from src.cloud.cosmos_jobs import JobClaimError
from src.service.auth import ActorContext, TenantContext
from src.service.jobs import build_write_job_envelope
from src.worker.consumer import PermanentHandlerError, TerminalHandlerError, TransientHandlerError
from src.worker.durable_activity import DurableWriteActivityRunner


class FakeJobStore:
    def __init__(self) -> None:
        self.status = "accepted"
        self.claims = []
        self.completed = False
        self.failed_error = ""
        self.fencing_token = 0
        self.error = ""

    async def claim_job(self, *, tenant_id, user_id, job_id, owner_id, ttl_seconds):
        self.claims.append({"owner_id": owner_id, "ttl_seconds": ttl_seconds})
        if self.status not in {"accepted", "running"}:
            raise JobClaimError(f"Cannot claim job in state {self.status!r}.")
        self.status = "running"
        self.fencing_token += 1
        return {"fencing_token": self.fencing_token}

    async def get_job_by_ids(self, *, tenant_id, user_id, job_id):
        return {"status": self.status, "error": self.error}

    async def complete_job(self, *, tenant_id, user_id, job_id, owner_id, fencing_token):
        if self.status != "running":
            raise JobClaimError(f"Cannot complete job in state {self.status!r}.")
        self.status = "succeeded"
        self.completed = True

    async def fail_job(self, *, tenant_id, user_id, job_id, owner_id, fencing_token, error):
        if self.status != "running":
            raise JobClaimError(f"Cannot fail job in state {self.status!r}.")
        self.status = "failed"
        self.error = error
        self.failed_error = error


class RecordingHandler:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = []

    async def handle(self, envelope, claimed_job):
        self.calls.append((envelope, claimed_job))
        if self.error is not None:
            raise self.error


def _envelope(job_type="ingest_session"):
    return build_write_job_envelope(
        ActorContext(actor_id="user-1", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        session_id="session-1",
        job_type=job_type,
        idempotency_key="idem-1",
        payload={"turns": [{"role": "user", "content": "hello"}]} if job_type == "ingest_session" else {},
    )


@pytest.mark.asyncio
async def test_durable_activity_claims_handles_and_completes_job() -> None:
    job_store = FakeJobStore()
    handler = RecordingHandler()
    runner = DurableWriteActivityRunner(job_store=job_store, handler=handler, owner_id="durable:job-1", lease_ttl_seconds=900)

    result = await runner.run(asdict(_envelope()))

    assert result["status"] == "succeeded"
    assert job_store.status == "succeeded"
    assert job_store.completed is True
    assert job_store.claims == [{"owner_id": "durable:job-1", "ttl_seconds": 900}]
    assert handler.calls[0][1]["fencing_token"] == 1


@pytest.mark.asyncio
async def test_durable_activity_marks_permanent_failure_without_retry_exception() -> None:
    job_store = FakeJobStore()
    runner = DurableWriteActivityRunner(
        job_store=job_store,
        handler=RecordingHandler(PermanentHandlerError("bad payload")),
        owner_id="durable:job-1",
    )

    result = await runner.run(asdict(_envelope()))

    assert result["status"] == "failed"
    assert result["reason"] == "permanent_handler_error"
    assert job_store.status == "failed"
    assert job_store.failed_error == "bad payload"


@pytest.mark.asyncio
async def test_durable_activity_marks_terminal_failure_reason() -> None:
    job_store = FakeJobStore()
    runner = DurableWriteActivityRunner(
        job_store=job_store,
        handler=RecordingHandler(TerminalHandlerError("blocked", reason="content_filter")),
        owner_id="durable:job-1",
    )

    result = await runner.run(asdict(_envelope()))

    assert result["status"] == "failed"
    assert result["reason"] == "content_filter"
    assert job_store.status == "failed"
    assert "content_filter" in job_store.failed_error


@pytest.mark.asyncio
async def test_durable_activity_raises_transient_failure_for_durable_retry() -> None:
    job_store = FakeJobStore()
    runner = DurableWriteActivityRunner(
        job_store=job_store,
        handler=RecordingHandler(TransientHandlerError("try again")),
        owner_id="durable:job-1",
    )

    with pytest.raises(TransientHandlerError, match="try again"):
        await runner.run(asdict(_envelope()))

    assert job_store.status == "running"
    assert job_store.failed_error == ""


@pytest.mark.asyncio
async def test_durable_activity_fails_unsupported_job_type_after_claim() -> None:
    job_store = FakeJobStore()
    runner = DurableWriteActivityRunner(job_store=job_store, handler=RecordingHandler(), owner_id="durable:job-1")

    result = await runner.run(asdict(_envelope(job_type="delete_session")))

    assert result["status"] == "failed"
    assert result["reason"] == "unsupported_job_type"
    assert job_store.status == "failed"


@pytest.mark.asyncio
async def test_durable_activity_compensation_reclaims_and_fails_job() -> None:
    job_store = FakeJobStore()
    job_store.status = "running"
    runner = DurableWriteActivityRunner(job_store=job_store, handler=RecordingHandler(), owner_id="durable:job-1")

    result = await runner.fail_after_retries(asdict(_envelope()), reason="durable_activity_failed")

    assert result["status"] == "failed"
    assert result["reason"] == "durable_activity_failed"
    assert job_store.status == "failed"


@pytest.mark.asyncio
async def test_durable_activity_replay_returns_existing_success() -> None:
    job_store = FakeJobStore()
    job_store.status = "succeeded"
    runner = DurableWriteActivityRunner(job_store=job_store, handler=RecordingHandler(), owner_id="durable:job-1")

    result = await runner.run(asdict(_envelope()))

    assert result["status"] == "succeeded"
    assert job_store.status == "succeeded"


@pytest.mark.asyncio
async def test_durable_activity_compensation_preserves_existing_terminal_status() -> None:
    job_store = FakeJobStore()
    job_store.status = "succeeded"
    runner = DurableWriteActivityRunner(job_store=job_store, handler=RecordingHandler(), owner_id="durable:job-1")

    result = await runner.fail_after_retries(asdict(_envelope()), reason="durable_activity_failed")

    assert result["status"] == "succeeded"
    assert job_store.status == "succeeded"
