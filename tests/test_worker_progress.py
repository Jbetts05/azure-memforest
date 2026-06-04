from __future__ import annotations

from src.service.auth import ActorContext, TenantContext
from src.service.jobs import build_write_job_envelope
from src.worker.progress import budget_for_envelope, classify_job_progress


def _envelope(*, turns):
    return build_write_job_envelope(
        ActorContext(actor_id="actor", tenant=TenantContext("tenant-1")),
        user_id="user-1",
        session_id="session-1",
        job_type="ingest_session",
        idempotency_key="idem-1",
        payload={"turns": turns},
    )


def test_ingest_budget_scales_with_input_size() -> None:
    small = budget_for_envelope(_envelope(turns=[{"role": "user", "content": "hello"}]))
    large = budget_for_envelope(_envelope(turns=[{"role": "user", "content": "x" * 10_000} for _ in range(50)]))

    assert small.stall_timeout_seconds < large.stall_timeout_seconds
    assert small.max_completed_model_calls < large.max_completed_model_calls


def test_ingest_budget_preserves_default_multiplier() -> None:
    envelope = _envelope(turns=[{"role": "user", "content": "x" * 10_000}])

    budget = budget_for_envelope(envelope, environ={})

    assert budget.max_completed_model_calls == 192


def test_ingest_budget_can_be_scaled_with_environment() -> None:
    envelope = _envelope(turns=[{"role": "user", "content": "x" * 10_000}])

    budget = budget_for_envelope(
        envelope,
        environ={
            "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MULTIPLIER": "8",
            "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX": "2000",
        },
    )

    assert budget.max_completed_model_calls == 384


def test_ingest_budget_cap_still_bounds_environment_multiplier() -> None:
    envelope = _envelope(turns=[{"role": "user", "content": "x" * 10_000} for _ in range(50)])

    budget = budget_for_envelope(
        envelope,
        environ={
            "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MULTIPLIER": "100",
            "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX": "1000",
        },
    )

    assert budget.max_completed_model_calls == 1000


def test_ingest_budget_can_be_floored_with_environment() -> None:
    envelope = _envelope(turns=[{"role": "user", "content": "hello"}])

    budget = budget_for_envelope(
        envelope,
        environ={
            "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MULTIPLIER": "20",
            "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN": "2000",
            "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX": "2000",
        },
    )

    assert budget.max_completed_model_calls == 2000


def test_ingest_budget_rejects_min_above_max() -> None:
    envelope = _envelope(turns=[{"role": "user", "content": "hello"}])

    try:
        budget_for_envelope(
            envelope,
            environ={
                "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN": "3000",
                "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX": "2000",
            },
        )
    except ValueError as exc:
        assert "BUDGET_MIN cannot exceed" in str(exc)
    else:
        raise AssertionError("inverted budget min/max should fail")


def test_ingest_budget_rejects_inverted_low_bounds_before_clamping() -> None:
    envelope = _envelope(turns=[{"role": "user", "content": "hello"}])

    try:
        budget_for_envelope(
            envelope,
            environ={
                "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN": "100",
                "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX": "50",
            },
        )
    except ValueError as exc:
        assert "BUDGET_MIN cannot exceed" in str(exc)
    else:
        raise AssertionError("low inverted budget min/max should fail before clamping")


def test_classify_job_progress_detects_stall_without_heartbeat_progress() -> None:
    classification = classify_job_progress(
        {
            "status": "running",
            "job_type": "ingest_session",
            "last_progress_at": 100.0,
            "progress_signature": {"stage": "extract_build", "completed_model_calls": 1},
            "progress_budget": {
                "job_type": "ingest_session",
                "stall_timeout_seconds": 60,
                "max_completed_model_calls": 100,
                "max_retries": 1,
            },
        },
        now=161.0,
    )

    assert classification.state == "stalled"
    assert classification.reason == "progress_signature_unchanged"


def test_classify_job_progress_detects_call_budget_excess() -> None:
    classification = classify_job_progress(
        {
            "status": "running",
            "job_type": "ingest_session",
            "last_progress_at": 100.0,
            "progress_signature": {"stage": "extract_build", "completed_model_calls": 101},
            "progress_budget": {
                "job_type": "ingest_session",
                "stall_timeout_seconds": 60,
                "max_completed_model_calls": 100,
                "max_retries": 1,
            },
        },
        now=101.0,
    )

    assert classification.state == "over_call_budget"


def test_classify_job_progress_ignores_non_running_jobs() -> None:
    classification = classify_job_progress(
        {
            "status": "succeeded",
            "last_progress_at": 100.0,
            "progress_signature": {"stage": "extract_build", "completed_model_calls": 999},
            "progress_budget": {"max_completed_model_calls": 1, "stall_timeout_seconds": 1},
        },
        now=200.0,
    )

    assert classification.state == "ok"
    assert classification.reason == "not_running"


def test_classify_job_progress_treats_missing_timestamp_as_unknown_not_stalled() -> None:
    classification = classify_job_progress(
        {
            "status": "running",
            "progress_signature": {"stage": "extract_build", "completed_model_calls": 1},
            "progress_budget": {"max_completed_model_calls": 100, "stall_timeout_seconds": 1},
        },
        now=200.0,
    )

    assert classification.state == "ok"
    assert classification.reason == "unknown_progress_timestamp"
