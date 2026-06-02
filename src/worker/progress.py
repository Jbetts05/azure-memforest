"""Progress budgets and stall classification for write jobs."""

from __future__ import annotations

import time
import os
from dataclasses import dataclass
from typing import Any, Literal

from src.service.jobs import WriteJobEnvelope

ProgressState = Literal["ok", "stalled", "over_call_budget"]


@dataclass(frozen=True, slots=True)
class JobProgressBudget:
    job_type: str
    stall_timeout_seconds: float
    max_completed_model_calls: int
    max_retries: int = 1


@dataclass(frozen=True, slots=True)
class JobProgressClassification:
    state: ProgressState
    reason: str = ""
    seconds_since_progress: float = 0.0


def budget_for_envelope(envelope: WriteJobEnvelope, environ: dict[str, str] | None = None) -> JobProgressBudget:
    values = os.environ if environ is None else environ
    if envelope.job_type != "ingest_session":
        return JobProgressBudget(
            job_type=envelope.job_type,
            stall_timeout_seconds=20 * 60,
            max_completed_model_calls=250,
            max_retries=1,
        )
    turns = list((envelope.payload or {}).get("turns") or [])
    char_count = sum(len(str(turn.get("content") or "")) for turn in turns if isinstance(turn, dict))
    expected_calls = max(20, len(turns) * 8 + (char_count // 1000) * 4)
    max_calls = _resolve_max_completed_model_calls(expected_calls, values)
    return JobProgressBudget(
        job_type="ingest_session",
        stall_timeout_seconds=_stall_timeout_for(char_count=char_count, turn_count=len(turns)),
        max_completed_model_calls=max_calls,
        max_retries=1,
    )


def budget_from_document(document: dict[str, Any]) -> JobProgressBudget | None:
    raw = dict(document.get("progress_budget") or {})
    if not raw:
        return None
    stall_timeout = raw.get("stall_timeout_seconds")
    max_calls = raw.get("max_completed_model_calls")
    max_retries = raw.get("max_retries")
    return JobProgressBudget(
        job_type=str(raw.get("job_type") or document.get("job_type") or "unknown"),
        stall_timeout_seconds=float(stall_timeout if stall_timeout is not None else 20 * 60),
        max_completed_model_calls=int(max_calls if max_calls is not None else 250),
        max_retries=int(max_retries if max_retries is not None else 1),
    )


def classify_job_progress(document: dict[str, Any], *, now: float | None = None) -> JobProgressClassification:
    now = time.time() if now is None else now
    if document.get("status") != "running":
        return JobProgressClassification(state="ok", reason="not_running")
    budget = _budget_from_document(document)
    signature = dict(document.get("progress_signature") or {})
    completed_calls = int(signature.get("completed_model_calls") or 0)
    if completed_calls > budget.max_completed_model_calls:
        return JobProgressClassification(
            state="over_call_budget",
            reason="completed_model_calls_exceeded_budget",
            seconds_since_progress=0.0,
        )
    last_progress_at = _last_progress_at(document)
    if last_progress_at <= 0:
        return JobProgressClassification(state="ok", reason="unknown_progress_timestamp")
    seconds_since_progress = max(0.0, now - last_progress_at)
    if seconds_since_progress >= budget.stall_timeout_seconds:
        return JobProgressClassification(
            state="stalled",
            reason="progress_signature_unchanged",
            seconds_since_progress=seconds_since_progress,
        )
    return JobProgressClassification(state="ok", seconds_since_progress=seconds_since_progress)


def _stall_timeout_for(*, char_count: int, turn_count: int) -> float:
    if turn_count <= 10 and char_count <= 20_000:
        return 10 * 60
    if turn_count <= 100 and char_count <= 200_000:
        return 20 * 60
    return 45 * 60


def _budget_from_document(document: dict[str, Any]) -> JobProgressBudget:
    budget = budget_from_document(document)
    if budget is not None:
        return budget
    return JobProgressBudget(
        job_type=str(document.get("job_type") or "unknown"),
        stall_timeout_seconds=20 * 60,
        max_completed_model_calls=250,
        max_retries=1,
    )


def _last_progress_at(document: dict[str, Any]) -> float:
    for key in ("last_progress_at", "stage_started_at", "started_at", "submitted_at", "updated_at"):
        value = document.get(key)
        if value is not None:
            return float(value)
    return 0.0


def _resolve_max_completed_model_calls(expected_calls: int, values: dict[str, str] | os._Environ[str]) -> int:
    multiplier = _float_setting(values, "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MULTIPLIER", default=4.0)
    cap = _int_setting(values, "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX", default=2000)
    multiplier = max(1.0, multiplier)
    cap = max(150, cap)
    return min(cap, max(150, int(expected_calls * multiplier)))


def _float_setting(values: dict[str, str] | os._Environ[str], name: str, *, default: float) -> float:
    try:
        return float(str(values.get(name, "")).strip() or default)
    except (TypeError, ValueError):
        return default


def _int_setting(values: dict[str, str] | os._Environ[str], name: str, *, default: int) -> int:
    try:
        return int(str(values.get(name, "")).strip() or default)
    except (TypeError, ValueError):
        return default
