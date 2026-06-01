"""Lightweight in-process model-call telemetry for a single job."""

from __future__ import annotations

import contextvars
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator


_current: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar("model_telemetry", default=None)
_global_lock = threading.Lock()
_global_current: dict[str, Any] | None = None


@contextmanager
def collect_model_telemetry() -> Iterator[dict[str, Any]]:
    global _global_current
    telemetry: dict[str, Any] = {"calls": {}, "total_calls": 0, "total_latency_ms": 0.0}
    token = _current.set(telemetry)
    with _global_lock:
        previous_global = _global_current
        _global_current = telemetry
    try:
        yield telemetry
    finally:
        with _global_lock:
            _global_current = previous_global
        _current.reset(token)


def record_model_call_started(*, role: str, deployment: str, operation: str, item_count: int = 1) -> None:
    telemetry = _current.get()
    if telemetry is None:
        with _global_lock:
            telemetry = _global_current
    if telemetry is None:
        return
    with _global_lock:
        key = f"{operation}:{role}:{deployment}"
        calls = telemetry.setdefault("calls", {})
        entry = calls.setdefault(
            key,
            {
                "operation": operation,
                "role": role,
                "deployment": deployment,
                "count": 0,
                "item_count": 0,
                "total_latency_ms": 0.0,
                "max_latency_ms": 0.0,
            },
        )
        entry["started_count"] = int(entry.get("started_count", 0)) + 1
        entry["in_flight"] = int(entry.get("in_flight", 0)) + 1
        entry["started_item_count"] = int(entry.get("started_item_count", 0)) + item_count
        entry["last_started_at_monotonic"] = time.perf_counter()
        telemetry["total_started_calls"] = int(telemetry.get("total_started_calls", 0)) + 1
        telemetry["in_flight_calls"] = int(telemetry.get("in_flight_calls", 0)) + 1


def record_model_call(*, role: str, deployment: str, operation: str, elapsed_seconds: float, item_count: int = 1) -> None:
    telemetry = _current.get()
    if telemetry is None:
        with _global_lock:
            telemetry = _global_current
    if telemetry is None:
        return
    with _global_lock:
        key = f"{operation}:{role}:{deployment}"
        calls = telemetry.setdefault("calls", {})
        entry = calls.setdefault(
            key,
            {
                "operation": operation,
                "role": role,
                "deployment": deployment,
                "count": 0,
                "item_count": 0,
                "total_latency_ms": 0.0,
                "max_latency_ms": 0.0,
            },
        )
        elapsed_ms = max(0.0, elapsed_seconds * 1000.0)
        entry["in_flight"] = max(0, int(entry.get("in_flight", 0)) - 1)
        entry["count"] += 1
        entry["item_count"] += item_count
        entry["total_latency_ms"] += elapsed_ms
        entry["max_latency_ms"] = max(float(entry["max_latency_ms"]), elapsed_ms)
        telemetry["total_calls"] = int(telemetry.get("total_calls", 0)) + 1
        telemetry["total_latency_ms"] = float(telemetry.get("total_latency_ms", 0.0)) + elapsed_ms
        telemetry["in_flight_calls"] = max(0, int(telemetry.get("in_flight_calls", 0)) - 1)


def record_model_call_failed(
    *,
    role: str,
    deployment: str,
    operation: str,
    elapsed_seconds: float,
    error_type: str,
    item_count: int = 1,
) -> None:
    telemetry = _current.get()
    if telemetry is None:
        with _global_lock:
            telemetry = _global_current
    if telemetry is None:
        return
    with _global_lock:
        key = f"{operation}:{role}:{deployment}"
        calls = telemetry.setdefault("calls", {})
        entry = calls.setdefault(
            key,
            {
                "operation": operation,
                "role": role,
                "deployment": deployment,
                "count": 0,
                "item_count": 0,
                "total_latency_ms": 0.0,
                "max_latency_ms": 0.0,
            },
        )
        elapsed_ms = max(0.0, elapsed_seconds * 1000.0)
        entry["in_flight"] = max(0, int(entry.get("in_flight", 0)) - 1)
        entry["failure_count"] = int(entry.get("failure_count", 0)) + 1
        entry["failed_item_count"] = int(entry.get("failed_item_count", 0)) + item_count
        entry["last_error_type"] = error_type
        entry["total_failure_latency_ms"] = float(entry.get("total_failure_latency_ms", 0.0)) + elapsed_ms
        entry["max_latency_ms"] = max(float(entry["max_latency_ms"]), elapsed_ms)
        telemetry["total_failures"] = int(telemetry.get("total_failures", 0)) + 1
        telemetry["in_flight_calls"] = max(0, int(telemetry.get("in_flight_calls", 0)) - 1)


def monotonic() -> float:
    return time.perf_counter()
