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
        entry["count"] += 1
        entry["item_count"] += item_count
        entry["total_latency_ms"] += elapsed_ms
        entry["max_latency_ms"] = max(float(entry["max_latency_ms"]), elapsed_ms)
        telemetry["total_calls"] = int(telemetry.get("total_calls", 0)) + 1
        telemetry["total_latency_ms"] = float(telemetry.get("total_latency_ms", 0.0)) + elapsed_ms


def monotonic() -> float:
    return time.perf_counter()
