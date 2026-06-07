"""Small async batching helpers for cloud data-plane operations."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

T = TypeVar("T")

DEFAULT_WRITE_CONCURRENCY = 4
DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_MAX_RETRY_DELAY_SECONDS = 30.0


async def run_bounded(
    items: Sequence[T],
    worker: Callable[[T], Awaitable[None]],
    *,
    concurrency: int | None = None,
    max_attempts: int | None = None,
) -> None:
    """Run one async worker per item in bounded chunks.

    The helper intentionally avoids creating unbounded task lists for very large
    projections. If a worker fails, the exception is propagated after the
    current chunk's already-started operations settle; callers must rely on
    idempotent writes and commit fencing for partial-write safety.
    """

    if not items:
        return
    limit = _resolved_concurrency(concurrency)
    attempts = _resolved_max_attempts(max_attempts)
    if limit == 1:
        for item in items:
            await _run_with_retry(item, worker, max_attempts=attempts)
        return
    for index in range(0, len(items), limit):
        chunk = items[index : index + limit]
        await asyncio.gather(*(_run_with_retry(item, worker, max_attempts=attempts) for item in chunk))


async def _run_with_retry(item: T, worker: Callable[[T], Awaitable[None]], *, max_attempts: int) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            await worker(item)
            return
        except Exception as exc:
            if attempt >= max_attempts or not _is_retryable_throttle(exc):
                raise
            await asyncio.sleep(_retry_delay_seconds(exc, attempt))


def _is_retryable_throttle(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code == 429:
        return True
    text = str(exc)
    return "TooManyRequests" in text or "request rate is too large" in text


def _retry_delay_seconds(exc: Exception, attempt: int) -> float:
    retry_after = getattr(exc, "retry_after_in_milliseconds", None)
    if retry_after is not None:
        try:
            return max(0.1, float(retry_after) / 1000.0)
        except (TypeError, ValueError):
            pass
    headers = getattr(exc, "headers", None) or {}
    retry_after = _header_value(headers, "x-ms-retry-after-ms")
    if retry_after is not None:
        try:
            return max(0.1, float(retry_after) / 1000.0)
        except (TypeError, ValueError):
            pass
    retry_after = _header_value(headers, "retry-after")
    if retry_after is not None:
        try:
            return max(0.1, float(retry_after))
        except (TypeError, ValueError):
            pass
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        try:
            return max(0.1, float(retry_after))
        except (TypeError, ValueError):
            pass
    return min(_max_retry_delay_seconds(), 0.5 * (2 ** (attempt - 1)))


def _header_value(headers: object, name: str) -> object | None:
    if not hasattr(headers, "get"):
        return None
    value = headers.get(name)  # type: ignore[attr-defined]
    if value is not None:
        return value
    return headers.get(name.lower())  # type: ignore[attr-defined]


def _resolved_concurrency(value: int | None) -> int:
    if value is not None:
        return max(1, int(value))
    return _int_env("AZURE_MEMFOREST_PROJECTION_WRITE_CONCURRENCY", DEFAULT_WRITE_CONCURRENCY)


def _resolved_max_attempts(value: int | None) -> int:
    if value is not None:
        return max(1, int(value))
    return _int_env("AZURE_MEMFOREST_PROJECTION_WRITE_MAX_ATTEMPTS", DEFAULT_MAX_ATTEMPTS)


def _max_retry_delay_seconds() -> float:
    return _float_env("AZURE_MEMFOREST_PROJECTION_WRITE_MAX_RETRY_DELAY_SECONDS", DEFAULT_MAX_RETRY_DELAY_SECONDS)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(0.1, float(raw))
    except ValueError:
        return default
