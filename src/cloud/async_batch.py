"""Small async batching helpers for cloud data-plane operations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

T = TypeVar("T")

DEFAULT_WRITE_CONCURRENCY = 8
DEFAULT_MAX_ATTEMPTS = 5


async def run_bounded(
    items: Sequence[T],
    worker: Callable[[T], Awaitable[None]],
    *,
    concurrency: int = DEFAULT_WRITE_CONCURRENCY,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> None:
    """Run one async worker per item in bounded chunks.

    The helper intentionally avoids creating unbounded task lists for very large
    projections. If a worker fails, the exception is propagated after the
    current chunk's already-started operations settle; callers must rely on
    idempotent writes and commit fencing for partial-write safety.
    """

    if not items:
        return
    limit = max(1, int(concurrency))
    attempts = max(1, int(max_attempts))
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
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        try:
            return max(0.1, float(retry_after))
        except (TypeError, ValueError):
            pass
    return min(8.0, 0.25 * (2 ** (attempt - 1)))
