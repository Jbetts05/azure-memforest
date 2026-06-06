"""Small async batching helpers for cloud data-plane operations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

T = TypeVar("T")

DEFAULT_WRITE_CONCURRENCY = 32


async def run_bounded(
    items: Sequence[T],
    worker: Callable[[T], Awaitable[None]],
    *,
    concurrency: int = DEFAULT_WRITE_CONCURRENCY,
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
    if limit == 1:
        for item in items:
            await worker(item)
        return
    for index in range(0, len(items), limit):
        chunk = items[index : index + limit]
        await asyncio.gather(*(worker(item) for item in chunk))
