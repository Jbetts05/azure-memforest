from __future__ import annotations

import asyncio

import pytest

from src.cloud.async_batch import run_bounded


class TooManyRequestsError(Exception):
    status_code = 429

    def __init__(self, message: str = "TooManyRequests", *, retry_after_in_milliseconds: int = 1):
        super().__init__(message)
        self.retry_after_in_milliseconds = retry_after_in_milliseconds


@pytest.mark.asyncio
async def test_run_bounded_limits_chunk_concurrency() -> None:
    active = 0
    max_active = 0
    seen: list[int] = []

    async def worker(item: int) -> None:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        seen.append(item)
        active -= 1

    await run_bounded(list(range(7)), worker, concurrency=3)

    assert sorted(seen) == list(range(7))
    assert max_active == 3


@pytest.mark.asyncio
async def test_run_bounded_concurrency_one_is_sequential() -> None:
    active = 0
    max_active = 0

    async def worker(_item: int) -> None:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1

    await run_bounded(list(range(4)), worker, concurrency=1)

    assert max_active == 1


@pytest.mark.asyncio
async def test_run_bounded_waits_for_started_chunk_on_failure() -> None:
    completed: list[int] = []

    async def worker(item: int) -> None:
        await asyncio.sleep(0.01)
        if item == 1:
            raise RuntimeError("boom")
        completed.append(item)

    with pytest.raises(RuntimeError, match="boom"):
        await run_bounded([0, 1, 2, 3], worker, concurrency=4)

    assert completed == [0, 2, 3]


@pytest.mark.asyncio
async def test_run_bounded_retries_throttled_items() -> None:
    attempts: dict[int, int] = {}

    async def worker(item: int) -> None:
        attempts[item] = attempts.get(item, 0) + 1
        if item == 1 and attempts[item] == 1:
            raise TooManyRequestsError()

    await run_bounded([0, 1, 2], worker, concurrency=2)

    assert attempts == {0: 1, 1: 2, 2: 1}
