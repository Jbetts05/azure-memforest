from __future__ import annotations

import time

import pytest

from src.cloud.cosmos_leases import CosmosUserLeaseStore, LeaseConflict


class ExistsError(Exception):
    status_code = 409


class NotFoundError(Exception):
    status_code = 404


class PreconditionFailed(Exception):
    status_code = 412


class FakeLeaseContainer:
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
        return stored

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
        stored["_etag"] = f"etag-{len(self.items) + 2}"
        self.items[key] = stored
        return stored


def _store(container=None):
    return CosmosUserLeaseStore(container or FakeLeaseContainer(), tenant_id="tenant-1", user_id="user-1")


@pytest.mark.asyncio
async def test_acquire_creates_user_lease() -> None:
    store = _store()

    lease = await store.acquire(owner_id="worker-1", ttl_seconds=60)

    assert lease.owner_id == "worker-1"
    assert lease.fencing_token == "1"
    await store.assert_current(lease)


@pytest.mark.asyncio
async def test_acquire_conflicts_when_other_owner_holds_unexpired_lease() -> None:
    store = _store()
    await store.acquire(owner_id="worker-1", ttl_seconds=60)

    with pytest.raises(LeaseConflict):
        await store.acquire(owner_id="worker-2", ttl_seconds=60)


@pytest.mark.asyncio
async def test_same_owner_reacquire_keeps_unexpired_token() -> None:
    store = _store()
    first = await store.acquire(owner_id="worker-1", ttl_seconds=60)

    second = await store.acquire(owner_id="worker-1", ttl_seconds=60)

    assert second.fencing_token == first.fencing_token
    assert second.expires_at_unix >= first.expires_at_unix


@pytest.mark.asyncio
async def test_expired_lease_can_be_acquired_with_incremented_token() -> None:
    container = FakeLeaseContainer()
    store = _store(container)
    first = await store.acquire(owner_id="worker-1", ttl_seconds=60)
    key = (store.tenant_user_key, first.lease_id)
    container.items[key]["expires_at_unix"] = time.time() - 1

    second = await store.acquire(owner_id="worker-2", ttl_seconds=60)

    assert second.owner_id == "worker-2"
    assert second.fencing_token == "2"


@pytest.mark.asyncio
async def test_renew_and_release_are_fenced() -> None:
    store = _store()
    lease = await store.acquire(owner_id="worker-1", ttl_seconds=60)

    renewed = await store.renew(lease, ttl_seconds=120)
    await store.release(renewed)

    with pytest.raises(LeaseConflict):
        await store.assert_current(renewed)


@pytest.mark.asyncio
async def test_precondition_failure_raises_lease_conflict() -> None:
    container = FakeLeaseContainer()
    store = _store(container)
    await store.acquire(owner_id="worker-1", ttl_seconds=60)
    container.fail_next_replace = True

    with pytest.raises(LeaseConflict):
        await store.acquire(owner_id="worker-1", ttl_seconds=60)
