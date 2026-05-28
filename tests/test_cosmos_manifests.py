from __future__ import annotations

import pytest

from src.cloud.cosmos_manifests import (
    CosmosDeletionLog,
    CosmosManifestStore,
    ManifestStateError,
)


class ExistsError(Exception):
    status_code = 409


class NotFoundError(Exception):
    status_code = 404


class PreconditionFailed(Exception):
    status_code = 412


class FakeTenantContainer:
    def __init__(self):
        self.items = {}
        self.fail_next_replace = False

    def create_item(self, item, *, partition_key):
        key = (partition_key, item["id"])
        if key in self.items:
            raise ExistsError()
        stored = dict(item)
        stored["_etag"] = "etag-1"
        self.items[key] = stored

    def read_item(self, *, item, partition_key):
        key = (partition_key, item)
        if key not in self.items:
            raise NotFoundError()
        return dict(self.items[key])

    def upsert_item(self, item):
        self.items[(item["tenant_id"], item["id"])] = dict(item)

    def replace_item(self, *, item, body, partition_key, etag=None, match_condition=None):
        key = (partition_key, item)
        if key not in self.items:
            raise NotFoundError()
        if self.fail_next_replace:
            self.fail_next_replace = False
            raise PreconditionFailed()
        if match_condition == "IfNotModified" and etag != self.items[key].get("_etag"):
            raise PreconditionFailed()
        updated = dict(body)
        updated["_etag"] = "etag-2"
        self.items[key] = updated

    def query_items(self, *, query, parameters, partition_key):
        params = {param["name"]: param["value"] for param in parameters}
        result = []
        for (pk, _id), item in self.items.items():
            if pk != partition_key:
                continue
            if item.get("tenant_id") != params.get("@tenant_id"):
                continue
            if "@user_id" in params and item.get("user_id") != params["@user_id"]:
                continue
            if "@generation" in params and item.get("generation") != params["@generation"]:
                continue
            if "status='committed'" in query and item.get("status") != "committed":
                continue
            result.append(dict(item))
        result.sort(key=lambda item: (item.get("generation", 0), item.get("committed_at_unix") or 0), reverse=True)
        return result


@pytest.mark.asyncio
async def test_manifest_store_create_commit_and_latest() -> None:
    store = CosmosManifestStore(FakeTenantContainer(), tenant_id="tenant-1", user_id="user-1")

    first = await store.create_pending(forest_version="v1", generation=1)
    second = await store.create_pending(forest_version="v2", generation=2)
    await store.commit_manifest(first.manifest_id)
    committed = await store.commit_manifest(second.manifest_id)
    latest = await store.latest_committed()

    assert committed.status == "committed"
    assert latest is not None
    assert latest.forest_version == "v2"


@pytest.mark.asyncio
async def test_manifest_store_create_pending_is_retry_safe() -> None:
    store = CosmosManifestStore(FakeTenantContainer(), tenant_id="tenant-1", user_id="user-1")

    first = await store.create_pending(forest_version="v1", generation=1)
    second = await store.create_pending(forest_version="v1", generation=1)

    assert first.manifest_id == second.manifest_id


@pytest.mark.asyncio
async def test_manifest_store_refuses_to_commit_non_pending() -> None:
    store = CosmosManifestStore(FakeTenantContainer(), tenant_id="tenant-1", user_id="user-1")
    manifest = await store.create_pending(forest_version="v1", generation=1)
    await store.commit_manifest(manifest.manifest_id)

    with pytest.raises(ManifestStateError):
        await store.commit_manifest(manifest.manifest_id)


@pytest.mark.asyncio
async def test_manifest_store_rejects_committed_generation_conflict() -> None:
    store = CosmosManifestStore(FakeTenantContainer(), tenant_id="tenant-1", user_id="user-1")
    first = await store.create_pending(forest_version="v1", generation=1)
    second = await store.create_pending(forest_version="v1-retry", generation=1)
    await store.commit_manifest(first.manifest_id)

    with pytest.raises(ManifestStateError):
        await store.commit_manifest(second.manifest_id)


@pytest.mark.asyncio
async def test_manifest_store_commit_uses_etag_guard() -> None:
    container = FakeTenantContainer()
    store = CosmosManifestStore(container, tenant_id="tenant-1", user_id="user-1")
    manifest = await store.create_pending(forest_version="v1", generation=1)
    container.fail_next_replace = True

    with pytest.raises(ManifestStateError):
        await store.commit_manifest(manifest.manifest_id)


@pytest.mark.asyncio
async def test_manifest_store_commit_merges_snapshot_metadata() -> None:
    store = CosmosManifestStore(FakeTenantContainer(), tenant_id="tenant-1", user_id="user-1")
    manifest = await store.create_pending(
        forest_version="v1",
        generation=1,
        metadata={"job_id": "job-1"},
    )

    committed = await store.commit_manifest(
        manifest.manifest_id,
        metadata_updates={
            "snapshot_id": "tenant-1/user-1/v1.json",
            "snapshot_content_sha256": "abc123",
        },
    )

    assert committed.metadata["job_id"] == "job-1"
    assert committed.metadata["snapshot_id"] == "tenant-1/user-1/v1.json"
    assert committed.metadata["snapshot_content_sha256"] == "abc123"


@pytest.mark.asyncio
async def test_deletion_log_records_and_lists_user_deletions() -> None:
    log = CosmosDeletionLog(FakeTenantContainer(), tenant_id="tenant-1")

    first = await log.record_deletion(
        user_id="user-1",
        deletion_type="session",
        target_id="session-1",
        reason="user_request",
    )
    await log.record_deletion(
        user_id="user-2",
        deletion_type="session",
        target_id="session-2",
        reason="user_request",
    )
    records = await log.list_deletions(user_id="user-1")

    assert len(records) == 1
    assert records[0].deletion_id == first.deletion_id
    assert records[0].status == "tombstoned"


@pytest.mark.asyncio
async def test_deletion_log_duplicate_returns_stored_record() -> None:
    log = CosmosDeletionLog(FakeTenantContainer(), tenant_id="tenant-1")

    first = await log.record_deletion(
        user_id="user-1",
        deletion_type="session",
        target_id="session-1",
        reason="original",
    )
    second = await log.record_deletion(
        user_id="user-1",
        deletion_type="session",
        target_id="session-1",
        reason="different",
    )

    assert second.deletion_id == first.deletion_id
    assert second.reason == "original"
