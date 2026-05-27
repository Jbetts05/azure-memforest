from __future__ import annotations

import pytest

from src.cloud.blob_snapshots import (
    BlobTreeSnapshotStore,
    blob_snapshot_store_from_env,
    validate_blob_metadata,
    validate_snapshot_id,
)


class NotFoundError(Exception):
    status_code = 404


class FakeDownload:
    def __init__(self, data: bytes):
        self._data = data

    def readall(self) -> bytes:
        return self._data


class FakeBlob:
    def __init__(self, name: str, blobs: dict[str, dict]):
        self.name = name
        self._blobs = blobs

    def upload_blob(self, **kwargs):
        if not kwargs.get("overwrite", False) and self.name in self._blobs:
            raise RuntimeError("already exists")
        self._blobs[self.name] = {
            "data": kwargs["data"],
            "metadata": kwargs.get("metadata", {}),
            "content_settings": kwargs.get("content_settings"),
        }

    def download_blob(self):
        if self.name not in self._blobs:
            raise NotFoundError()
        return FakeDownload(self._blobs[self.name]["data"])


class FakeContainer:
    def __init__(self):
        self.blobs: dict[str, dict] = {}

    def get_blob_client(self, name: str) -> FakeBlob:
        return FakeBlob(name, self.blobs)


def test_blob_snapshot_store_round_trips_payload_and_metadata() -> None:
    container = FakeContainer()
    store = BlobTreeSnapshotStore(container)

    store.write_snapshot(
        snapshot_id="tenant/user/snapshot-1.json",
        payload=b"payload",
        metadata={"schema_version": "1", "tenant_id": "tenant-1"},
    )

    assert store.read_snapshot("tenant/user/snapshot-1.json") == b"payload"
    assert container.blobs["tenant/user/snapshot-1.json"]["metadata"]["tenant_id"] == "tenant-1"


def test_blob_snapshot_store_missing_snapshot_raises_key_error() -> None:
    store = BlobTreeSnapshotStore(FakeContainer())

    with pytest.raises(KeyError):
        store.read_snapshot("missing")


def test_snapshot_id_validation_rejects_unsafe_names() -> None:
    assert validate_snapshot_id("tenant/user/snapshot.json") == "tenant/user/snapshot.json"
    for value in ("", "/leading", "trailing/", "a/../b", "bad\\path", "bad path"):
        with pytest.raises(ValueError):
            validate_snapshot_id(value)


def test_blob_metadata_validation_rejects_lossy_coercion() -> None:
    assert validate_blob_metadata({"schema_version": 1}) == {"schema_version": "1"}
    with pytest.raises(ValueError):
        validate_blob_metadata({"bad-key": "value"})
    with pytest.raises(ValueError):
        validate_blob_metadata({"good_key": "ümlaut"})


def test_blob_snapshot_store_from_env_requires_endpoint_and_container() -> None:
    assert blob_snapshot_store_from_env({}) is None
