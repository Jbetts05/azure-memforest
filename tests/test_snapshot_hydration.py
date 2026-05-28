from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

from src.worker.snapshot_hydration import (
    ArchiveHydratingUserForestCore,
    build_user_forest_archive,
    restore_user_forest_archive,
    restore_user_forest_from_manifest,
)


class FakeForest:
    def __init__(self, snapshot_dir: Path):
        self.snapshot_dir = snapshot_dir
        self.saved = 0
        self.loaded = 0
        self.reset_loaded = 0
        self.state = {"stale": True}
        self.ingested = []
        self.deleted = []

    def save(self):
        self.saved += 1
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        (self.snapshot_dir / "state.txt").write_text("saved", encoding="utf-8")

    def load(self):
        self.loaded += 1
        self.state["loaded"] = (self.snapshot_dir / "state.txt").read_text(encoding="utf-8")

    def reset_and_load(self):
        self.reset_loaded += 1
        self.state.clear()
        self.load()

    def ingest_session(self, session_id, turns):
        self.ingested.append((session_id, turns))
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        (self.snapshot_dir / "state.txt").write_text(f"ingested:{session_id}", encoding="utf-8")

    def delete_session(self, session_id):
        self.deleted.append(session_id)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        (self.snapshot_dir / "state.txt").write_text(f"deleted:{session_id}", encoding="utf-8")

    def snapshot_for_materialization(self):
        return {"state": dict(self.state), "ingested": list(self.ingested), "deleted": list(self.deleted)}


class FakeSnapshotStore:
    def __init__(self, payloads):
        self.payloads = payloads

    def read_snapshot(self, snapshot_id):
        return self.payloads[snapshot_id]


class FakeManifest:
    def __init__(self, metadata):
        self.metadata = metadata


def test_user_forest_archive_round_trips_and_clears_stale_files(tmp_path: Path) -> None:
    source = FakeForest(tmp_path / "source")
    archive = build_user_forest_archive(source)
    target = FakeForest(tmp_path / "target")
    target.snapshot_dir.mkdir()
    (target.snapshot_dir / "stale.txt").write_text("stale", encoding="utf-8")

    restore_user_forest_archive(target, archive)

    assert target.reset_loaded == 1
    assert target.state == {"loaded": "saved"}
    assert (target.snapshot_dir / "state.txt").read_text(encoding="utf-8") == "saved"
    assert not (target.snapshot_dir / "stale.txt").exists()


def test_restore_rejects_path_traversal(tmp_path: Path) -> None:
    forest = FakeForest(tmp_path / "target")
    archive = _archive_with_member("../escape.txt", b"bad")

    with pytest.raises(ValueError):
        restore_user_forest_archive(forest, archive)

    assert not (tmp_path / "escape.txt").exists()


def test_restore_rejects_symlink_members(tmp_path: Path) -> None:
    forest = FakeForest(tmp_path / "target")
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        info = tarfile.TarInfo("link")
        info.type = tarfile.SYMTYPE
        info.linkname = "../outside"
        archive.addfile(info)

    with pytest.raises(ValueError):
        restore_user_forest_archive(forest, buffer.getvalue())


def test_restore_from_manifest_verifies_snapshot_hash(tmp_path: Path) -> None:
    source = FakeForest(tmp_path / "source")
    archive = build_user_forest_archive(source)
    manifest = FakeManifest(
        {
            "snapshot_id": "tenant/user/g1.json",
            "snapshot_content_sha256": __import__("hashlib").sha256(archive).hexdigest(),
        }
    )
    target = FakeForest(tmp_path / "target")

    restore_user_forest_from_manifest(target, FakeSnapshotStore({"tenant/user/g1.json": archive}), manifest)

    assert target.state == {"loaded": "saved"}


def test_restore_from_manifest_rejects_hash_mismatch(tmp_path: Path) -> None:
    forest = FakeForest(tmp_path / "target")
    manifest = FakeManifest({"snapshot_id": "snap", "snapshot_content_sha256": "bad"})

    with pytest.raises(ValueError, match="hash"):
        restore_user_forest_from_manifest(forest, FakeSnapshotStore({"snap": b"not matching"}), manifest)


def test_archive_hydrating_core_restores_then_applies_ingest(tmp_path: Path) -> None:
    source = FakeForest(tmp_path / "source")
    archive = build_user_forest_archive(source)
    manifest = FakeManifest(
        {
            "snapshot_id": "tenant/user/g1.json",
            "snapshot_content_sha256": __import__("hashlib").sha256(archive).hexdigest(),
        }
    )
    target = FakeForest(tmp_path / "target")
    core = ArchiveHydratingUserForestCore(
        user_forest=target,
        snapshot_store=FakeSnapshotStore({"tenant/user/g1.json": archive}),
    )

    core.hydrate_from_manifest(manifest)
    snapshot = core.ingest_session(session_id="session-2", turns=[{"role": "user", "content": "hello"}], metadata={})

    assert snapshot["state"]["loaded"] == "saved"
    assert snapshot["ingested"][0][0] == "session-2"
    assert core.snapshot_payload()


def _archive_with_member(name: str, payload: bytes) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        info = tarfile.TarInfo(name)
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()
