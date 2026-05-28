"""UserForest archive snapshots for worker hydration."""

from __future__ import annotations

import io
import hashlib
import shutil
import tarfile
import uuid
from pathlib import Path
from typing import Any


class ArchiveHydratingUserForestCore:
    """Sync core wrapper that restores committed snapshots before mutation."""

    def __init__(self, *, user_forest: Any, snapshot_store: Any) -> None:
        self.user_forest = user_forest
        self.snapshot_store = snapshot_store

    def hydrate_from_manifest(self, manifest: Any) -> None:
        restore_user_forest_from_manifest(self.user_forest, self.snapshot_store, manifest)

    def ingest_session(self, *, session_id: str, turns: list[dict[str, Any]], metadata: dict[str, Any]) -> Any:
        self.user_forest.ingest_session(session_id, turns)
        return self.user_forest.snapshot_for_materialization()

    def delete_session(self, *, session_id: str) -> Any:
        self.user_forest.delete_session(session_id)
        return self.user_forest.snapshot_for_materialization()

    def snapshot_for_materialization(self) -> Any:
        return self.user_forest.snapshot_for_materialization()

    def snapshot_payload(self) -> bytes:
        return build_user_forest_archive(self.user_forest)


def build_user_forest_archive(user_forest: Any) -> bytes:
    """Persist and archive a UserForest snapshot directory."""

    snapshot_dir = Path(user_forest.snapshot_dir)
    lock = getattr(user_forest, "_lock", None)
    if lock is None:
        user_forest.save()
        return _archive_dir(snapshot_dir)
    with lock:
        user_forest.save()
        return _archive_dir(snapshot_dir)


def restore_user_forest_archive(user_forest: Any, payload: bytes) -> None:
    """Replace a UserForest snapshot directory from archive bytes and reload it."""

    snapshot_dir = Path(user_forest.snapshot_dir)
    parent = snapshot_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = parent / f".{snapshot_dir.name}.restore-{uuid.uuid4().hex}"
    backup_dir = parent / f".{snapshot_dir.name}.backup-{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True)
    try:
        _extract_archive(payload, tmp_dir)
        had_existing = snapshot_dir.exists()
        if had_existing:
            snapshot_dir.rename(backup_dir)
        tmp_dir.rename(snapshot_dir)
        try:
            _reset_and_load(user_forest)
        except Exception:
            if snapshot_dir.exists():
                shutil.rmtree(snapshot_dir)
            if had_existing and backup_dir.exists():
                backup_dir.rename(snapshot_dir)
            raise
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)


def restore_user_forest_from_manifest(user_forest: Any, snapshot_store: Any, manifest: Any) -> None:
    """Restore the UserForest archive referenced by a committed manifest."""

    metadata = getattr(manifest, "metadata", {}) or {}
    _validate_manifest_fingerprint_metadata(metadata)
    snapshot_id = str(metadata.get("snapshot_id", "")).strip()
    expected_hash = str(metadata.get("snapshot_content_sha256", "")).strip()
    if not snapshot_id or not expected_hash:
        raise ValueError("manifest does not contain snapshot_id and snapshot_content_sha256 metadata")
    payload = snapshot_store.read_snapshot(snapshot_id)
    actual_hash = hashlib.sha256(payload).hexdigest()
    if actual_hash != expected_hash:
        raise ValueError("snapshot content hash does not match manifest metadata")
    restore_user_forest_archive(user_forest, payload)


def _validate_manifest_fingerprint_metadata(metadata: dict[str, Any]) -> None:
    embedding_model_version = str(metadata.get("embedding_model_version", "")).strip()
    fingerprint = str(metadata.get("materialization_fingerprint", "")).strip()
    if not embedding_model_version:
        raise ValueError("manifest does not contain embedding_model_version metadata")
    if not fingerprint:
        raise ValueError("manifest does not contain materialization_fingerprint metadata")


def _archive_dir(snapshot_dir: Path) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        if snapshot_dir.exists():
            for path in sorted(snapshot_dir.rglob("*")):
                if path.is_file():
                    archive.add(path, arcname=path.relative_to(snapshot_dir).as_posix(), recursive=False)
    return buffer.getvalue()


def _extract_archive(payload: bytes, destination: Path) -> None:
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            _validate_member(member, destination)
        for member in members:
            archive.extract(member, path=destination, filter="data")


def _validate_member(member: tarfile.TarInfo, destination: Path) -> None:
    if member.issym() or member.islnk() or member.isdev() or member.ischr() or member.isblk() or member.isfifo():
        raise ValueError(f"unsupported archive member type: {member.name}")
    target = destination / member.name
    if Path(member.name).is_absolute() or ".." in Path(member.name).parts:
        raise ValueError(f"unsafe archive member path: {member.name}")
    if not _is_relative_to(target.resolve(), destination.resolve()):
        raise ValueError(f"archive member escapes destination: {member.name}")


def _reset_and_load(user_forest: Any) -> None:
    reset_and_load = getattr(user_forest, "reset_and_load", None)
    if reset_and_load is not None:
        reset_and_load()
        return
    user_forest.load()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False
