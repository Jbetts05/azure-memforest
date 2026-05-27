"""Azure Blob-backed TreeSnapshotStore adapter."""

from __future__ import annotations

import re
from typing import Any

_SNAPSHOT_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$")
_METADATA_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


class BlobTreeSnapshotStore:
    """Synchronous Blob Storage snapshot adapter.

    `read_snapshot` loads the full blob into memory. Add a streaming protocol
    before using this for very large snapshots.
    """

    def __init__(self, container_client: Any) -> None:
        self._container = container_client

    def write_snapshot(self, *, snapshot_id: str, payload: bytes, metadata: dict[str, Any]) -> None:
        blob_name = validate_snapshot_id(snapshot_id)
        safe_metadata = validate_blob_metadata(metadata)
        blob = self._container.get_blob_client(blob_name)
        try:
            from azure.storage.blob import ContentSettings

            content_settings = ContentSettings(content_type="application/octet-stream")
        except Exception:
            content_settings = None
        kwargs = {
            "data": payload,
            "overwrite": False,
            "metadata": safe_metadata,
        }
        if content_settings is not None:
            kwargs["content_settings"] = content_settings
        blob.upload_blob(**kwargs)

    def read_snapshot(self, snapshot_id: str) -> bytes:
        blob_name = validate_snapshot_id(snapshot_id)
        blob = self._container.get_blob_client(blob_name)
        try:
            return blob.download_blob().readall()
        except Exception as exc:
            if _is_blob_not_found(exc):
                raise KeyError(snapshot_id) from exc
            raise


def blob_snapshot_store_from_env(environ: dict[str, str], *, credential: Any | None = None) -> BlobTreeSnapshotStore | None:
    endpoint = str(environ.get("AZURE_STORAGE_BLOB_ENDPOINT", "")).strip()
    container = str(environ.get("AZURE_STORAGE_SNAPSHOT_CONTAINER", "")).strip()
    client_id = str(environ.get("AZURE_CLIENT_ID", "")).strip()
    if not (endpoint and container):
        return None
    if credential is None:
        if client_id:
            from azure.identity import ManagedIdentityCredential

            credential = ManagedIdentityCredential(client_id=client_id)
        else:
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    from azure.storage.blob import BlobServiceClient

    service = BlobServiceClient(account_url=endpoint, credential=credential)
    return BlobTreeSnapshotStore(service.get_container_client(container))


def validate_snapshot_id(snapshot_id: str) -> str:
    value = str(snapshot_id or "")
    if len(value) > 1024 or not _SNAPSHOT_ID_PATTERN.fullmatch(value):
        raise ValueError("snapshot_id contains unsupported characters or path segments.")
    if any(part in {".", ".."} for part in value.split("/")):
        raise ValueError("snapshot_id must not contain relative path segments.")
    return value


def validate_blob_metadata(metadata: dict[str, Any]) -> dict[str, str]:
    safe: dict[str, str] = {}
    total_size = 0
    for key, value in metadata.items():
        if not _METADATA_KEY_PATTERN.fullmatch(str(key)):
            raise ValueError(f"Invalid Azure Blob metadata key: {key!r}")
        text = str(value)
        if not text.isascii():
            raise ValueError(f"Azure Blob metadata value for {key!r} must be ASCII.")
        total_size += len(str(key)) + len(text)
        if total_size > 8 * 1024:
            raise ValueError("Azure Blob metadata exceeds the 8 KiB limit.")
        safe[str(key)] = text
    return safe


def _is_blob_not_found(exc: Exception) -> bool:
    if exc.__class__.__name__ == "ResourceNotFoundError":
        return True
    return getattr(exc, "status_code", None) == 404
