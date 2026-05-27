"""Shared schema/versioning invariants for cloud-backed MemForest state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SCHEMA_VERSION = 1
EmbeddingRole = Literal["default", "query", "tree_summary", "fact", "browse_node"]
MigrationStatus = Literal["planned", "running", "succeeded", "failed"]


@dataclass(frozen=True, slots=True)
class CloudRecordMetadata:
    schema_version: int
    tenant_id: str
    forest_version: str
    user_id: str | None = None
    created_at_unix: float | None = None
    updated_at_unix: float | None = None


@dataclass(frozen=True, slots=True)
class EmbeddingMetadata:
    embedding_model_version: str
    embedding_dimension: int
    embedding_role: EmbeddingRole = "default"


@dataclass(frozen=True, slots=True)
class DeletionMetadata:
    tombstoned_at_unix: float
    deletion_reason: str
    hard_delete_after_unix: float | None = None


@dataclass(frozen=True, slots=True)
class MigrationRecord:
    migration_id: str
    from_schema_version: int
    to_schema_version: int
    status: MigrationStatus
    started_at_unix: float | None = None
    completed_at_unix: float | None = None


def require_tenant_scope(metadata: CloudRecordMetadata) -> None:
    if metadata.schema_version != SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {SCHEMA_VERSION}.")
    if not metadata.tenant_id:
        raise ValueError("tenant_id is required for cloud records.")
    if not metadata.forest_version:
        raise ValueError("forest_version is required for cloud records.")


def partition_key(*, tenant_id: str, user_id: str | None = None) -> str:
    if not tenant_id:
        raise ValueError("tenant_id is required for partition keys.")
    if ":" in tenant_id:
        raise ValueError("tenant_id must not contain ':'.")
    if user_id is not None and ":" in user_id:
        raise ValueError("user_id must not contain ':'.")
    return tenant_id if user_id is None else f"{tenant_id}:{user_id}"
