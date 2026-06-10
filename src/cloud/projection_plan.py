"""Pure planning helpers for skipping unchanged projection writes."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Iterable, Mapping


@dataclass(frozen=True, slots=True, order=True)
class ProjectionRecordKey:
    record_kind: str
    logical_id: str

    def __post_init__(self) -> None:
        if not self.record_kind:
            raise ValueError("record_kind is required")
        if not self.logical_id:
            raise ValueError("logical_id is required")


@dataclass(frozen=True, slots=True)
class ProjectionRecordRef:
    """Stable metadata for one projected record.

    ``logical_id`` is the write/delete key within ``record_kind``. ``content_id``
    is for records whose immutable body ID already encodes content, while
    ``content_hash`` is for records that expose a separate content digest.
    """

    record_kind: str
    logical_id: str
    content_hash: str = ""
    content_id: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        ProjectionRecordKey(self.record_kind, self.logical_id)
        object.__setattr__(self, "content_hash", str(self.content_hash or ""))
        object.__setattr__(self, "content_id", str(self.content_id or ""))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    @property
    def key(self) -> ProjectionRecordKey:
        return ProjectionRecordKey(self.record_kind, self.logical_id)


@dataclass(frozen=True, slots=True)
class ProjectionSkipPlan:
    unchanged: tuple[ProjectionRecordRef, ...]
    upsert: tuple[ProjectionRecordRef, ...]
    delete: tuple[ProjectionRecordRef, ...]

    @property
    def unchanged_keys(self) -> frozenset[ProjectionRecordKey]:
        return frozenset(record.key for record in self.unchanged)

    @property
    def upsert_keys(self) -> frozenset[ProjectionRecordKey]:
        return frozenset(record.key for record in self.upsert)

    @property
    def delete_keys(self) -> frozenset[ProjectionRecordKey]:
        return frozenset(record.key for record in self.delete)


def plan_projection_skip(
    projected_records: Iterable[ProjectionRecordRef],
    retained_committed_records: Iterable[ProjectionRecordRef],
) -> ProjectionSkipPlan:
    """Split projected records into unchanged/upsert/delete groups.

    The function is side-effect free: callers pass the newly projected metadata
    and the retained metadata from committed state. A projected record is
    unchanged when its logical key exists in committed state and the stable
    content identifier, or otherwise content hash, is equal. Committed records
    absent from the projection are returned as deletes.
    """

    projected = _index_unique(projected_records, label="projected_records")
    committed = _index_unique(retained_committed_records, label="retained_committed_records")

    unchanged: list[ProjectionRecordRef] = []
    upsert: list[ProjectionRecordRef] = []
    for key, record in projected.items():
        previous = committed.get(key)
        if previous is not None and _same_content(record, previous):
            unchanged.append(record)
        else:
            upsert.append(record)

    delete = [record for key, record in committed.items() if key not in projected]

    return ProjectionSkipPlan(
        unchanged=tuple(sorted(unchanged, key=_sort_key)),
        upsert=tuple(sorted(upsert, key=_sort_key)),
        delete=tuple(sorted(delete, key=_sort_key)),
    )


def _index_unique(records: Iterable[ProjectionRecordRef], *, label: str) -> dict[ProjectionRecordKey, ProjectionRecordRef]:
    indexed: dict[ProjectionRecordKey, ProjectionRecordRef] = {}
    for record in records:
        if not isinstance(record, ProjectionRecordRef):
            raise TypeError(f"{label} must contain ProjectionRecordRef values")
        if not record.content_id and not record.content_hash:
            raise ValueError(f"{label} record {record.key!r} needs content_id or content_hash")
        if record.key in indexed:
            raise ValueError(f"{label} contains duplicate record key {record.key!r}")
        indexed[record.key] = record
    return indexed


def _same_content(projected: ProjectionRecordRef, committed: ProjectionRecordRef) -> bool:
    if projected.content_id or committed.content_id:
        return bool(projected.content_id) and projected.content_id == committed.content_id
    return bool(projected.content_hash) and projected.content_hash == committed.content_hash


def _sort_key(record: ProjectionRecordRef) -> tuple[str, str]:
    return (record.record_kind, record.logical_id)


def tree_node_body_content_id(*, tree_id: str, node_id: str, content_sha256: str) -> str:
    """Return the durable tree-node body id for a content-addressed node body."""

    raw = "\x1f".join(("tree_node_body", tree_id, node_id, content_sha256))
    return f"tree_node_body_{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


def browse_embedding_content_hash(
    *,
    node_id: str,
    tree_id: str,
    tree_type: str,
    embedding_sha256: str = "",
    embedding: Iterable[float] | None = None,
) -> str:
    """Return the stable logical content hash for a browse embedding row."""

    if not embedding_sha256:
        if embedding is None:
            raise ValueError("embedding_sha256 or embedding is required")
        embedding_sha256 = _embedding_sha256(tuple(float(value) for value in embedding))
    payload = "\x1f".join(("browse_embedding", str(node_id), str(tree_id), str(tree_type), str(embedding_sha256)))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _embedding_sha256(embedding: tuple[float, ...]) -> str:
    raw = ",".join(repr(float(value)) for value in embedding)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
