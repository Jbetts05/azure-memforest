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
    superseded: tuple[ProjectionRecordRef, ...] = ()

    @property
    def unchanged_keys(self) -> frozenset[ProjectionRecordKey]:
        return frozenset(record.key for record in self.unchanged)

    @property
    def upsert_keys(self) -> frozenset[ProjectionRecordKey]:
        return frozenset(record.key for record in self.upsert)

    @property
    def delete_keys(self) -> frozenset[ProjectionRecordKey]:
        return frozenset(record.key for record in self.delete)


@dataclass(frozen=True, slots=True)
class AffectedTreePlan:
    affected_tree_ids: tuple[str, ...]
    affected_fact_ids: tuple[str, ...]
    dirty_node_keys: tuple[str, ...]
    dirty_root_tree_ids: tuple[str, ...]

    @property
    def affected_tree_count(self) -> int:
        return len(self.affected_tree_ids)

    @property
    def affected_fact_count(self) -> int:
        return len(self.affected_fact_ids)

    @property
    def dirty_node_count(self) -> int:
        return len(self.dirty_node_keys)

    @property
    def dirty_root_tree_count(self) -> int:
        return len(self.dirty_root_tree_ids)


@dataclass(frozen=True, slots=True)
class ProjectionTreeScope:
    full_projection_required: bool
    projected_tree_ids: tuple[str, ...]
    deleted_tree_ids: tuple[str, ...]
    omitted_tree_ids: tuple[str, ...]
    unknown_changed_tree_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PartialHydrationPlan:
    """Tree/state subset required for a future partial cloud hydration attempt."""

    full_hydration_required: bool
    tree_ids_to_load: tuple[str, ...]
    deleted_tree_ids: tuple[str, ...]
    omitted_tree_ids: tuple[str, ...]
    unknown_changed_tree_ids: tuple[str, ...] = ()


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
    superseded = [
        committed[key]
        for key, record in projected.items()
        if key in committed and not _same_content(record, committed[key])
    ]

    return ProjectionSkipPlan(
        unchanged=tuple(sorted(unchanged, key=_sort_key)),
        upsert=tuple(sorted(upsert, key=_sort_key)),
        delete=tuple(sorted(delete, key=_sort_key)),
        superseded=tuple(sorted(superseded, key=_sort_key)),
    )


def plan_projection_tree_scope(
    *,
    all_tree_ids: Iterable[str],
    changed_tree_ids: Iterable[str] | None,
    deleted_tree_ids: Iterable[str] = (),
) -> ProjectionTreeScope:
    """Plan which trees are candidates for projection in a known tree delta.

    ``changed_tree_ids=None`` means the delta is unknown, so callers must treat
    all trees as projection candidates. When the delta is known, unchanged trees
    can be omitted from future projection inputs; deleted trees are tracked
    separately because they may no longer exist in the new tree set.
    """

    all_ids = tuple(sorted({str(tree_id) for tree_id in all_tree_ids if str(tree_id)}))
    deleted = tuple(sorted({str(tree_id) for tree_id in deleted_tree_ids if str(tree_id)}))
    if changed_tree_ids is None:
        return ProjectionTreeScope(
            full_projection_required=True,
            projected_tree_ids=all_ids,
            deleted_tree_ids=deleted,
            omitted_tree_ids=(),
            unknown_changed_tree_ids=(),
        )
    changed = {str(tree_id) for tree_id in changed_tree_ids if str(tree_id)}
    projected = tuple(tree_id for tree_id in all_ids if tree_id in changed)
    omitted = tuple(tree_id for tree_id in all_ids if tree_id not in changed)
    known_deleted = set(deleted)
    unknown_changed = tuple(sorted(tree_id for tree_id in changed if tree_id not in set(all_ids) and tree_id not in known_deleted))
    return ProjectionTreeScope(
        full_projection_required=False,
        projected_tree_ids=projected,
        deleted_tree_ids=deleted,
        omitted_tree_ids=omitted,
        unknown_changed_tree_ids=unknown_changed,
    )


def plan_partial_hydration(
    *,
    all_tree_ids: Iterable[str],
    changed_tree_ids: Iterable[str] | None,
    deleted_tree_ids: Iterable[str] = (),
) -> PartialHydrationPlan:
    """Plan the tree subset needed to hydrate for an incremental mutation.

    This is intentionally conservative and side-effect free. ``changed_tree_ids``
    being ``None`` means the writer does not know the dirty-tree frontier, so a
    future partial-hydration path must fall back to full hydration. Deleted tree
    IDs are included in ``tree_ids_to_load`` in every mode because deletion and
    tombstone paths may need committed node/location metadata even when the tree
    is absent from the new active tree set. Passing ``[]`` for
    ``changed_tree_ids`` means a known no-op delta; pass ``None`` for unknown.
    """

    all_ids = tuple(sorted({str(tree_id) for tree_id in all_tree_ids if str(tree_id)}))
    deleted = tuple(sorted({str(tree_id) for tree_id in deleted_tree_ids if str(tree_id)}))
    if changed_tree_ids is None:
        return PartialHydrationPlan(
            full_hydration_required=True,
            tree_ids_to_load=tuple(sorted(set(all_ids) | set(deleted))),
            deleted_tree_ids=deleted,
            omitted_tree_ids=(),
            unknown_changed_tree_ids=(),
        )

    changed = {str(tree_id) for tree_id in changed_tree_ids if str(tree_id)}
    known_deleted = set(deleted)
    known_active = set(all_ids)
    unknown_changed = tuple(sorted(tree_id for tree_id in changed if tree_id not in known_active and tree_id not in known_deleted))
    tree_ids_to_load = tuple(sorted((changed & known_active) | known_deleted))
    omitted = tuple(tree_id for tree_id in all_ids if tree_id not in set(tree_ids_to_load))
    return PartialHydrationPlan(
        full_hydration_required=bool(unknown_changed),
        tree_ids_to_load=tuple(sorted(set(all_ids) | known_deleted)) if unknown_changed else tree_ids_to_load,
        deleted_tree_ids=deleted,
        omitted_tree_ids=() if unknown_changed else omitted,
        unknown_changed_tree_ids=unknown_changed,
    )


def filter_projection_refs_to_tree_scope(
    records: Iterable[ProjectionRecordRef],
    *,
    tree_ids: Iterable[str],
) -> tuple[ProjectionRecordRef, ...]:
    """Return refs whose metadata tree_id is in scope.

    This helper is for future incremental projection inputs. It intentionally
    drops refs with no tree_id metadata so unsupported/shared surfaces cannot be
    accidentally driven by a tree-scoped plan.
    """

    scoped = {str(tree_id) for tree_id in tree_ids if str(tree_id)}
    return tuple(
        record
        for record in records
        if str(record.metadata.get("tree_id") or "") in scoped
    )


def plan_affected_trees(projection_skip_plans: Mapping[str, ProjectionSkipPlan]) -> AffectedTreePlan:
    """Derive affected tree/fact/node sets from projection delta plans.

    This is a pure planning helper: it only interprets ProjectionRecordRef
    metadata. It intentionally returns complete sorted ID tuples; callers that
    persist telemetry may truncate their own serialized copies. Superseded refs
    contribute their prior committed IDs so future invalidation paths can dirty
    both old and new locations. Dirty root tree IDs are intentionally derived
    from recall-document deltas only.
    """

    dirty_node_refs = _dirty_record_refs(projection_skip_plans.get("tree_node_membership"))
    dirty_location_refs = _dirty_record_refs(projection_skip_plans.get("fact_location"))
    dirty_recall_refs = _dirty_record_refs(projection_skip_plans.get("recall_document"))
    affected_tree_ids = sorted(
        {
            str(ref.metadata.get("tree_id") or "")
            for ref in [*dirty_node_refs, *dirty_location_refs, *dirty_recall_refs]
            if str(ref.metadata.get("tree_id") or "")
        }
    )
    affected_fact_ids = sorted(
        {
            str(ref.metadata.get("fact_id") or "")
            for ref in dirty_location_refs
            if str(ref.metadata.get("fact_id") or "")
        }
    )
    dirty_node_keys = sorted(
        {
            f"{ref.metadata.get('tree_id')}/{ref.metadata.get('node_id')}"
            for ref in dirty_node_refs
            if ref.metadata.get("tree_id") and ref.metadata.get("node_id")
        }
    )
    dirty_root_tree_ids = sorted(
        {
            str(ref.metadata.get("tree_id") or "")
            for ref in dirty_recall_refs
            if str(ref.metadata.get("tree_id") or "")
        }
    )
    return AffectedTreePlan(
        affected_tree_ids=tuple(affected_tree_ids),
        affected_fact_ids=tuple(affected_fact_ids),
        dirty_node_keys=tuple(dirty_node_keys),
        dirty_root_tree_ids=tuple(dirty_root_tree_ids),
    )


def _dirty_record_refs(plan: ProjectionSkipPlan | None) -> tuple[ProjectionRecordRef, ...]:
    if plan is None:
        return ()
    return (*plan.upsert, *plan.delete, *plan.superseded)


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


def tree_node_body_content_id(*, tree_id: str, node_id: str, content_sha256: str, tenant_user_key: str = "") -> str:
    """Return the durable tree-node body id for a content-addressed node body."""

    raw = "\x1f".join((tenant_user_key, tree_id, node_id, content_sha256))
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
