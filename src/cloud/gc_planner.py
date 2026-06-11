"""Pure GC planning helpers for versioned cloud projection records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True, slots=True)
class ProjectionGcPolicy:
    enabled: bool = False
    retention_count: int = 0
    live_delete_enabled: bool = False

    def __post_init__(self) -> None:
        if self.retention_count < 0:
            raise ValueError("retention_count must not be negative")
        if self.enabled and self.retention_count < 1:
            raise ValueError("retention_count must be positive when projection GC is enabled")
        if self.live_delete_enabled and not self.enabled:
            raise ValueError("live_delete_enabled requires projection GC to be enabled")


@dataclass(frozen=True, slots=True)
class ManifestRef:
    forest_version: str
    generation: int
    status: str = "committed"


@dataclass(frozen=True, slots=True)
class MvccProjectionRecordRef:
    record_id: str
    record_kind: str
    valid_from_generation: int | None
    valid_from_forest_version: str | None
    valid_to_generation: int | None = None
    valid_to_forest_version: str | None = None


@dataclass(frozen=True, slots=True)
class ProjectionGcPlan:
    retained_forest_versions: frozenset[str]
    dropped_forest_versions: frozenset[str]
    deletable_body_ids: frozenset[str]
    deletable_record_ids: frozenset[str] = frozenset()
    policy_enabled: bool = False
    retention_count: int = 0
    live_delete_enabled: bool = False


def plan_projection_gc(
    manifests: Iterable[ManifestRef],
    *,
    retention_count: int,
    all_body_ids: Iterable[str],
    retained_membership_body_ids: Iterable[str],
    mvcc_records: Iterable[MvccProjectionRecordRef] = (),
    live_delete_enabled: bool = False,
) -> ProjectionGcPlan:
    """Plan safe projection GC without touching storage.

    Body IDs are retained if referenced by any retained membership across the
    retained forest-version set. Browse embeddings are intentionally excluded
    because they are logical rows keyed by node/model, not versioned bodies.
    """

    if retention_count < 1:
        raise ValueError("retention_count must be positive")
    committed = sorted(
        (manifest for manifest in manifests if manifest.status == "committed"),
        key=lambda item: item.generation,
        reverse=True,
    )
    retained = frozenset(manifest.forest_version for manifest in committed[:retention_count])
    dropped = frozenset(manifest.forest_version for manifest in committed[retention_count:])
    retained_manifests = tuple(committed[:retention_count])
    referenced = {body_id for body_id in retained_membership_body_ids if body_id}
    deletable = frozenset(body_id for body_id in all_body_ids if body_id and body_id not in referenced)
    deletable_records = frozenset(
        record.record_id
        for record in mvcc_records
        if record.record_id and not _record_visible_in_any_retained_manifest(record, retained_manifests)
    )
    return ProjectionGcPlan(
        retained_forest_versions=retained,
        dropped_forest_versions=dropped,
        deletable_body_ids=deletable,
        deletable_record_ids=deletable_records,
        policy_enabled=True,
        retention_count=retention_count,
        live_delete_enabled=live_delete_enabled,
    )


def plan_projection_gc_for_policy(
    manifests: Iterable[ManifestRef],
    *,
    policy: ProjectionGcPolicy,
    all_body_ids: Iterable[str],
    retained_membership_body_ids: Iterable[str],
    mvcc_records: Iterable[MvccProjectionRecordRef] = (),
) -> ProjectionGcPlan:
    """Plan projection GC only when an explicit policy enables it."""

    if not policy.enabled:
        return ProjectionGcPlan(
            retained_forest_versions=frozenset(),
            dropped_forest_versions=frozenset(),
            deletable_body_ids=frozenset(),
            policy_enabled=False,
            retention_count=policy.retention_count,
            live_delete_enabled=False,
        )
    return plan_projection_gc(
        manifests,
        retention_count=policy.retention_count,
        all_body_ids=all_body_ids,
        retained_membership_body_ids=retained_membership_body_ids,
        mvcc_records=mvcc_records,
        live_delete_enabled=policy.live_delete_enabled,
    )


def _record_visible_in_any_retained_manifest(record: MvccProjectionRecordRef, retained_manifests: tuple[ManifestRef, ...]) -> bool:
    return any(_record_visible_in_manifest(record, manifest) for manifest in retained_manifests)


def _record_visible_in_manifest(record: MvccProjectionRecordRef, manifest: ManifestRef) -> bool:
    if record.valid_from_generation is None:
        return True
    from_generation = int(record.valid_from_generation)
    from_version = str(record.valid_from_forest_version or "")
    if from_generation > manifest.generation:
        return False
    if from_generation == manifest.generation and from_version != manifest.forest_version:
        return False
    if record.valid_to_generation is None:
        return True
    to_generation = int(record.valid_to_generation)
    to_version = str(record.valid_to_forest_version or "")
    if to_generation > manifest.generation:
        return True
    if to_generation < manifest.generation:
        return False
    return to_version != manifest.forest_version
