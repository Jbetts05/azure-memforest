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


@dataclass(frozen=True, slots=True)
class EntityRouteRecord:
    entity_key: str
    tree_id: str
    state: str
    distinct_fact_count: int
    distinct_session_count: int


@dataclass(frozen=True, slots=True)
class SceneRouteRecord:
    cluster_id: str
    tree_id: str
    scope_key: str
    fact_count: int
    centroid_hash: str


@dataclass(frozen=True, slots=True)
class RouteIndexManifest:
    """Durable router metadata needed before authoritative partial hydration."""

    embedding_model_version: str
    entity_active_min_facts: int
    entity_active_min_sessions: int
    scene_assign_threshold: float
    scene_secondary_threshold: float
    scene_spawn_threshold: float
    scene_max_cluster_size: int
    scene_merge_threshold: float
    scene_max_time_gap_days: float
    scene_bootstrap_initialized: bool
    scene_scores_include_time_bonus: bool
    entity_records: tuple[EntityRouteRecord, ...] = ()
    scene_records: tuple[SceneRouteRecord, ...] = ()
    cluster_merge_possible: bool = False
    suppression_flip_possible: bool = False
    cluster_merge_currently_possible: bool = False
    suppression_currently_possible: bool = False
    cluster_merge_pair_count: int = 0
    suppression_candidate_count: int = 0


@dataclass(frozen=True, slots=True)
class FactRouteHint:
    """Pre-ingest routing hints for one newly extracted/canonicalized fact."""

    fact_id: str
    entity_keys: tuple[str, ...] = ()
    scene_similarities: Mapping[str, float] = field(default_factory=dict)
    scene_scope_key: str = ""
    new_session_id: str = ""
    cluster_merge_possible: bool = False
    suppression_flip_possible: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "entity_keys", tuple(sorted({str(key) for key in self.entity_keys if str(key)})))
        object.__setattr__(self, "scene_similarities", MappingProxyType({str(k): float(v) for k, v in dict(self.scene_similarities).items()}))


@dataclass(frozen=True, slots=True)
class PreIngestTreeScope:
    full_hydration_required: bool
    tree_ids_to_load: tuple[str, ...]
    escalation_reasons: tuple[str, ...]
    predicted_entity_tree_ids: tuple[str, ...]
    predicted_scene_tree_ids: tuple[str, ...]
    deleted_tree_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.full_hydration_required and not self.escalation_reasons:
            raise ValueError("full_hydration_required requires at least one escalation reason")
        predicted = set(self.predicted_entity_tree_ids) | set(self.predicted_scene_tree_ids) | set(self.deleted_tree_ids)
        if not predicted.issubset(set(self.tree_ids_to_load)):
            raise ValueError("tree_ids_to_load must include predicted entity, scene, and deleted trees")


@dataclass(frozen=True, slots=True)
class RouteRiskFlags:
    """Current router risk diagnostics; not sufficient to authorize hydration."""

    cluster_merge_possible: bool
    suppression_flip_possible: bool
    merge_pair_count: int = 0
    suppression_candidate_count: int = 0


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


def plan_pre_ingest_tree_scope(
    *,
    route_index: RouteIndexManifest,
    new_fact_route_hints: Iterable[FactRouteHint],
    embedding_model_version: str,
    deleted_tree_ids: Iterable[str] = (),
) -> PreIngestTreeScope:
    """Plan a conservative pre-ingest tree subset, or fail closed to full.

    The planner is intentionally strict. If router metadata cannot prove that no
    entity activation, scene bootstrap, scene merge, or suppression transition can
    affect an unloaded tree, it requires full hydration. ``route_index`` and
    ``new_fact_route_hints`` must be derived from one consistent router snapshot;
    stale or mixed snapshots must fail closed before calling this function.
    """

    deleted = tuple(sorted({str(tree_id) for tree_id in deleted_tree_ids if str(tree_id)}))
    entity_by_key = {record.entity_key: record for record in route_index.entity_records}
    scene_by_cluster = {record.cluster_id: record for record in route_index.scene_records}
    scene_by_scope: dict[str, list[SceneRouteRecord]] = {}
    for record in route_index.scene_records:
        scene_by_scope.setdefault(record.scope_key, []).append(record)
    reasons: set[str] = set()
    predicted_entities: set[str] = set()
    predicted_scenes: set[str] = set()
    hints = tuple(new_fact_route_hints)
    entity_fact_ids: dict[str, set[str]] = {}
    entity_session_ids: dict[str, set[str]] = {}
    for hint in hints:
        for entity_key in hint.entity_keys:
            entity_fact_ids.setdefault(entity_key, set()).add(str(hint.fact_id))
            if hint.new_session_id:
                entity_session_ids.setdefault(entity_key, set()).add(str(hint.new_session_id))

    if str(route_index.embedding_model_version or "") != str(embedding_model_version or ""):
        reasons.add("embedding_version_mismatch")
    if route_index.cluster_merge_possible:
        reasons.add("cluster_merge_possible")
    if route_index.suppression_flip_possible:
        reasons.add("suppression_flip_possible")
    if hints and not route_index.scene_bootstrap_initialized:
        reasons.add("bootstrap_uninitialized")

    for hint in hints:
        if hint.cluster_merge_possible:
            reasons.add("cluster_merge_possible")
        if hint.suppression_flip_possible:
            reasons.add("suppression_flip_possible")
    for entity_key, fact_ids in entity_fact_ids.items():
        record = entity_by_key.get(entity_key)
        if record is None:
            reasons.add("new_entity_key")
            continue
        if record.state == "active":
            predicted_entities.add(record.tree_id)
        elif _candidate_can_activate(record, route_index, new_fact_count=len(fact_ids), new_session_count=len(entity_session_ids.get(entity_key, set()))):
            reasons.add("candidate_near_activation")

    for hint in hints:
        if hint.scene_similarities:
            if not hint.scene_scope_key:
                reasons.add("missing_scene_scope_key")
                continue
            if not route_index.scene_scores_include_time_bonus:
                reasons.add("scene_scores_missing_time_bonus")
                continue
            if hint.scene_scope_key and not scene_by_scope.get(hint.scene_scope_key):
                reasons.add("no_domain_cluster")
                continue
            known_scores = [
                (cluster_id, float(similarity))
                for cluster_id, similarity in hint.scene_similarities.items()
                if cluster_id in scene_by_cluster
                and (not hint.scene_scope_key or scene_by_cluster[cluster_id].scope_key == hint.scene_scope_key)
            ]
            unknown_high = [
                cluster_id
                for cluster_id, similarity in hint.scene_similarities.items()
                if cluster_id not in scene_by_cluster and float(similarity) >= route_index.scene_secondary_threshold
            ]
            if unknown_high:
                reasons.add("unknown_scene_cluster")
            if not known_scores:
                reasons.add("below_assign_threshold")
                continue
            best_id, best_score = max(known_scores, key=lambda item: item[1])
            best_record = scene_by_cluster[best_id]
            if best_score < route_index.scene_spawn_threshold:
                reasons.add("below_assign_threshold")
                continue
            if best_record.fact_count >= route_index.scene_max_cluster_size:
                reasons.add("cluster_capacity_full")
                continue
            if (
                best_score < route_index.scene_assign_threshold
                and best_record.fact_count >= route_index.scene_max_cluster_size // 2
            ):
                reasons.add("weak_match_near_capacity")
                continue
            predicted_scenes.add(best_record.tree_id)
            for cluster_id, similarity in known_scores:
                if cluster_id == best_id or similarity < route_index.scene_secondary_threshold:
                    continue
                record = scene_by_cluster[cluster_id]
                if record.fact_count >= route_index.scene_max_cluster_size:
                    reasons.add("cluster_capacity_full")
                else:
                    predicted_scenes.add(record.tree_id)
        elif route_index.scene_records:
            reasons.add("missing_scene_similarity")

    tree_ids = tuple(sorted(predicted_entities | predicted_scenes | set(deleted)))
    return PreIngestTreeScope(
        full_hydration_required=bool(reasons),
        tree_ids_to_load=tree_ids,
        escalation_reasons=tuple(sorted(reasons)),
        predicted_entity_tree_ids=tuple(sorted(predicted_entities)),
        predicted_scene_tree_ids=tuple(sorted(predicted_scenes)),
        deleted_tree_ids=deleted,
    )


def build_route_index_manifest(
    *,
    entity_router: Any,
    scene_router: Any,
    tree_config: Any,
    embedding_model_version: str,
    risk_flags: RouteRiskFlags | None = None,
) -> RouteIndexManifest:
    """Project live router state into durable pre-ingest planning metadata."""

    entity_cfg = tree_config.entity
    scene_cfg = tree_config.scene
    risks = risk_flags or compute_route_risk_flags(
        entity_router=entity_router,
        scene_router=scene_router,
        tree_config=tree_config,
    )
    entity_records = tuple(
        sorted(
            (
                EntityRouteRecord(
                    entity_key=str(candidate.entity_key),
                    tree_id=f"entity:{candidate.entity_key}",
                    state=str(candidate.state),
                    distinct_fact_count=int(candidate.distinct_fact_count),
                    distinct_session_count=int(candidate.distinct_session_count),
                )
                for candidate in entity_router.iter_candidates()
            ),
            key=lambda record: record.entity_key,
        )
    )
    scene_records = []
    for cluster_id in sorted(scene_router.all_cluster_ids()):
        cluster = scene_router.get_cluster(cluster_id)
        if cluster is None:
            continue
        scene_records.append(
            SceneRouteRecord(
                cluster_id=str(cluster.cluster_id),
                tree_id=f"tree:{cluster.cluster_id}",
                scope_key=str(cluster.scope_key),
                fact_count=int(cluster.fact_count),
                centroid_hash=_vector_hash(cluster.centroid),
            )
        )
    return RouteIndexManifest(
        embedding_model_version=str(embedding_model_version or ""),
        entity_active_min_facts=int(entity_cfg.active_min_facts),
        entity_active_min_sessions=int(entity_cfg.active_min_sessions),
        scene_assign_threshold=float(scene_cfg.theta_assign),
        scene_secondary_threshold=float(scene_cfg.theta_second),
        scene_spawn_threshold=float(scene_cfg.theta_spawn),
        scene_max_cluster_size=int(scene_cfg.max_cluster_size),
        scene_merge_threshold=float(scene_cfg.theta_merge),
        scene_max_time_gap_days=float(scene_cfg.max_time_gap_days),
        scene_bootstrap_initialized=bool(scene_router.bootstrap_initialized()),
        scene_scores_include_time_bonus=True,
        entity_records=tuple(entity_records),
        scene_records=tuple(scene_records),
        # Forward-looking batch risk is not proven by current router state, so
        # planner-facing risk flags remain fail-closed until a batch analyzer
        # computes them explicitly.
        cluster_merge_possible=True,
        suppression_flip_possible=True,
        cluster_merge_currently_possible=bool(risks.cluster_merge_possible),
        suppression_currently_possible=bool(risks.suppression_flip_possible),
        cluster_merge_pair_count=int(risks.merge_pair_count),
        suppression_candidate_count=int(risks.suppression_candidate_count),
    )


def compute_route_risk_flags(
    *,
    entity_router: Any,
    scene_router: Any,
    tree_config: Any,
) -> RouteRiskFlags:
    """Compute current router risk diagnostics; future batch risk still fails closed."""

    merge_pairs = list(scene_router.merge_check())
    scene_fact_sets = []
    for cluster_id in scene_router.all_cluster_ids():
        cluster = scene_router.get_cluster(cluster_id)
        if cluster is not None and getattr(cluster, "fact_ids", None):
            scene_fact_sets.append(set(cluster.fact_ids))
    suppression_candidates = []
    for candidate in entity_router.iter_candidates():
        if getattr(candidate, "state", "") != "active":
            continue
        if getattr(candidate, "entity_key", "") == "user":
            continue
        if int(getattr(candidate, "distinct_fact_count", 0)) > int(tree_config.entity.suppress_below_facts):
            continue
        fact_ids = set(getattr(candidate, "fact_ids", ()) or ())
        if any(fact_ids and fact_ids.issubset(scene_fact_ids) for scene_fact_ids in scene_fact_sets):
            suppression_candidates.append(getattr(candidate, "entity_key", ""))
    return RouteRiskFlags(
        cluster_merge_possible=bool(merge_pairs),
        suppression_flip_possible=bool(suppression_candidates),
        merge_pair_count=len(merge_pairs),
        suppression_candidate_count=len(suppression_candidates),
    )


def route_index_manifest_from_dict(value: Mapping[str, Any]) -> RouteIndexManifest:
    """Rehydrate route metadata from a JSON-compatible durable document."""

    return RouteIndexManifest(
        embedding_model_version=str(value.get("embedding_model_version") or ""),
        entity_active_min_facts=int(value.get("entity_active_min_facts") or 0),
        entity_active_min_sessions=int(value.get("entity_active_min_sessions") or 0),
        scene_assign_threshold=float(value.get("scene_assign_threshold") or 0.0),
        scene_secondary_threshold=float(value.get("scene_secondary_threshold") or 0.0),
        scene_spawn_threshold=float(value.get("scene_spawn_threshold") or 0.0),
        scene_max_cluster_size=int(value.get("scene_max_cluster_size") or 0),
        scene_merge_threshold=float(value.get("scene_merge_threshold") or 0.0),
        scene_max_time_gap_days=float(value.get("scene_max_time_gap_days") or 0.0),
        scene_bootstrap_initialized=bool(value.get("scene_bootstrap_initialized")),
        scene_scores_include_time_bonus=bool(value.get("scene_scores_include_time_bonus")),
        entity_records=tuple(
            EntityRouteRecord(
                entity_key=str(record.get("entity_key") or ""),
                tree_id=str(record.get("tree_id") or ""),
                state=str(record.get("state") or ""),
                distinct_fact_count=int(record.get("distinct_fact_count") or 0),
                distinct_session_count=int(record.get("distinct_session_count") or 0),
            )
            for record in value.get("entity_records", ())
        ),
        scene_records=tuple(
            SceneRouteRecord(
                cluster_id=str(record.get("cluster_id") or ""),
                tree_id=str(record.get("tree_id") or ""),
                scope_key=str(record.get("scope_key") or ""),
                fact_count=int(record.get("fact_count") or 0),
                centroid_hash=str(record.get("centroid_hash") or ""),
            )
            for record in value.get("scene_records", ())
        ),
        cluster_merge_possible=_fail_closed_bool(value, "cluster_merge_possible"),
        suppression_flip_possible=_fail_closed_bool(value, "suppression_flip_possible"),
        cluster_merge_currently_possible=bool(value.get("cluster_merge_currently_possible")),
        suppression_currently_possible=bool(value.get("suppression_currently_possible")),
        cluster_merge_pair_count=int(value.get("cluster_merge_pair_count") or 0),
        suppression_candidate_count=int(value.get("suppression_candidate_count") or 0),
    )


def route_index_manifest_to_dict(manifest: RouteIndexManifest) -> dict[str, Any]:
    """Serialize route metadata to a JSON-compatible durable document."""

    return {
        "schema_version": 1,
        "embedding_model_version": manifest.embedding_model_version,
        "entity_active_min_facts": manifest.entity_active_min_facts,
        "entity_active_min_sessions": manifest.entity_active_min_sessions,
        "scene_assign_threshold": manifest.scene_assign_threshold,
        "scene_secondary_threshold": manifest.scene_secondary_threshold,
        "scene_spawn_threshold": manifest.scene_spawn_threshold,
        "scene_max_cluster_size": manifest.scene_max_cluster_size,
        "scene_merge_threshold": manifest.scene_merge_threshold,
        "scene_max_time_gap_days": manifest.scene_max_time_gap_days,
        "scene_bootstrap_initialized": manifest.scene_bootstrap_initialized,
        "scene_scores_include_time_bonus": manifest.scene_scores_include_time_bonus,
        "cluster_merge_possible": manifest.cluster_merge_possible,
        "suppression_flip_possible": manifest.suppression_flip_possible,
        "cluster_merge_currently_possible": manifest.cluster_merge_currently_possible,
        "suppression_currently_possible": manifest.suppression_currently_possible,
        "cluster_merge_pair_count": manifest.cluster_merge_pair_count,
        "suppression_candidate_count": manifest.suppression_candidate_count,
        "entity_records": [
            {
                "entity_key": record.entity_key,
                "tree_id": record.tree_id,
                "state": record.state,
                "distinct_fact_count": record.distinct_fact_count,
                "distinct_session_count": record.distinct_session_count,
            }
            for record in manifest.entity_records
        ],
        "scene_records": [
            {
                "cluster_id": record.cluster_id,
                "tree_id": record.tree_id,
                "scope_key": record.scope_key,
                "fact_count": record.fact_count,
                "centroid_hash": record.centroid_hash,
            }
            for record in manifest.scene_records
        ],
    }


def route_index_manifest_matches_router_state(
    manifest: RouteIndexManifest,
    *,
    entity_router: Any,
    scene_router: Any,
    tree_config: Any,
    embedding_model_version: str,
) -> bool:
    """Return whether durable route metadata matches the current live routers."""

    current = build_route_index_manifest(
        entity_router=entity_router,
        scene_router=scene_router,
        tree_config=tree_config,
        embedding_model_version=embedding_model_version,
    )
    return current == manifest


def _candidate_can_activate(
    record: EntityRouteRecord,
    route_index: RouteIndexManifest,
    *,
    new_fact_count: int,
    new_session_count: int,
) -> bool:
    next_fact_count = int(record.distinct_fact_count) + max(0, int(new_fact_count))
    next_session_count = int(record.distinct_session_count) + max(0, int(new_session_count))
    return (
        next_fact_count >= int(route_index.entity_active_min_facts)
        and next_session_count >= int(route_index.entity_active_min_sessions)
    )


def _vector_hash(values: Iterable[float]) -> str:
    text = ",".join(f"{float(value):.8g}" for value in values)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _fail_closed_bool(value: Mapping[str, Any], key: str) -> bool:
    if key not in value or value.get(key) is None:
        return True
    return bool(value.get(key))


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
