from __future__ import annotations

import pytest

from src.cloud.projection_plan import (
    build_route_index_manifest,
    EntityRouteRecord,
    FactRouteHint,
    PreIngestTreeScope,
    ProjectionRecordKey,
    ProjectionRecordRef,
    ProjectionSkipPlan,
    RouteIndexManifest,
    route_index_manifest_from_dict,
    route_index_manifest_matches_router_state,
    route_index_manifest_to_dict,
    SceneRouteRecord,
    filter_projection_refs_to_tree_scope,
    plan_affected_trees,
    plan_partial_hydration,
    plan_pre_ingest_tree_scope,
    plan_projection_skip,
    plan_projection_tree_scope,
    tree_node_body_content_id,
)
from src.config.config import load_default_config
from src.utils.types import FactOccurrence, ManagedFact


def test_projection_skip_plan_splits_unchanged_upsert_and_delete_by_content_hash() -> None:
    plan = plan_projection_skip(
        projected_records=[
            ProjectionRecordRef("tree_node_membership", "tree-1/node-1", content_hash="same"),
            ProjectionRecordRef("fact_location", "fact-1/tree-1/node-1", content_hash="new"),
            ProjectionRecordRef("recall_document", "tree-2/root", content_hash="insert"),
        ],
        retained_committed_records=[
            ProjectionRecordRef("tree_node_membership", "tree-1/node-1", content_hash="same"),
            ProjectionRecordRef("fact_location", "fact-1/tree-1/node-1", content_hash="old"),
            ProjectionRecordRef("recall_document", "tree-old/root", content_hash="old"),
        ],
    )

    assert plan.unchanged_keys == frozenset({ProjectionRecordKey("tree_node_membership", "tree-1/node-1")})
    assert plan.upsert_keys == frozenset(
        {
            ProjectionRecordKey("fact_location", "fact-1/tree-1/node-1"),
            ProjectionRecordKey("recall_document", "tree-2/root"),
        }
    )
    assert plan.delete_keys == frozenset({ProjectionRecordKey("recall_document", "tree-old/root")})
    assert [record.logical_id for record in plan.superseded] == ["fact-1/tree-1/node-1"]


def test_projection_skip_plan_compares_content_ids_for_stable_bodies() -> None:
    plan = plan_projection_skip(
        projected_records=[
            ProjectionRecordRef(
                "tree_node_membership",
                "tree-1/node-1",
                content_id="tree_node_body_abc",
                content_hash="abc",
            ),
            ProjectionRecordRef(
                "tree_node_membership",
                "tree-1/node-2",
                content_id="tree_node_body_new",
                content_hash="new",
            ),
        ],
        retained_committed_records=[
            ProjectionRecordRef(
                "tree_node_membership",
                "tree-1/node-1",
                content_id="tree_node_body_abc",
                content_hash="missing-is-ok-when-body-id-matches",
            ),
            ProjectionRecordRef(
                "tree_node_membership",
                "tree-1/node-2",
                content_id="tree_node_body_old",
                content_hash="new",
            ),
        ],
    )

    assert plan.unchanged_keys == frozenset({ProjectionRecordKey("tree_node_membership", "tree-1/node-1")})
    assert plan.upsert_keys == frozenset({ProjectionRecordKey("tree_node_membership", "tree-1/node-2")})
    assert plan.delete == ()


def test_tree_node_body_content_id_is_namespaced_by_tenant_user() -> None:
    first = tree_node_body_content_id(
        tenant_user_key="tenant-a:user-a",
        tree_id="tree-1",
        node_id="node-1",
        content_sha256="same-content",
    )
    second = tree_node_body_content_id(
        tenant_user_key="tenant-b:user-b",
        tree_id="tree-1",
        node_id="node-1",
        content_sha256="same-content",
    )

    assert first != second


def test_projection_skip_plan_treats_empty_committed_metadata_as_all_upserts() -> None:
    plan = plan_projection_skip(
        projected_records=[
            ProjectionRecordRef("browse_embedding", "node-1/embed-v1", content_hash="embedding-hash"),
            ProjectionRecordRef("summary_cache", "summary-key", content_id="summary-key"),
        ],
        retained_committed_records=[],
    )

    assert plan.unchanged == ()
    assert [record.logical_id for record in plan.upsert] == ["node-1/embed-v1", "summary-key"]
    assert plan.delete == ()


def test_projection_skip_plan_rejects_duplicate_logical_keys() -> None:
    with pytest.raises(ValueError, match="duplicate record key"):
        plan_projection_skip(
            projected_records=[
                ProjectionRecordRef("recall_document", "tree-1/root", content_hash="a"),
                ProjectionRecordRef("recall_document", "tree-1/root", content_hash="b"),
            ],
            retained_committed_records=[],
        )


def test_projection_skip_plan_rejects_records_without_content_identity() -> None:
    with pytest.raises(ValueError, match="content_id or content_hash"):
        plan_projection_skip(
            projected_records=[ProjectionRecordRef("recall_document", "tree-1/root")],
            retained_committed_records=[],
        )


def test_plan_affected_trees_collects_changed_tree_fact_node_and_root_sets() -> None:
    plan = plan_affected_trees(
        {
            "tree_node_membership": ProjectionSkipPlan(
                unchanged=(),
                upsert=(
                    ProjectionRecordRef("tree_node_membership", "entity:alice/root", content_hash="new", metadata={"tree_id": "entity:alice", "node_id": "root"}),
                    ProjectionRecordRef("tree_node_membership", "entity:alice/leaf", content_hash="new", metadata={"tree_id": "entity:alice", "node_id": "leaf"}),
                ),
                delete=(
                    ProjectionRecordRef("tree_node_membership", "entity:old/root", content_hash="old", metadata={"tree_id": "entity:old", "node_id": "root"}),
                ),
            ),
            "fact_location": ProjectionSkipPlan(
                unchanged=(),
                upsert=(
                    ProjectionRecordRef("fact_location", "fact-1/entity:alice/leaf/entity/L0", content_hash="new", metadata={"tree_id": "entity:alice", "node_id": "leaf", "fact_id": "fact-1"}),
                ),
                delete=(
                    ProjectionRecordRef("fact_location", "fact-old/entity:old/leaf/entity/L0", content_hash="old", metadata={"tree_id": "entity:old", "node_id": "leaf", "fact_id": "fact-old"}),
                ),
            ),
            "recall_document": ProjectionSkipPlan(
                unchanged=(),
                upsert=(
                    ProjectionRecordRef("recall_document", "entity:alice/root", content_hash="new", metadata={"tree_id": "entity:alice", "node_id": "root"}),
                ),
                delete=(),
                superseded=(
                    ProjectionRecordRef("recall_document", "entity:old/root", content_hash="old", metadata={"tree_id": "entity:old", "node_id": "root"}),
                ),
            ),
        }
    )

    assert plan.affected_tree_ids == ("entity:alice", "entity:old")
    assert plan.affected_fact_ids == ("fact-1", "fact-old")
    assert plan.dirty_node_keys == ("entity:alice/leaf", "entity:alice/root", "entity:old/root")
    assert plan.dirty_root_tree_ids == ("entity:alice", "entity:old")


def test_plan_affected_trees_ignores_unchanged_records_and_missing_metadata() -> None:
    plan = plan_affected_trees(
        {
            "tree_node_membership": ProjectionSkipPlan(
                unchanged=(
                    ProjectionRecordRef("tree_node_membership", "entity:alice/root", content_hash="same", metadata={"tree_id": "entity:alice", "node_id": "root"}),
                ),
                upsert=(
                    ProjectionRecordRef("tree_node_membership", "bad", content_hash="new", metadata={}),
                ),
                delete=(),
            )
        }
    )

    assert plan.affected_tree_ids == ()
    assert plan.affected_fact_ids == ()
    assert plan.dirty_node_keys == ()
    assert plan.dirty_root_tree_ids == ()


def test_plan_affected_trees_excludes_unchanged_records_with_valid_metadata() -> None:
    plan = plan_affected_trees(
        {
            "tree_node_membership": ProjectionSkipPlan(
                unchanged=(
                    ProjectionRecordRef("tree_node_membership", "entity:unchanged/root", content_hash="same", metadata={"tree_id": "entity:unchanged", "node_id": "root"}),
                ),
                upsert=(
                    ProjectionRecordRef("tree_node_membership", "entity:changed/root", content_hash="new", metadata={"tree_id": "entity:changed", "node_id": "root"}),
                ),
                delete=(),
            )
        }
    )

    assert plan.affected_tree_ids == ("entity:changed",)
    assert plan.dirty_node_keys == ("entity:changed/root",)


def test_plan_projection_tree_scope_projects_all_when_delta_unknown() -> None:
    scope = plan_projection_tree_scope(
        all_tree_ids=["entity:b", "entity:a"],
        changed_tree_ids=None,
        deleted_tree_ids=["entity:old"],
    )

    assert scope.full_projection_required is True
    assert scope.projected_tree_ids == ("entity:a", "entity:b")
    assert scope.deleted_tree_ids == ("entity:old",)
    assert scope.omitted_tree_ids == ()


def test_plan_projection_tree_scope_projects_only_known_changed_trees() -> None:
    scope = plan_projection_tree_scope(
        all_tree_ids=["entity:a", "entity:b", "session:s1"],
        changed_tree_ids=["entity:b", "missing:new"],
        deleted_tree_ids=["entity:old"],
    )

    assert scope.full_projection_required is False
    assert scope.projected_tree_ids == ("entity:b",)
    assert scope.omitted_tree_ids == ("entity:a", "session:s1")
    assert scope.deleted_tree_ids == ("entity:old",)
    assert scope.unknown_changed_tree_ids == ("missing:new",)


class FakeEmbeddingClient:
    def embed_texts(self, texts, *, batch_size=256):
        return [[1.0, 0.0] for _ in texts]


def _managed_fact(fact_id: str, *, entities=("Alice",), domain_keys=("travel",), session_id="session-1") -> ManagedFact:
    return ManagedFact(
        fact_id=fact_id,
        fact_text=f"{fact_id} text",
        embedding_id=f"emb-{fact_id}",
        origin="user",
        semantic_role="preference",
        entities=list(entities),
        topics=["topic"],
        attribute_keys=[],
        domain_keys=list(domain_keys),
        collection_keys=[],
        detail_level="atomic",
        confidence=1.0,
        first_session_id=session_id,
        first_cell_id=f"{session_id}#cell_0000",
        occurrences=[
            FactOccurrence(
                session_id=session_id,
                cell_id=f"{session_id}#cell_0000",
                item_id=f"item-{fact_id}",
                source_turn_ids=[f"{session_id}:0"],
                source_spans=[],
                participants=["user"],
                time_text="",
                time_start=1.0,
                time_end=1.0,
            )
        ],
        metadata={"embedding": [1.0, 0.0]},
        time_start=1.0,
        time_end=1.0,
    )


def test_build_route_index_manifest_projects_live_router_state() -> None:
    from src.build.entity_router import EntityRouter
    from src.build.scene_router import SceneRouter

    config = load_default_config()
    entity_router = EntityRouter(config=config.tree.entity)
    scene_router = SceneRouter(embedding_client=FakeEmbeddingClient(), config=config.tree.scene)
    facts = [
        _managed_fact("fact-1", entities=("Alice",), session_id="session-1"),
        _managed_fact("fact-2", entities=("Alice",), session_id="session-2"),
        _managed_fact("fact-3", entities=("Alice",), session_id="session-3"),
    ]
    for fact in facts:
        entity_router.assign(fact)
    scene_router.bootstrap(facts)

    manifest = build_route_index_manifest(
        entity_router=entity_router,
        scene_router=scene_router,
        tree_config=config.tree,
        embedding_model_version="embed-v1",
        cluster_merge_possible=False,
        suppression_flip_possible=False,
    )

    assert manifest.embedding_model_version == "embed-v1"
    assert manifest.entity_active_min_facts == config.tree.entity.active_min_facts
    assert manifest.scene_assign_threshold == config.tree.scene.theta_assign
    assert manifest.scene_secondary_threshold == config.tree.scene.theta_second
    assert manifest.scene_spawn_threshold == config.tree.scene.theta_spawn
    assert manifest.scene_max_cluster_size == config.tree.scene.max_cluster_size
    assert manifest.scene_max_time_gap_days == config.tree.scene.max_time_gap_days
    assert manifest.scene_scores_include_time_bonus is True
    alice = next(record for record in manifest.entity_records if record.entity_key == "alice")
    assert alice.tree_id == "entity:alice"
    assert alice.state == "active"
    assert alice.distinct_fact_count == 3
    assert len(manifest.scene_records) == len(scene_router.all_cluster_ids())
    assert route_index_manifest_matches_router_state(
        manifest,
        entity_router=entity_router,
        scene_router=scene_router,
        tree_config=config.tree,
        embedding_model_version="embed-v1",
    )


def test_route_index_manifest_round_trips_json_shape() -> None:
    manifest = _route_index(cluster_merge_possible=True, suppression_flip_possible=True)

    restored = route_index_manifest_from_dict(route_index_manifest_to_dict(manifest))

    assert restored == manifest


def test_route_index_manifest_from_dict_defaults_missing_risk_flags_to_fail_closed() -> None:
    raw = route_index_manifest_to_dict(_route_index(cluster_merge_possible=False, suppression_flip_possible=False))
    raw.pop("cluster_merge_possible")
    raw["suppression_flip_possible"] = None

    restored = route_index_manifest_from_dict(raw)

    assert restored.cluster_merge_possible is True
    assert restored.suppression_flip_possible is True


def test_filter_projection_refs_to_tree_scope_keeps_only_tree_scoped_records() -> None:
    refs = [
        ProjectionRecordRef("tree_node_membership", "entity:a/root", content_hash="a", metadata={"tree_id": "entity:a", "node_id": "root"}),
        ProjectionRecordRef("tree_node_membership", "entity:b/root", content_hash="b", metadata={"tree_id": "entity:b", "node_id": "root"}),
        ProjectionRecordRef("summary_cache", "summary", content_hash="s", metadata={}),
    ]

    filtered = filter_projection_refs_to_tree_scope(refs, tree_ids=["entity:b"])

    assert [record.logical_id for record in filtered] == ["entity:b/root"]


def test_plan_partial_hydration_requires_full_when_delta_unknown() -> None:
    plan = plan_partial_hydration(
        all_tree_ids=["entity:b", "entity:a"],
        changed_tree_ids=None,
        deleted_tree_ids=["entity:old"],
    )

    assert plan.full_hydration_required is True
    assert plan.tree_ids_to_load == ("entity:a", "entity:b", "entity:old")
    assert plan.deleted_tree_ids == ("entity:old",)
    assert plan.omitted_tree_ids == ()


def test_plan_partial_hydration_loads_changed_and_deleted_trees_only() -> None:
    plan = plan_partial_hydration(
        all_tree_ids=["entity:a", "entity:b", "session:s1", "tree:scene:x"],
        changed_tree_ids=["entity:b", "session:s1"],
        deleted_tree_ids=["entity:old"],
    )

    assert plan.full_hydration_required is False
    assert plan.tree_ids_to_load == ("entity:b", "entity:old", "session:s1")
    assert plan.deleted_tree_ids == ("entity:old",)
    assert plan.omitted_tree_ids == ("entity:a", "tree:scene:x")
    assert plan.unknown_changed_tree_ids == ()


def test_plan_partial_hydration_fails_closed_on_unknown_changed_tree() -> None:
    plan = plan_partial_hydration(
        all_tree_ids=["entity:a", "entity:b"],
        changed_tree_ids=["entity:b", "missing:new"],
        deleted_tree_ids=[],
    )

    assert plan.full_hydration_required is True
    assert plan.tree_ids_to_load == ("entity:a", "entity:b")
    assert plan.omitted_tree_ids == ()
    assert plan.unknown_changed_tree_ids == ("missing:new",)


def test_plan_partial_hydration_distinguishes_empty_known_delta_from_unknown() -> None:
    plan = plan_partial_hydration(
        all_tree_ids=["entity:a", "entity:b"],
        changed_tree_ids=[],
        deleted_tree_ids=["entity:old"],
    )

    assert plan.full_hydration_required is False
    assert plan.tree_ids_to_load == ("entity:old",)
    assert plan.deleted_tree_ids == ("entity:old",)
    assert plan.omitted_tree_ids == ("entity:a", "entity:b")


def _route_index(**overrides) -> RouteIndexManifest:
    values = {
        "embedding_model_version": "embed-v1",
        "entity_active_min_facts": 3,
        "entity_active_min_sessions": 2,
        "scene_assign_threshold": 0.7,
        "scene_secondary_threshold": 0.55,
        "scene_spawn_threshold": 0.45,
        "scene_max_cluster_size": 60,
        "scene_merge_threshold": 0.88,
        "scene_max_time_gap_days": 45.0,
        "scene_bootstrap_initialized": True,
        "scene_scores_include_time_bonus": True,
        "entity_records": (
            EntityRouteRecord("alice", "entity:alice", "active", 3, 2),
            EntityRouteRecord("bob", "entity:bob", "lazy", 2, 1),
        ),
        "scene_records": (
            SceneRouteRecord("scene:travel", "tree:scene:travel", "travel", 5, "centroid-a"),
            SceneRouteRecord("scene:work", "tree:scene:work", "travel", 7, "centroid-b"),
        ),
    }
    values.update(overrides)
    return RouteIndexManifest(**values)


def test_plan_pre_ingest_tree_scope_returns_safe_subset_for_active_entity_and_assigned_scene() -> None:
    scope = plan_pre_ingest_tree_scope(
        route_index=_route_index(),
        embedding_model_version="embed-v1",
        new_fact_route_hints=[
            FactRouteHint(
                fact_id="fact-1",
                entity_keys=("alice",),
                scene_similarities={"scene:travel": 0.8, "scene:work": 0.2},
                scene_scope_key="travel",
                new_session_id="session-1",
            )
        ],
        deleted_tree_ids=["entity:old"],
    )

    assert scope.full_hydration_required is False
    assert scope.escalation_reasons == ()
    assert scope.predicted_entity_tree_ids == ("entity:alice",)
    assert scope.predicted_scene_tree_ids == ("tree:scene:travel",)
    assert scope.deleted_tree_ids == ("entity:old",)
    assert scope.tree_ids_to_load == ("entity:alice", "entity:old", "tree:scene:travel")


@pytest.mark.parametrize(
    ("route_index", "hint", "expected_reason"),
    [
        (_route_index(embedding_model_version="embed-old"), FactRouteHint("fact-1"), "embedding_version_mismatch"),
        (_route_index(scene_bootstrap_initialized=False), FactRouteHint("fact-1", scene_similarities={"scene:travel": 0.8}), "bootstrap_uninitialized"),
        (_route_index(), FactRouteHint("fact-1", scene_similarities={"scene:travel": 0.1}, scene_scope_key="travel"), "below_assign_threshold"),
        (_route_index(cluster_merge_possible=True), FactRouteHint("fact-1"), "cluster_merge_possible"),
        (_route_index(suppression_flip_possible=True), FactRouteHint("fact-1"), "suppression_flip_possible"),
        (_route_index(), FactRouteHint("fact-1", entity_keys=("new_person",)), "new_entity_key"),
        (_route_index(), FactRouteHint("fact-1", entity_keys=("bob",), new_session_id="session-2"), "candidate_near_activation"),
        (_route_index(), FactRouteHint("fact-1", scene_similarities={"scene:missing": 0.9}, scene_scope_key="travel"), "unknown_scene_cluster"),
        (_route_index(), FactRouteHint("fact-1", scene_similarities={"scene:travel": 0.9}, scene_scope_key="new_domain"), "no_domain_cluster"),
        (_route_index(), FactRouteHint("fact-1", scene_similarities={"scene:travel": 0.9}), "missing_scene_scope_key"),
        (_route_index(scene_scores_include_time_bonus=False), FactRouteHint("fact-1", scene_similarities={"scene:travel": 0.9}, scene_scope_key="travel"), "scene_scores_missing_time_bonus"),
        (_route_index(scene_records=(SceneRouteRecord("scene:travel", "tree:scene:travel", "travel", 60, "centroid-a"),)), FactRouteHint("fact-1", scene_similarities={"scene:travel": 0.9}, scene_scope_key="travel"), "cluster_capacity_full"),
        (_route_index(scene_records=(SceneRouteRecord("scene:travel", "tree:scene:travel", "travel", 30, "centroid-a"),)), FactRouteHint("fact-1", scene_similarities={"scene:travel": 0.5}, scene_scope_key="travel"), "weak_match_near_capacity"),
        (_route_index(), FactRouteHint("fact-1"), "missing_scene_similarity"),
    ],
)
def test_plan_pre_ingest_tree_scope_escalates_when_conservative_superset_cannot_be_proven(route_index, hint, expected_reason) -> None:
    scope = plan_pre_ingest_tree_scope(
        route_index=route_index,
        embedding_model_version="embed-v1",
        new_fact_route_hints=[hint],
    )

    assert scope.full_hydration_required is True
    assert expected_reason in scope.escalation_reasons


def test_plan_pre_ingest_tree_scope_aggregates_batch_entity_activation_risk() -> None:
    index = _route_index(entity_records=(EntityRouteRecord("bob", "entity:bob", "lazy", 1, 1),))

    scope = plan_pre_ingest_tree_scope(
        route_index=index,
        embedding_model_version="embed-v1",
        new_fact_route_hints=[
            FactRouteHint("fact-1", entity_keys=("bob",), new_session_id="session-2"),
            FactRouteHint("fact-2", entity_keys=("bob",), new_session_id="session-2"),
        ],
    )

    assert scope.full_hydration_required is True
    assert "candidate_near_activation" in scope.escalation_reasons


def test_plan_pre_ingest_tree_scope_includes_secondary_scene_assignment() -> None:
    scope = plan_pre_ingest_tree_scope(
        route_index=_route_index(),
        embedding_model_version="embed-v1",
        new_fact_route_hints=[
            FactRouteHint(
                "fact-1",
                scene_similarities={"scene:travel": 0.8, "scene:work": 0.6},
                scene_scope_key="travel",
            )
        ],
    )

    assert scope.full_hydration_required is False
    assert scope.predicted_scene_tree_ids == ("tree:scene:travel", "tree:scene:work")


def test_plan_pre_ingest_tree_scope_allows_empty_noop_scope() -> None:
    scope = plan_pre_ingest_tree_scope(
        route_index=_route_index(),
        embedding_model_version="embed-v1",
        new_fact_route_hints=[],
    )

    assert scope.full_hydration_required is False
    assert scope.tree_ids_to_load == ()
    assert scope.escalation_reasons == ()


def test_pre_ingest_tree_scope_rejects_full_without_reason_and_missing_predicted_trees() -> None:
    with pytest.raises(ValueError, match="requires at least one escalation reason"):
        PreIngestTreeScope(
            full_hydration_required=True,
            tree_ids_to_load=(),
            escalation_reasons=(),
            predicted_entity_tree_ids=(),
            predicted_scene_tree_ids=(),
        )
    with pytest.raises(ValueError, match="tree_ids_to_load must include"):
        PreIngestTreeScope(
            full_hydration_required=False,
            tree_ids_to_load=(),
            escalation_reasons=(),
            predicted_entity_tree_ids=("entity:alice",),
            predicted_scene_tree_ids=(),
        )
