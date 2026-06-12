from __future__ import annotations

import pytest

from src.cloud.gc_planner import ManifestRef, MvccProjectionRecordRef, ProjectionGcPolicy, plan_projection_gc, plan_projection_gc_for_policy


def test_projection_gc_retains_latest_committed_generations_by_generation_order() -> None:
    plan = plan_projection_gc(
        [
            ManifestRef("v-old", generation=1),
            ManifestRef("v-new", generation=3),
            ManifestRef("v-mid", generation=2),
            ManifestRef("v-pending", generation=4, status="pending"),
        ],
        retention_count=2,
        all_body_ids=[],
        retained_membership_body_ids=[],
    )

    assert plan.retained_forest_versions == frozenset({"v-new", "v-mid"})
    assert plan.dropped_forest_versions == frozenset({"v-old"})
    assert plan.policy_enabled is True
    assert plan.retention_count == 2


def test_projection_gc_keeps_body_referenced_by_any_retained_membership() -> None:
    plan = plan_projection_gc(
        [
            ManifestRef("v1", generation=1),
            ManifestRef("v2", generation=2),
            ManifestRef("v3", generation=3),
        ],
        retention_count=1,
        all_body_ids=["shared-body", "old-body"],
        retained_membership_body_ids=["shared-body"],
    )

    assert plan.retained_forest_versions == frozenset({"v3"})
    assert plan.dropped_forest_versions == frozenset({"v1", "v2"})
    assert plan.deletable_body_ids == frozenset({"old-body"})


def test_projection_gc_requires_membership_refs_for_all_retained_versions_when_declared() -> None:
    with pytest.raises(ValueError, match="missing retained forest versions"):
        plan_projection_gc(
            [
                ManifestRef("v1", generation=1),
                ManifestRef("v2", generation=2),
                ManifestRef("v3", generation=3),
            ],
            retention_count=2,
            all_body_ids=["body-old", "body-current"],
            retained_membership_body_ids=["body-current"],
            retained_membership_forest_versions=["v3"],
        )


def test_projection_gc_accepts_complete_retained_membership_version_coverage() -> None:
    plan = plan_projection_gc(
        [
            ManifestRef("v1", generation=1),
            ManifestRef("v2", generation=2),
            ManifestRef("v3", generation=3),
        ],
        retention_count=2,
        all_body_ids=["body-old", "body-current"],
        retained_membership_body_ids=["body-current"],
        retained_membership_forest_versions=["v2", "v3"],
    )

    assert plan.retained_forest_versions == frozenset({"v2", "v3"})
    assert plan.deletable_body_ids == frozenset({"body-old"})
    assert plan.membership_coverage_verified is True


def test_projection_gc_rejects_empty_retention() -> None:
    with pytest.raises(ValueError, match="retention_count"):
        plan_projection_gc([], retention_count=0, all_body_ids=[], retained_membership_body_ids=[])


def test_projection_gc_policy_is_disabled_by_default_and_plans_no_deletes() -> None:
    plan = plan_projection_gc_for_policy(
        [ManifestRef("v1", generation=1), ManifestRef("v2", generation=2)],
        policy=ProjectionGcPolicy(),
        all_body_ids=["body-a"],
        retained_membership_body_ids=[],
    )

    assert plan.policy_enabled is False
    assert plan.retention_count == 0
    assert plan.live_delete_enabled is False
    assert plan.retained_forest_versions == frozenset()
    assert plan.dropped_forest_versions == frozenset()
    assert plan.deletable_body_ids == frozenset()


def test_projection_gc_policy_requires_positive_retention_when_enabled() -> None:
    with pytest.raises(ValueError, match="retention_count"):
        ProjectionGcPolicy(enabled=True, retention_count=0)


def test_projection_gc_policy_protects_latest_retained_versions() -> None:
    plan = plan_projection_gc_for_policy(
        [
            ManifestRef("v1", generation=1),
            ManifestRef("v2", generation=2),
            ManifestRef("v3", generation=3),
        ],
        policy=ProjectionGcPolicy(enabled=True, retention_count=2, live_delete_enabled=True),
        all_body_ids=["body-old", "body-current"],
        retained_membership_body_ids=["body-current"],
    )

    assert plan.retained_forest_versions == frozenset({"v2", "v3"})
    assert plan.dropped_forest_versions == frozenset({"v1"})
    assert plan.deletable_body_ids == frozenset({"body-old"})
    assert plan.live_delete_enabled is True


def test_projection_gc_deletes_only_mvcc_records_not_visible_to_retained_manifests() -> None:
    plan = plan_projection_gc(
        [
            ManifestRef("g1-old", generation=1),
            ManifestRef("g2-mid", generation=2),
            ManifestRef("g3-current", generation=3),
        ],
        retention_count=2,
        all_body_ids=[],
        retained_membership_body_ids=[],
        mvcc_records=[
            MvccProjectionRecordRef(
                record_id="closed-before-retention",
                record_kind="fact_location",
                valid_from_generation=1,
                valid_from_forest_version="g1-old",
                valid_to_generation=2,
                valid_to_forest_version="g2-mid",
            ),
            MvccProjectionRecordRef(
                record_id="visible-at-retained-boundary",
                record_kind="tree_node_membership",
                valid_from_generation=1,
                valid_from_forest_version="g1-old",
                valid_to_generation=2,
                valid_to_forest_version="other-g2-attempt",
            ),
            MvccProjectionRecordRef(
                record_id="still-open",
                record_kind="recall_document",
                valid_from_generation=1,
                valid_from_forest_version="g1-old",
            ),
        ],
    )

    assert plan.retained_forest_versions == frozenset({"g2-mid", "g3-current"})
    assert plan.deletable_record_ids == frozenset({"closed-before-retention"})


def test_projection_gc_keeps_legacy_records_out_of_mvcc_deletion() -> None:
    plan = plan_projection_gc(
        [ManifestRef("g2-current", generation=2)],
        retention_count=1,
        all_body_ids=[],
        retained_membership_body_ids=[],
        mvcc_records=[
            MvccProjectionRecordRef(
                record_id="legacy",
                record_kind="recall_document",
                valid_from_generation=None,
                valid_from_forest_version=None,
                valid_to_generation=1,
                valid_to_forest_version="g1-old",
            )
        ],
    )

    assert plan.deletable_record_ids == frozenset()
