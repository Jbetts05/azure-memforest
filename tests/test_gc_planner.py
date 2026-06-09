from __future__ import annotations

import pytest

from src.cloud.gc_planner import ManifestRef, plan_projection_gc


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


def test_projection_gc_rejects_empty_retention() -> None:
    with pytest.raises(ValueError, match="retention_count"):
        plan_projection_gc([], retention_count=0, all_body_ids=[], retained_membership_body_ids=[])

