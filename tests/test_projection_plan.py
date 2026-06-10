from __future__ import annotations

import pytest

from src.cloud.projection_plan import ProjectionRecordKey, ProjectionRecordRef, plan_projection_skip


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

