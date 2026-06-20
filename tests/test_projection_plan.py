from __future__ import annotations

import pytest

from src.cloud.projection_plan import ProjectionRecordKey, ProjectionRecordRef, ProjectionSkipPlan, filter_projection_refs_to_tree_scope, plan_affected_trees, plan_projection_skip, plan_projection_tree_scope, tree_node_body_content_id


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


def test_filter_projection_refs_to_tree_scope_keeps_only_tree_scoped_records() -> None:
    refs = [
        ProjectionRecordRef("tree_node_membership", "entity:a/root", content_hash="a", metadata={"tree_id": "entity:a", "node_id": "root"}),
        ProjectionRecordRef("tree_node_membership", "entity:b/root", content_hash="b", metadata={"tree_id": "entity:b", "node_id": "root"}),
        ProjectionRecordRef("summary_cache", "summary", content_hash="s", metadata={}),
    ]

    filtered = filter_projection_refs_to_tree_scope(refs, tree_ids=["entity:b"])

    assert [record.logical_id for record in filtered] == ["entity:b/root"]
