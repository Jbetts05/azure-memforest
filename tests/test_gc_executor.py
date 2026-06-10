from __future__ import annotations

import pytest

from src.cloud.gc_executor import (
    ProjectionGcAuditEvent,
    ProjectionGcExecutionError,
    execute_projection_gc_plan,
    projection_gc_plan_sha256,
)
from src.cloud.gc_planner import ProjectionGcPlan


def _plan() -> ProjectionGcPlan:
    return ProjectionGcPlan(
        retained_forest_versions=frozenset({"v2"}),
        dropped_forest_versions=frozenset({"v1"}),
        deletable_body_ids=frozenset({"body-b", "body-a"}),
        policy_enabled=True,
        retention_count=1,
        live_delete_enabled=True,
    )


@pytest.mark.asyncio
async def test_projection_gc_executor_dry_run_audits_without_delete_callback() -> None:
    events: list[ProjectionGcAuditEvent] = []

    result = await execute_projection_gc_plan(
        plan=_plan(),
        audit_callback=events.append,
        reason="retention_window",
        dry_run=True,
        tenant_id="tenant-1",
        user_id="user-1",
    )

    assert result.dry_run is True
    assert result.intended_body_ids == ("body-a", "body-b")
    assert result.deleted_body_ids == ()
    assert result.skipped_body_ids == ("body-a", "body-b")
    assert [event.action for event in events] == [
        "plan_started",
        "delete_intent",
        "delete_skipped_dry_run",
        "delete_intent",
        "delete_skipped_dry_run",
        "plan_completed",
    ]
    assert {event.plan_sha256 for event in events} == {projection_gc_plan_sha256(_plan())}
    assert events[1].body_id == "body-a"
    assert events[1].tenant_id == "tenant-1"
    assert events[1].user_id == "user-1"
    assert events[0].metadata["policy_enabled"] is True
    assert events[0].metadata["retention_count"] == 1
    assert events[0].metadata["policy_live_delete_enabled"] is True
    assert events[0].metadata["execution_live_delete_enabled"] is False


@pytest.mark.asyncio
async def test_projection_gc_executor_requires_plan_audit_reason_and_delete_callback() -> None:
    with pytest.raises(TypeError, match="ProjectionGcPlan"):
        await execute_projection_gc_plan(
            plan="not-a-plan",  # type: ignore[arg-type]
            audit_callback=lambda event: None,
            reason="retention_window",
        )

    with pytest.raises(ValueError, match="audit_callback"):
        await execute_projection_gc_plan(
            plan=_plan(),
            audit_callback=None,  # type: ignore[arg-type]
            reason="retention_window",
        )

    with pytest.raises(ValueError, match="reason"):
        await execute_projection_gc_plan(
            plan=_plan(),
            audit_callback=lambda event: None,
            reason=None,  # type: ignore[arg-type]
        )

    with pytest.raises(ValueError, match="live_delete_enabled"):
        await execute_projection_gc_plan(
            plan=_plan(),
            audit_callback=lambda event: None,
            reason="retention_window",
            dry_run=False,
        )

    with pytest.raises(ValueError, match="enabled projection GC policy"):
        await execute_projection_gc_plan(
            plan=ProjectionGcPlan(
                retained_forest_versions=frozenset({"v2"}),
                dropped_forest_versions=frozenset({"v1"}),
                deletable_body_ids=frozenset({"body-a"}),
            ),
            audit_callback=lambda event: None,
            reason="retention_window",
            dry_run=False,
            live_delete_enabled=True,
            delete_callback=lambda body_id: None,
        )

    with pytest.raises(ValueError, match="live-delete projection GC policy"):
        await execute_projection_gc_plan(
            plan=ProjectionGcPlan(
                retained_forest_versions=frozenset({"v2"}),
                dropped_forest_versions=frozenset({"v1"}),
                deletable_body_ids=frozenset({"body-a"}),
                policy_enabled=True,
                retention_count=1,
            ),
            audit_callback=lambda event: None,
            reason="retention_window",
            dry_run=False,
            live_delete_enabled=True,
            delete_callback=lambda body_id: None,
        )

    with pytest.raises(ValueError, match="delete_callback"):
        await execute_projection_gc_plan(
            plan=_plan(),
            audit_callback=lambda event: None,
            reason="retention_window",
            dry_run=False,
            live_delete_enabled=True,
        )


@pytest.mark.asyncio
async def test_projection_gc_executor_deletes_only_after_intent_audit() -> None:
    events: list[ProjectionGcAuditEvent] = []
    deleted: list[str] = []

    async def audit(event: ProjectionGcAuditEvent) -> None:
        events.append(event)

    async def delete(body_id: str) -> None:
        assert events[-1].action == "delete_intent"
        assert events[-1].body_id == body_id
        deleted.append(body_id)

    result = await execute_projection_gc_plan(
        plan=_plan(),
        audit_callback=audit,
        delete_callback=delete,
        reason="retention_window",
        dry_run=False,
        live_delete_enabled=True,
    )

    assert deleted == ["body-a", "body-b"]
    assert result.deleted_body_ids == ("body-a", "body-b")
    assert [event.action for event in events] == [
        "plan_started",
        "delete_intent",
        "delete_succeeded",
        "delete_intent",
        "delete_succeeded",
        "plan_completed",
    ]


@pytest.mark.asyncio
async def test_projection_gc_executor_does_not_delete_when_intent_audit_fails() -> None:
    deleted: list[str] = []

    def audit(event: ProjectionGcAuditEvent) -> None:
        if event.action == "delete_intent":
            raise RuntimeError("audit unavailable")

    with pytest.raises(RuntimeError, match="audit unavailable"):
        await execute_projection_gc_plan(
            plan=_plan(),
            audit_callback=audit,
            delete_callback=deleted.append,
            reason="retention_window",
            dry_run=False,
            live_delete_enabled=True,
        )

    assert deleted == []


@pytest.mark.asyncio
async def test_projection_gc_executor_audits_delete_failure() -> None:
    events: list[ProjectionGcAuditEvent] = []

    def delete(body_id: str) -> None:
        raise RuntimeError(f"cannot delete {body_id}")

    with pytest.raises(ProjectionGcExecutionError) as exc:
        await execute_projection_gc_plan(
            plan=_plan(),
            audit_callback=events.append,
            delete_callback=delete,
            reason="retention_window",
            dry_run=False,
            live_delete_enabled=True,
        )

    assert exc.value.body_id == "body-a"
    assert [event.action for event in events] == ["plan_started", "delete_intent", "delete_failed"]
    assert events[-1].body_id == "body-a"
    assert events[-1].metadata["error_type"] == "RuntimeError"


@pytest.mark.asyncio
async def test_projection_gc_executor_preserves_delete_error_when_failure_audit_fails() -> None:
    def audit(event: ProjectionGcAuditEvent) -> None:
        if event.action == "delete_failed":
            raise RuntimeError("audit failed")

    def delete(body_id: str) -> None:
        raise RuntimeError(f"cannot delete {body_id}")

    with pytest.raises(ProjectionGcExecutionError) as exc:
        await execute_projection_gc_plan(
            plan=_plan(),
            audit_callback=audit,
            delete_callback=delete,
            reason="retention_window",
            dry_run=False,
            live_delete_enabled=True,
        )

    assert exc.value.body_id == "body-a"
    assert isinstance(exc.value.__cause__, RuntimeError)
    assert "cannot delete body-a" in str(exc.value.__cause__)
