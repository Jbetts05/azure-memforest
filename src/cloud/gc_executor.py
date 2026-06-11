"""Audited execution scaffold for projection GC plans.

This module is intentionally not wired into live cleanup paths.  It provides a
small, explicit boundary around :mod:`src.cloud.gc_planner` output so future
callers must supply both a concrete plan and an audit/deletion-log callback
before any projection body deletion callback can run.
"""

from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Literal, Mapping

from src.cloud.gc_planner import ProjectionGcPlan

ProjectionGcAuditAction = Literal[
    "plan_started",
    "delete_intent",
    "delete_skipped_dry_run",
    "delete_succeeded",
    "delete_failed",
    "plan_completed",
]

ProjectionGcAuditCallback = Callable[["ProjectionGcAuditEvent"], Any | Awaitable[Any]]
ProjectionGcDeleteCallback = Callable[[str], Any | Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class ProjectionGcAuditEvent:
    """One auditable execution event emitted before and after deletion attempts."""

    action: ProjectionGcAuditAction
    plan_sha256: str
    dry_run: bool
    reason: str
    body_id: str | None = None
    tenant_id: str | None = None
    user_id: str | None = None
    retained_forest_versions: tuple[str, ...] = field(default_factory=tuple)
    dropped_forest_versions: tuple[str, ...] = field(default_factory=tuple)
    deletable_body_count: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "retained_forest_versions", tuple(self.retained_forest_versions))
        object.__setattr__(self, "dropped_forest_versions", tuple(self.dropped_forest_versions))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class ProjectionGcExecutionResult:
    """Summary of a dry-run or deletion-callback execution."""

    dry_run: bool
    plan_sha256: str
    intended_body_ids: tuple[str, ...]
    deleted_body_ids: tuple[str, ...] = field(default_factory=tuple)
    skipped_body_ids: tuple[str, ...] = field(default_factory=tuple)
    audit_event_count: int = 0


class ProjectionGcExecutionError(RuntimeError):
    """Raised after a deletion callback fails and a failure audit was attempted."""

    def __init__(self, body_id: str, message: str) -> None:
        super().__init__(message)
        self.body_id = body_id


async def execute_projection_gc_plan(
    *,
    plan: ProjectionGcPlan,
    audit_callback: ProjectionGcAuditCallback,
    reason: str,
    delete_callback: ProjectionGcDeleteCallback | None = None,
    dry_run: bool = True,
    live_delete_enabled: bool = False,
    tenant_id: str | None = None,
    user_id: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> ProjectionGcExecutionResult:
    """Execute a projection GC plan through explicit audit and delete callbacks.

    The default is a dry run.  Non-dry-run execution refuses to proceed unless a
    deletion callback, explicit live-delete flag, and enabled retention policy are
    supplied.  Every deletion callback invocation is preceded by a
    ``delete_intent`` audit event; if that audit fails, deletion is not attempted.
    """

    if not isinstance(plan, ProjectionGcPlan):
        raise TypeError("plan must be a ProjectionGcPlan produced by plan_projection_gc")
    if audit_callback is None:
        raise ValueError("audit_callback is required")
    reason = str(reason or "").strip()
    if not reason:
        raise ValueError("reason is required")
    if not dry_run:
        if not live_delete_enabled:
            raise ValueError("live_delete_enabled is required when dry_run is False")
        if not plan.policy_enabled:
            raise ValueError("an enabled projection GC policy is required when dry_run is False")
        if not plan.live_delete_enabled:
            raise ValueError("an explicit live-delete projection GC policy is required when dry_run is False")
        if plan.retention_count < 1:
            raise ValueError("retention_count must be positive when dry_run is False")
        if not plan.retained_forest_versions:
            raise ValueError("at least one retained forest version is required when dry_run is False")
        if delete_callback is None:
            raise ValueError("delete_callback is required when dry_run is False")

    body_ids = tuple(sorted(plan.deletable_body_ids))
    plan_sha256 = projection_gc_plan_sha256(plan)
    common = {
        "plan_sha256": plan_sha256,
        "dry_run": dry_run,
        "reason": reason,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "retained_forest_versions": tuple(sorted(plan.retained_forest_versions)),
        "dropped_forest_versions": tuple(sorted(plan.dropped_forest_versions)),
        "deletable_body_count": len(body_ids),
        "metadata": {
            **dict(metadata or {}),
            "policy_enabled": plan.policy_enabled,
            "retention_count": plan.retention_count,
            "policy_live_delete_enabled": plan.live_delete_enabled,
            "execution_live_delete_enabled": live_delete_enabled,
            "deletable_record_count": len(plan.deletable_record_ids),
        },
    }
    audit_count = 0

    await _call_audit(audit_callback, ProjectionGcAuditEvent(action="plan_started", **common))
    audit_count += 1

    deleted: list[str] = []
    skipped: list[str] = []
    for body_id in body_ids:
        await _call_audit(audit_callback, ProjectionGcAuditEvent(action="delete_intent", body_id=body_id, **common))
        audit_count += 1
        if dry_run:
            skipped.append(body_id)
            await _call_audit(
                audit_callback,
                ProjectionGcAuditEvent(action="delete_skipped_dry_run", body_id=body_id, **common),
            )
            audit_count += 1
            continue

        try:
            await _maybe_await(delete_callback(body_id))  # type: ignore[misc]
        except Exception as exc:
            try:
                await _call_audit(
                    audit_callback,
                    ProjectionGcAuditEvent(
                        action="delete_failed",
                        body_id=body_id,
                        metadata={**dict(metadata or {}), "error_type": type(exc).__name__},
                        **{key: value for key, value in common.items() if key != "metadata"},
                    ),
                )
            except Exception:
                pass
            raise ProjectionGcExecutionError(body_id, f"Projection GC deletion failed for {body_id!r}.") from exc

        deleted.append(body_id)
        await _call_audit(audit_callback, ProjectionGcAuditEvent(action="delete_succeeded", body_id=body_id, **common))
        audit_count += 1

    await _call_audit(audit_callback, ProjectionGcAuditEvent(action="plan_completed", **common))
    audit_count += 1

    return ProjectionGcExecutionResult(
        dry_run=dry_run,
        plan_sha256=plan_sha256,
        intended_body_ids=body_ids,
        deleted_body_ids=tuple(deleted),
        skipped_body_ids=tuple(skipped),
        audit_event_count=audit_count,
    )


def projection_gc_plan_sha256(plan: ProjectionGcPlan) -> str:
    """Return a stable digest for audit correlation."""

    if not isinstance(plan, ProjectionGcPlan):
        raise TypeError("plan must be a ProjectionGcPlan")
    payload = {
        "retained_forest_versions": sorted(plan.retained_forest_versions),
        "dropped_forest_versions": sorted(plan.dropped_forest_versions),
        "deletable_body_ids": sorted(plan.deletable_body_ids),
        "deletable_record_ids": sorted(plan.deletable_record_ids),
        "policy_enabled": plan.policy_enabled,
        "retention_count": plan.retention_count,
        "live_delete_enabled": plan.live_delete_enabled,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def _call_audit(callback: ProjectionGcAuditCallback, event: ProjectionGcAuditEvent) -> None:
    await _maybe_await(callback(event))


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
