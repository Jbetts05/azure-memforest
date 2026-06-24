from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.partial_hydration_canary_gate import evaluate_artifact


AUTHORITY_CONFIRMATIONS = (
    "rollback_switch_confirmed",
    "guarded_switch_config_confirmed",
    "tree_scoped_projection_writes_confirmed",
    "live_canary_reviewed",
)


def evaluate_authority_preflight(
    artifact: dict[str, Any],
    *,
    confirmations: dict[str, bool] | None = None,
) -> dict[str, Any]:
    confirmations = dict(confirmations or {})
    canary_report = evaluate_artifact(artifact, require_live_canary=True)
    unknown_confirmations = sorted(set(confirmations) - set(AUTHORITY_CONFIRMATIONS))
    missing_confirmations = [
        name
        for name in AUTHORITY_CONFIRMATIONS
        if confirmations.get(name) is not True
    ]
    blockers = [
        *[f"canary:{blocker}" for blocker in canary_report["promotion_blockers"]],
        *[f"confirmation:{name}" for name in missing_confirmations],
        *[f"unknown_confirmation:{name}" for name in unknown_confirmations],
    ]
    return {
        "schema_version": 1,
        "artifact_run_id": canary_report.get("artifact_run_id"),
        "canary_promotion_allowed": canary_report["promotion_allowed"],
        "required_confirmations": list(AUTHORITY_CONFIRMATIONS),
        "missing_confirmations": missing_confirmations,
        "unknown_confirmations": unknown_confirmations,
        "authority_allowed": not blockers,
        "authority_blockers": blockers,
        "canary_report": canary_report,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preflight partial-hydration authoritative rollout readiness."
    )
    parser.add_argument("artifact", help="Trusted live canary artifact JSON.")
    parser.add_argument("--confirm-rollback-switch", action="store_true")
    parser.add_argument("--confirm-guarded-switch-config", action="store_true")
    parser.add_argument("--confirm-tree-scoped-projection-writes", action="store_true")
    parser.add_argument("--confirm-live-canary-reviewed", action="store_true")
    parser.add_argument("--output", help="Optional path for authority preflight report JSON.")
    args = parser.parse_args()

    artifact = json.loads(Path(args.artifact).read_text(encoding="utf-8"))
    report = evaluate_authority_preflight(
        artifact,
        confirmations={
            "rollback_switch_confirmed": bool(args.confirm_rollback_switch),
            "guarded_switch_config_confirmed": bool(args.confirm_guarded_switch_config),
            "tree_scoped_projection_writes_confirmed": bool(args.confirm_tree_scoped_projection_writes),
            "live_canary_reviewed": bool(args.confirm_live_canary_reviewed),
        },
    )
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["authority_allowed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
