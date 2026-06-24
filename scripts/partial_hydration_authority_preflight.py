from __future__ import annotations

import argparse
import json
import sys
import time
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
MAX_FUTURE_SKEW_SECONDS = 300


def evaluate_authority_preflight(
    artifact: dict[str, Any],
    *,
    confirmations: dict[str, bool] | None = None,
    expected_code_version: str | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    confirmations = dict(confirmations or {})
    canary_report = evaluate_artifact(artifact, require_live_canary=True)
    unknown_confirmations = sorted(set(confirmations) - set(AUTHORITY_CONFIRMATIONS))
    missing_confirmations = [
        name
        for name in AUTHORITY_CONFIRMATIONS
        if confirmations.get(name) is not True
    ]
    provenance = artifact.get("provenance") if isinstance(artifact, dict) else {}
    provenance = provenance if isinstance(provenance, dict) else {}
    code_version_blockers = []
    normalized_expected_code_version = str(expected_code_version or "").strip() if expected_code_version is not None else None
    if expected_code_version is not None and not normalized_expected_code_version:
        code_version_blockers.append("expected_code_version_missing")
    elif normalized_expected_code_version:
        actual_code_version = str(provenance.get("code_version") or "").strip()
        if actual_code_version != normalized_expected_code_version:
            code_version_blockers.append("code_version_mismatch")
    freshness_blockers = []
    if max_age_seconds is not None:
        try:
            created_at = float(provenance.get("created_at_unix"))
        except (TypeError, ValueError):
            created_at = 0.0
        if created_at <= 0.0:
            freshness_blockers.append("created_at_unix_missing")
        else:
            now = time.time()
            if created_at - now > MAX_FUTURE_SKEW_SECONDS:
                freshness_blockers.append("created_at_unix_in_future")
            elif now - created_at > max_age_seconds:
                freshness_blockers.append("artifact_too_old")
    blockers = [
        *[f"canary:{blocker}" for blocker in canary_report["promotion_blockers"]],
        *[f"confirmation:{name}" for name in missing_confirmations],
        *[f"unknown_confirmation:{name}" for name in unknown_confirmations],
        *[f"code_version:{name}" for name in code_version_blockers],
        *[f"freshness:{name}" for name in freshness_blockers],
    ]
    return {
        "schema_version": 1,
        "artifact_run_id": canary_report.get("artifact_run_id"),
        "artifact_created_at_unix": provenance.get("created_at_unix"),
        "canary_promotion_allowed": canary_report["promotion_allowed"],
        "required_confirmations": list(AUTHORITY_CONFIRMATIONS),
        "missing_confirmations": missing_confirmations,
        "unknown_confirmations": unknown_confirmations,
        "expected_code_version": normalized_expected_code_version,
        "artifact_code_version": provenance.get("code_version"),
        "code_version_failures": code_version_blockers,
        "max_age_seconds": max_age_seconds,
        "freshness_failures": freshness_blockers,
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
    parser.add_argument("--expected-code-version", required=True, help="Code version/SHA that the canary artifact must represent.")
    parser.add_argument("--max-age-seconds", type=int, default=86400, help="Maximum artifact age for authority preflight.")
    parser.add_argument("--output", help="Optional path for authority preflight report JSON.")
    args = parser.parse_args()

    artifact = json.loads(Path(args.artifact).read_text(encoding="utf-8"))
    if not args.expected_code_version.strip():
        raise SystemExit("--expected-code-version must be non-empty")
    report = evaluate_authority_preflight(
        artifact,
        confirmations={
            "rollback_switch_confirmed": bool(args.confirm_rollback_switch),
            "guarded_switch_config_confirmed": bool(args.confirm_guarded_switch_config),
            "tree_scoped_projection_writes_confirmed": bool(args.confirm_tree_scoped_projection_writes),
            "live_canary_reviewed": bool(args.confirm_live_canary_reviewed),
        },
        expected_code_version=args.expected_code_version,
        max_age_seconds=args.max_age_seconds,
    )
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["authority_allowed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
