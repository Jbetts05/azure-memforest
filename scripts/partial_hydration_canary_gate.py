from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REQUIRED_LOCAL_GATES = (
    "clean_case_no_under_selection",
    "clean_case_covers_oracle",
    "clean_case_predicted_shadow_supported",
    "clean_case_scoped_equivalent",
    "clean_case_no_unassessed_kinds",
    "under_selection_detected",
    "escalation_blocks_predicted_shadow",
    "full_commit_path_used",
    "dual_commit_store_outputs_equal",
    "dual_commit_metadata_without_shadow_equal",
    "dual_commit_oracle_shadow_disabled",
    "dual_commit_predicted_shadow_supported",
)

TRUSTED_LIVE_MODES = frozenset({"live_canary", "trusted_live_canary"})
TRUSTED_LIVE_PRODUCERS = frozenset({"azure-memforest-live-canary", "trusted-ci-canary"})
MIN_ASSESSED_KINDS = 4
MIN_COMMIT_STORE_COUNT = 4


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coverage_int(coverage: dict[str, Any], name: str) -> int | None:
    value = coverage.get(name)
    if value is None or value == "":
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def evaluate_artifact(artifact: dict[str, Any], *, require_live_canary: bool) -> dict[str, Any]:
    # Provenance fields are still artifact data, not a cryptographic signature.
    # Use the non-local path only for artifacts from a trusted canary/CI
    # channel; do not accept arbitrary operator-supplied JSON for promotion.
    if not isinstance(artifact, dict):
        artifact = {}
    gates = _mapping(artifact.get("gate"))
    coverage = _mapping(artifact.get("coverage"))
    provenance = _mapping(artifact.get("provenance"))
    missing = [name for name in REQUIRED_LOCAL_GATES if name not in gates]
    failed = [name for name in REQUIRED_LOCAL_GATES if gates.get(name) is not True]
    coverage_failures = []
    coverage_thresholds = (
        ("case_count", 3),
        ("clean_case_actual_tree_count", 1),
        ("clean_case_planned_tree_count", 1),
        ("clean_case_assessed_kind_count", MIN_ASSESSED_KINDS),
        ("negative_case_missing_tree_count", 1),
        ("commit_safety_store_count", MIN_COMMIT_STORE_COUNT),
    )
    for name, minimum in coverage_thresholds:
        value = _coverage_int(coverage, name)
        if value is None or value < minimum:
            coverage_failures.append(name)
    mode = str(artifact.get("mode") or "").strip().lower()
    trusted_live = mode in TRUSTED_LIVE_MODES
    producer = str(provenance.get("producer") or "").strip().lower()
    run_channel = str(provenance.get("run_channel") or "").strip()
    live_provenance_failures = []
    if require_live_canary:
        if provenance.get("live_services_used") is not True:
            live_provenance_failures.append("live_services_used")
        if provenance.get("fake_stores_used") is not False:
            live_provenance_failures.append("fake_stores_used")
        if producer not in TRUSTED_LIVE_PRODUCERS:
            live_provenance_failures.append("producer")
        if not run_channel:
            live_provenance_failures.append("run_channel")
    live_required_failed = require_live_canary and not trusted_live
    live_provenance_failed = bool(live_provenance_failures)
    return {
        "schema_version": 1,
        "artifact_run_id": artifact.get("run_id"),
        "mode": mode,
        "local_gates_passed": not missing and not failed,
        "coverage_passed": not coverage_failures,
        "missing_gates": missing,
        "failed_gates": failed,
        "coverage_failures": coverage_failures,
        "live_canary_required": require_live_canary,
        "live_canary_satisfied": not live_required_failed and not live_provenance_failed,
        "trusted_live_mode": trusted_live,
        "trusted_live_provenance_satisfied": not live_provenance_failed,
        "live_provenance_failures": live_provenance_failures,
        "promotion_allowed": not missing
        and not failed
        and not coverage_failures
        and not live_required_failed
        and not live_provenance_failed,
        "promotion_blockers": [
            *[f"missing_gate:{name}" for name in missing],
            *[f"failed_gate:{name}" for name in failed],
            *[f"coverage:{name}" for name in coverage_failures],
            *(["live_canary_required"] if live_required_failed else []),
            *[f"live_provenance:{name}" for name in live_provenance_failures],
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate partial hydration evidence before canary/promotion.")
    parser.add_argument("artifact", help="Path to partial_hydration_shadow_evidence artifact JSON.")
    parser.add_argument(
        "--allow-local-only",
        action="store_true",
        help="Do not require a non-fake/live canary artifact. Use only for local development.",
    )
    parser.add_argument("--output", help="Optional path for gate report JSON.")
    args = parser.parse_args()

    artifact_path = Path(args.artifact)
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    report = evaluate_artifact(artifact, require_live_canary=not args.allow_local_only)
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    if not report["promotion_allowed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
