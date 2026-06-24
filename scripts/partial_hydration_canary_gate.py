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


def evaluate_artifact(artifact: dict[str, Any], *, require_live_canary: bool) -> dict[str, Any]:
    # The live mode is self-asserted by the artifact. Use the non-local path
    # only for artifacts produced by a trusted canary/CI channel; do not accept
    # arbitrary operator-supplied JSON for authoritative promotion.
    if not isinstance(artifact, dict):
        artifact = {}
    gates = dict(artifact.get("gate") or {})
    missing = [name for name in REQUIRED_LOCAL_GATES if name not in gates]
    failed = [name for name in REQUIRED_LOCAL_GATES if gates.get(name) is not True]
    mode = str(artifact.get("mode") or "").strip().lower()
    trusted_live = mode in TRUSTED_LIVE_MODES
    live_required_failed = require_live_canary and not trusted_live
    return {
        "schema_version": 1,
        "artifact_run_id": artifact.get("run_id"),
        "mode": mode,
        "local_gates_passed": not missing and not failed,
        "missing_gates": missing,
        "failed_gates": failed,
        "live_canary_required": require_live_canary,
        "live_canary_satisfied": not live_required_failed,
        "trusted_live_mode": trusted_live,
        "promotion_allowed": not missing and not failed and not live_required_failed,
        "promotion_blockers": [
            *[f"missing_gate:{name}" for name in missing],
            *[f"failed_gate:{name}" for name in failed],
            *(["live_canary_required"] if live_required_failed else []),
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
