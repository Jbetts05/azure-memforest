from __future__ import annotations

import argparse
import json
import os
import shlex
from pathlib import Path
from typing import Any


REQUIRED_ENV = (
    "AZURE_MEMFOREST_LIVE_DURABLE_SMOKE",
    "AZURE_MEMFOREST_API_URL",
    "AZURE_MEMFOREST_DURABLE_START_URL",
    "AZURE_MEMFOREST_DURABLE_FUNCTION_KEY",
    "AZURE_MEMFOREST_TENANT_ID",
    "AZURE_MEMFOREST_COSMOS_ENDPOINT",
    "AZURE_MEMFOREST_COSMOS_DATABASE",
    "AZURE_CLIENT_ID",
    "AZURE_MEMFOREST_GIT_SHA",
)

REQUIRED_DIAGNOSTIC_FLAGS = {
    "AZURE_MEMFOREST_ENABLE_PRE_INGEST_ROUTE_PREDICTION_PREPASS": "true",
    "AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_SHADOW": "true",
    "AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_EQUIVALENCE_SHADOW": "true",
    "AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_GUARDED_SWITCH": "false",
    "AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_AUTHORITATIVE_SWITCH": "false",
}
ADVISORY_DIAGNOSTIC_FLAGS = {
    "AZURE_MEMFOREST_ENABLE_PARTIAL_HYDRATION_DUAL_COMPUTE_SHADOW": "true",
}


def _quote(value: str) -> str:
    return shlex.quote(str(value))


def _missing_env(values: dict[str, str]) -> list[str]:
    missing = [name for name in REQUIRED_ENV if not str(values.get(name, "")).strip()]
    if str(values.get("AZURE_MEMFOREST_LIVE_DURABLE_SMOKE", "")).strip() != "1":
        missing.append("AZURE_MEMFOREST_LIVE_DURABLE_SMOKE=1")
    if str(values.get("AZURE_MEMFOREST_GIT_SHA", "")).strip().lower() == "unknown":
        missing.append("AZURE_MEMFOREST_GIT_SHA(non-unknown)")
    return sorted(set(missing))


def _env_flag(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _diagnostic_flag_mismatches(values: dict[str, str]) -> list[str]:
    mismatches = []
    for name, expected in REQUIRED_DIAGNOSTIC_FLAGS.items():
        expected_enabled = expected == "true"
        if _env_flag(values.get(name)) is not expected_enabled:
            mismatches.append(f"{name}={expected}")
    return sorted(mismatches)


def build_runbook(
    values: dict[str, str],
    *,
    output_root: str = "artifacts/partial-hydration-live-canary",
    session_count: int = 3,
    auth_mode: str = "dev",
    run_channel: str = "trusted-ci",
) -> dict[str, Any]:
    missing = _missing_env(values)
    flag_mismatches = _diagnostic_flag_mismatches(values)
    git_sha = str(values.get("AZURE_MEMFOREST_GIT_SHA") or "").strip()
    output = str(output_root).rstrip("/\\")
    local_artifact = "<path printed by partial_hydration_shadow_evidence.py>"
    smoke_artifact = "<path printed by durable_ingest_smoke.py>"
    live_artifact = f"{output}/live-canary.json"
    gate_report = f"{output}/canary-gate.json"
    authority_report = f"{output}/authority-preflight.json"
    commands = [
        f"python scripts/partial_hydration_shadow_evidence.py --output-root {_quote(output)}",
        (
            "python scripts/durable_ingest_smoke.py "
            f"--api-url {_quote(values.get('AZURE_MEMFOREST_API_URL', ''))} "
            f"--durable-start-url {_quote(values.get('AZURE_MEMFOREST_DURABLE_START_URL', ''))} "
            f"--tenant-id {_quote(values.get('AZURE_MEMFOREST_TENANT_ID', ''))} "
            f"--function-key <from AZURE_MEMFOREST_DURABLE_FUNCTION_KEY> "
            f"--durable-status-key <from AZURE_MEMFOREST_DURABLE_STATUS_KEY or host durable key> "
            f"--auth-mode {_quote(auth_mode)} --session-count {int(session_count)} "
            f"--cosmos-endpoint {_quote(values.get('AZURE_MEMFOREST_COSMOS_ENDPOINT', ''))} "
            f"--cosmos-database {_quote(values.get('AZURE_MEMFOREST_COSMOS_DATABASE', ''))} "
            f"--azure-client-id {_quote(values.get('AZURE_CLIENT_ID', ''))} "
            f"--export-manifest-metadata --output-root {_quote(output)}"
        ),
        (
            "python scripts/partial_hydration_live_canary_artifact.py "
            f"--local-evidence {_quote(local_artifact)} "
            f"--metadata-json {_quote(smoke_artifact)} "
            f"--output {_quote(live_artifact)} "
            f"--run-channel {_quote(run_channel)} "
            f"--code-version {_quote(git_sha)}"
        ),
        f"python scripts/partial_hydration_canary_gate.py {_quote(live_artifact)} --output {_quote(gate_report)}",
        (
            "python scripts/partial_hydration_authority_preflight.py "
            f"{_quote(live_artifact)} "
            f"--expected-code-version {_quote(git_sha)} "
            "--confirm-rollback-switch "
            "--confirm-guarded-switch-config "
            "--confirm-tree-scoped-projection-writes "
            "--confirm-live-canary-reviewed "
            f"--output {_quote(authority_report)}"
        ),
    ]
    return {
        "schema_version": 1,
        "ready": not missing and not flag_mismatches,
        "missing_env": missing,
        "required_diagnostic_flags": REQUIRED_DIAGNOSTIC_FLAGS,
        "advisory_diagnostic_flags": ADVISORY_DIAGNOSTIC_FLAGS,
        "diagnostic_flag_mismatches": flag_mismatches,
        "session_count": int(session_count),
        "code_version": git_sha,
        "artifacts": {
            "local_evidence": local_artifact,
            "durable_smoke": smoke_artifact,
            "live_canary": live_artifact,
            "canary_gate": gate_report,
            "authority_preflight": authority_report,
        },
        "commands": commands,
        "notes": [
            "Enable diagnostic flags in the deployed worker/API environment before the live run.",
            "Flag readiness uses local env as a checklist proxy; the canary gate remains authoritative because it reads worker-produced metadata.",
            "Keep authoritative switch disabled until authority preflight passes and rollout approval is explicit.",
            "Use a fresh user_id for the durable smoke so exported manifests correspond to this run.",
            "Use the exact artifact paths printed by partial_hydration_shadow_evidence.py and durable_ingest_smoke.py; both filenames are dynamic.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate partial-hydration live canary runbook commands.")
    parser.add_argument("--output-root", default="artifacts/partial-hydration-live-canary")
    parser.add_argument("--session-count", type=int, default=3)
    parser.add_argument("--auth-mode", choices=["dev", "benchmark"], default="dev")
    parser.add_argument("--run-channel", default="trusted-ci")
    parser.add_argument("--output", help="Optional JSON runbook output path.")
    args = parser.parse_args()
    runbook = build_runbook(
        dict(os.environ),
        output_root=args.output_root,
        session_count=args.session_count,
        auth_mode=args.auth_mode,
        run_channel=args.run_channel,
    )
    if args.output:
        Path(args.output).write_text(json.dumps(runbook, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(runbook, indent=2, sort_keys=True))
    if not runbook["ready"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
