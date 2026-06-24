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

from scripts.partial_hydration_canary_gate import TRUSTED_LIVE_PRODUCERS

REQUIRED_STAGE_TIMINGS = frozenset({"hydrate", "extract_build", "project", "write_projection", "total_before_commit"})


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_objects(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        manifest_metadata = value.get("manifest_metadata")
        if isinstance(manifest_metadata, dict):
            return _iter_objects(manifest_metadata)
        for key in ("records", "manifests", "items", "cases", "materializations"):
            nested = value.get(key)
            if isinstance(nested, list):
                return nested
    return [value]


def _metadata_from_object(value: Any) -> dict[str, Any] | None:
    obj = _mapping(value)
    if "pre_ingest_scope_prediction" in obj:
        return obj
    metadata = _mapping(obj.get("metadata"))
    if "pre_ingest_scope_prediction" in metadata:
        return metadata
    manifest = _mapping(obj.get("manifest"))
    metadata = _mapping(manifest.get("metadata"))
    if "pre_ingest_scope_prediction" in metadata:
        return metadata
    return None


def collect_metadata_records(paths: list[Path]) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    input_object_count = 0
    unparsed_record_count = 0
    for path in paths:
        value = _load_json(path)
        for item in _iter_objects(value):
            input_object_count += 1
            metadata = _metadata_from_object(item)
            if metadata is not None:
                records.append(metadata)
            else:
                unparsed_record_count += 1
    return {
        "records": records,
        "input_object_count": input_object_count,
        "unparsed_record_count": unparsed_record_count,
    }


def load_metadata_records(paths: list[Path]) -> list[dict[str, Any]]:
    return list(collect_metadata_records(paths)["records"])


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _is_number(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def summarize_live_metadata(
    records: list[dict[str, Any]],
    *,
    input_object_count: int | None = None,
    unparsed_record_count: int = 0,
    code_version: str | None = None,
) -> dict[str, Any]:
    unsupported_prediction_count = 0
    under_selection_count = 0
    unsupported_predicted_shadow_count = 0
    scoped_divergence_count = 0
    unassessed_kind_count = 0
    partial_resident_commit_count = 0
    missing_model_telemetry_count = 0
    missing_stage_timing_count = 0
    missing_code_version_count = 0
    mismatched_code_version_count = 0
    expected_code_version = str(code_version or "").strip()
    total_model_calls = 0
    total_model_failures = 0
    total_model_started_calls = 0
    total_model_latency_ms = 0.0
    stage_timing_totals_ms: dict[str, float] = {}
    for metadata in records:
        prediction = _mapping(metadata.get("pre_ingest_scope_prediction"))
        shadow = _mapping(metadata.get("predicted_subset_equivalence_shadow"))
        scoped = _mapping(shadow.get("scoped_comparison"))
        safety = _mapping(metadata.get("partial_residency_projection_safety"))
        record_code_version = str(metadata.get("code_version") or "").strip()
        if not record_code_version:
            missing_code_version_count += 1
        elif expected_code_version and record_code_version != expected_code_version:
            mismatched_code_version_count += 1
        if prediction.get("supported") is not True:
            unsupported_prediction_count += 1
        if prediction.get("under_selected_without_escalation") is True:
            under_selection_count += 1
        if shadow.get("supported") is not True:
            unsupported_predicted_shadow_count += 1
        if scoped.get("equivalent") is not True:
            scoped_divergence_count += 1
        unassessed = scoped.get("unassessed_kinds") or []
        if isinstance(unassessed, list):
            unassessed_kind_count += len(unassessed)
        else:
            unassessed_kind_count += 1
        if safety.get("partial_resident") is not False:
            partial_resident_commit_count += 1
        telemetry = _mapping(metadata.get("materialization_model_telemetry"))
        telemetry_numeric_keys = ("total_calls", "total_failures", "total_started_calls", "total_latency_ms")
        if telemetry.get("schema_version") != 1 or any(not _is_number(telemetry.get(key)) for key in telemetry_numeric_keys):
            missing_model_telemetry_count += 1
        total_model_calls += int(_number(telemetry.get("total_calls")))
        total_model_failures += int(_number(telemetry.get("total_failures")))
        total_model_started_calls += int(_number(telemetry.get("total_started_calls")))
        total_model_latency_ms += _number(telemetry.get("total_latency_ms"))
        timings = _mapping(metadata.get("materialization_stage_timings_ms"))
        if not REQUIRED_STAGE_TIMINGS.issubset(set(timings)) or any(
            not _is_number(timings.get(stage))
            for stage in REQUIRED_STAGE_TIMINGS
        ):
            missing_stage_timing_count += 1
        for stage, elapsed_ms in timings.items():
            try:
                stage_timing_totals_ms[str(stage)] = stage_timing_totals_ms.get(str(stage), 0.0) + float(elapsed_ms)
            except (TypeError, ValueError):
                continue
    return {
        "schema_version": 1,
        "input_object_count": len(records) if input_object_count is None else input_object_count,
        "metadata_record_count": len(records),
        "unparsed_record_count": unparsed_record_count,
        "under_selection_count": under_selection_count,
        "unsupported_prediction_count": unsupported_prediction_count,
        "unsupported_predicted_shadow_count": unsupported_predicted_shadow_count,
        "scoped_divergence_count": scoped_divergence_count,
        "unassessed_kind_count": unassessed_kind_count,
        "partial_resident_commit_count": partial_resident_commit_count,
        "missing_model_telemetry_count": missing_model_telemetry_count,
        "missing_stage_timing_count": missing_stage_timing_count,
        "expected_code_version": expected_code_version or None,
        "missing_code_version_count": missing_code_version_count,
        "mismatched_code_version_count": mismatched_code_version_count,
        "model_call_totals": {
            "total_calls": total_model_calls,
            "total_failures": total_model_failures,
            "total_started_calls": total_model_started_calls,
            "total_latency_ms": round(total_model_latency_ms, 3),
        },
        "stage_timing_totals_ms": {
            stage: round(elapsed, 3)
            for stage, elapsed in sorted(stage_timing_totals_ms.items())
        },
        "checks": {
            "zero_under_selection": under_selection_count == 0,
            "all_predictions_supported": unsupported_prediction_count == 0,
            "all_predicted_shadows_supported": unsupported_predicted_shadow_count == 0,
            "all_scoped_comparisons_equivalent": scoped_divergence_count == 0,
            "no_unassessed_kinds": unassessed_kind_count == 0,
            "full_commit_path_used": partial_resident_commit_count == 0,
            "no_unparsed_records": unparsed_record_count == 0,
            "all_model_telemetry_present": missing_model_telemetry_count == 0,
            "all_stage_timings_present": missing_stage_timing_count == 0,
            "all_records_match_code_version": bool(expected_code_version)
            and missing_code_version_count == 0
            and mismatched_code_version_count == 0,
        },
    }


def build_live_artifact(
    *,
    local_evidence: dict[str, Any],
    live_canary: dict[str, Any],
    producer: str,
    run_channel: str,
    code_version: str,
    mode: str = "live_canary",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": f"partial-hydration-live-canary-{int(time.time())}",
        "mode": mode,
        "provenance": {
            "producer": producer,
            "run_channel": run_channel,
            "live_services_used": True,
            "fake_stores_used": False,
            "code_version": code_version,
            "created_at_unix": time.time(),
        },
        "coverage": _mapping(local_evidence.get("coverage")),
        "gate": _mapping(local_evidence.get("gate")),
        "live_canary": live_canary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a trusted-live partial hydration canary artifact from materialization metadata."
    )
    parser.add_argument("--local-evidence", required=True, help="Passing local evidence artifact JSON.")
    parser.add_argument("--metadata-json", nargs="+", required=True, help="JSON files containing manifest metadata records.")
    parser.add_argument("--output", required=True, help="Output artifact path.")
    parser.add_argument("--producer", default="azure-memforest-live-canary", choices=sorted(TRUSTED_LIVE_PRODUCERS))
    parser.add_argument("--run-channel", required=True, help="Trusted CI/canary channel identifier.")
    parser.add_argument("--code-version", required=True, help="Git SHA or immutable code version represented by this canary.")
    args = parser.parse_args()

    local_evidence = _mapping(_load_json(Path(args.local_evidence)))
    collected = collect_metadata_records([Path(path) for path in args.metadata_json])
    artifact = build_live_artifact(
        local_evidence=local_evidence,
        live_canary=summarize_live_metadata(
            list(collected["records"]),
            input_object_count=int(collected["input_object_count"]),
            unparsed_record_count=int(collected["unparsed_record_count"]),
            code_version=args.code_version,
        ),
        producer=args.producer,
        run_channel=args.run_channel,
        code_version=args.code_version,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "artifact": str(output),
                "input_object_count": collected["input_object_count"],
                "metadata_record_count": len(collected["records"]),
                "unparsed_record_count": collected["unparsed_record_count"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
