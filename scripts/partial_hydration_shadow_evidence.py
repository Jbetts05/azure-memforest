from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from collections.abc import Mapping
from dataclasses import fields, is_dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.partial_hydration_canary_gate import TRUSTED_LIVE_MODES
from tests.test_worker_materialization import (
    FactRouteHint,
    FakeIngestCore,
    FakeManifestStore,
    ForestManifest,
    MatchingProjectionBaselineProvider,
    MaterializationHandlerConfig,
    PreIngestTreeScope,
    RecordingStore,
    RouteRiskFlags,
    _envelope,
    _handler,
    _snapshot,
    _snapshot_with_scoped_summary_cache,
    _two_tree_incremental_snapshot_with_fact_and_route_manifest,
)

LOCAL_FAKE_STORE_MODE = "deterministic_local_fake_stores_no_live_services"


def _stable(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"sha256": hashlib.sha256(value).hexdigest(), "size": len(value)}
    if isinstance(value, Mapping):
        return {str(k): _stable(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if is_dataclass(value):
        return {field.name: _stable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_stable(v) for v in value]
    if hasattr(value, "__dict__"):
        return _stable(vars(value))
    return value


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _get_path(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _manifest_store(route_index_manifest: dict[str, Any] | None) -> FakeManifestStore:
    store = FakeManifestStore()
    metadata = {"job_id": "previous", "job_type": "ingest_session", "embedding_model_version": "embed-v1"}
    if route_index_manifest is not None:
        metadata["route_index_manifest"] = route_index_manifest
    store.latest = ForestManifest(
        manifest_id="manifest-v1",
        tenant_id="tenant-1",
        user_id="user-1",
        forest_version="v1",
        generation=1,
        status="committed",
        created_at_unix=1.0,
        metadata=metadata,
    )
    return store


def _config(*, dual: bool, oracle: bool = False) -> MaterializationHandlerConfig:
    return MaterializationHandlerConfig(
        owner_id="worker-1",
        embedding_model_version="embed-v1",
        code_version="local-evidence",
        prompt_config_hash="prompt-hash",
        lease_renewal_seconds=9999,
        enable_pre_ingest_route_prediction_prepass=True,
        enable_partial_hydration_shadow=True,
        enable_partial_hydration_equivalence_shadow=oracle,
        enable_partial_hydration_dual_compute_shadow=dual,
    )


def _prediction(snapshot: Any, *, tree_ids: tuple[str, ...], full: bool = False, reasons: tuple[str, ...] = ()) -> Any:
    return SimpleNamespace(
        touched_facts=snapshot.facts,
        route_hints=(
            FactRouteHint(
                "fact-1",
                entity_keys=("alice", "user"),
                scene_similarities={"scene:travel": 1.0},
                scene_scope_key="travel",
                new_session_id="session-1",
                embedding=(1.0, 0.0),
            ),
        ),
        route_risks=RouteRiskFlags(cluster_merge_possible=False, suppression_flip_possible=False),
        pre_ingest_scope=PreIngestTreeScope(
            full_hydration_required=full,
            tree_ids_to_load=tree_ids,
            escalation_reasons=reasons if full else (),
            predicted_entity_tree_ids=tuple(t for t in tree_ids if t.startswith("entity:")),
            predicted_scene_tree_ids=tuple(t for t in tree_ids if t.startswith("tree:")),
        ),
    )


async def _run_case(
    name: str,
    *,
    snapshot: Any,
    route_index_manifest: dict[str, Any] | None,
    prediction: Any,
    shadow_snapshot: Any,
    dual: bool = True,
    oracle: bool = False,
) -> dict[str, Any]:
    calls: list[str] = []
    manifest_store = _manifest_store(route_index_manifest)
    core = FakeIngestCore(snapshot)
    core.pre_ingest_prediction = prediction
    core.partial_hydration_equivalence_snapshot = shadow_snapshot
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=core,
        baseline_provider=MatchingProjectionBaselineProvider(),
        config=_config(dual=dual, oracle=oracle),
    )
    await handler.handle(_envelope(), {"fencing_token": 1})
    metadata = manifest_store.latest.metadata
    return {
        "name": name,
        "prepass_called": bool(core.pre_ingest_prediction_calls),
        "finalized_prepass": bool(core.finalized_pre_ingest_predictions),
        "pre_ingest_scope_prediction": metadata.get("pre_ingest_scope_prediction"),
        "predicted_subset_equivalence_shadow": metadata.get("predicted_subset_equivalence_shadow"),
        "partial_hydration_equivalence_shadow": metadata.get("partial_hydration_equivalence_shadow"),
        "partial_residency_projection_safety": metadata.get("partial_residency_projection_safety"),
        "materialization_stage_timings_ms": metadata.get("materialization_stage_timings_ms"),
    }


async def _run_commit_safety_case(*, dual: bool) -> dict[str, Any]:
    base = _two_tree_incremental_snapshot_with_fact_and_route_manifest()
    calls: list[str] = []
    manifest_store = _manifest_store(base.route_index_manifest)
    core = FakeIngestCore(base)
    core.pre_ingest_prediction = _prediction(base, tree_ids=("entity:alice",))
    core.partial_hydration_equivalence_snapshot = replace(_snapshot(), baseline_forest_version=None)
    tree_store = RecordingStore(calls, "tree_nodes")
    location_store = RecordingStore(calls, "locations")
    recall_store = RecordingStore(calls, "recall")
    browse_store = RecordingStore(calls, "browse_embeddings")
    handler = _handler(
        manifest_store=manifest_store,
        calls=calls,
        core=core,
        baseline_provider=MatchingProjectionBaselineProvider(),
        config=_config(dual=dual),
        node_store=tree_store,
        location_store=location_store,
        recall_index=recall_store,
        browse_store=browse_store,
    )
    await handler.handle(_envelope(), {"fencing_token": 1})
    metadata = dict(manifest_store.latest.metadata)
    for volatile in (
        "materialization_fingerprint",
        "fingerprint_inputs_id",
        "pre_ingest_scope_prediction",
        "predicted_subset_equivalence_shadow",
        "partial_hydration_equivalence_shadow",
        "materialization_stage_timings_ms",
    ):
        metadata.pop(volatile, None)
    return {
        "stores": {
            "tree_nodes": _stable(tree_store.items),
            "locations": _stable(location_store.items),
            "recall": _stable(recall_store.items),
            "browse_embeddings": _stable(browse_store.items),
        },
        "metadata_without_shadow": _stable(metadata),
        "prepass_called": bool(core.pre_ingest_prediction_calls),
        "oracle_shadow_enabled": manifest_store.latest.metadata.get("partial_hydration_equivalence_shadow", {}).get("enabled"),
        "predicted_shadow_supported": manifest_store.latest.metadata.get("predicted_subset_equivalence_shadow", {}).get("supported"),
    }


async def build_artifact(*, mode: str = LOCAL_FAKE_STORE_MODE) -> dict[str, Any]:
    scoped_summary = _snapshot_with_scoped_summary_cache()
    base = replace(
        _two_tree_incremental_snapshot_with_fact_and_route_manifest(),
        summary_cache=dict(scoped_summary.summary_cache),
    )
    cases = [
        await _run_case(
            "clean_prediction_covers_oracle",
            snapshot=base,
            route_index_manifest=base.route_index_manifest,
            prediction=_prediction(base, tree_ids=("entity:alice",)),
            shadow_snapshot=replace(_snapshot_with_scoped_summary_cache(), baseline_forest_version=None),
            dual=True,
        ),
        await _run_case(
            "under_selection_blocks_switch",
            snapshot=base,
            route_index_manifest=base.route_index_manifest,
            prediction=_prediction(base, tree_ids=("entity:user",)),
            shadow_snapshot=replace(_snapshot(), baseline_forest_version=None),
            dual=True,
        ),
        await _run_case(
            "oracle_independent_escalation_blocks_partial",
            snapshot=base,
            route_index_manifest=base.route_index_manifest,
            prediction=_prediction(base, tree_ids=("entity:alice",), full=True, reasons=("cluster_merge_possible",)),
            shadow_snapshot=replace(base, baseline_forest_version=None),
            dual=True,
        ),
    ]
    commit_control = await _run_commit_safety_case(dual=False)
    commit_dual = await _run_commit_safety_case(dual=True)
    clean_prediction = _dict(cases[0].get("pre_ingest_scope_prediction"))
    negative_prediction = _dict(cases[1].get("pre_ingest_scope_prediction"))
    clean_shadow = _dict(cases[0].get("predicted_subset_equivalence_shadow"))
    clean_scoped = _dict(clean_shadow.get("scoped_comparison"))
    assessed_kinds = [
        kind
        for kind, details in clean_scoped.get("by_kind", {}).items()
        if isinstance(details, dict) and details.get("assessed") is True
    ]
    return {
        "schema_version": 1,
        "run_id": f"partial-hydration-shadow-evidence-{int(time.time())}",
        "mode": mode,
        "coverage": {
            "case_count": len(cases),
            "clean_case_actual_tree_count": clean_prediction.get("actual_tree_count") or 0,
            "clean_case_planned_tree_count": clean_prediction.get("planned_tree_load_count") or 0,
            "clean_case_assessed_kind_count": len(assessed_kinds),
            "clean_case_assessed_kinds": assessed_kinds,
            "negative_case_missing_tree_count": negative_prediction.get("missing_tree_count") or 0,
            "commit_safety_store_count": len(commit_dual["stores"]),
        },
        "gate": {
            "clean_case_no_under_selection": clean_prediction.get("under_selected_without_escalation") is False,
            "clean_case_covers_oracle": clean_prediction.get("covers_oracle_canonical_input") is True,
            "clean_case_predicted_shadow_supported": clean_shadow.get("supported") is True,
            "clean_case_scoped_equivalent": clean_scoped.get("equivalent") is True,
            "clean_case_no_unassessed_kinds": clean_scoped.get("unassessed_kinds") == [],
            "under_selection_detected": negative_prediction.get("under_selected_without_escalation") is True,
            "escalation_blocks_predicted_shadow": _get_path(cases[2], "predicted_subset_equivalence_shadow", "reason") == "predicted_scope_requires_full_hydration",
            "full_commit_path_used": all(_get_path(case, "partial_residency_projection_safety", "partial_resident") is False for case in cases),
            "dual_commit_store_outputs_equal": commit_control["stores"] == commit_dual["stores"],
            "dual_commit_metadata_without_shadow_equal": commit_control["metadata_without_shadow"] == commit_dual["metadata_without_shadow"],
            "dual_commit_oracle_shadow_disabled": commit_control["oracle_shadow_enabled"] is False and commit_dual["oracle_shadow_enabled"] is False,
            "dual_commit_predicted_shadow_supported": commit_dual["predicted_shadow_supported"] is True,
        },
        "cases": _stable(cases),
        "commit_safety": {
            "control": commit_control,
            "dual": commit_dual,
        },
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run bounded local partial-hydration shadow evidence.")
    parser.add_argument("--output-root", default=str(Path("artifacts") / "incremental-shadow-smoke"))
    parser.add_argument(
        "--mode",
        default=LOCAL_FAKE_STORE_MODE,
        help="Artifact mode label for local diagnostics. Trusted live modes are refused by this fake-store harness.",
    )
    parser.add_argument("--no-fail", action="store_true", help="Write the artifact even if gates fail, but exit 0.")
    args = parser.parse_args()
    mode = str(args.mode or "").strip().lower()
    if mode in TRUSTED_LIVE_MODES:
        parser.error("this fake-store harness cannot emit trusted live canary modes")
    artifact = await build_artifact(mode=mode or LOCAL_FAKE_STORE_MODE)
    out_dir = Path(args.output_root)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{artifact['run_id']}.json"
    out_path.write_text(json.dumps(artifact, indent=2, sort_keys=True), encoding="utf-8")
    failed = [name for name, passed in artifact["gate"].items() if passed is not True]
    print(json.dumps({"artifact": str(out_path), "gate": artifact["gate"], "failed_gates": failed}, indent=2))
    if failed and not args.no_fail:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())
