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

from tests.test_worker_materialization import (
    FactRouteHint,
    FakeIngestCore,
    FakeManifestStore,
    ForestManifest,
    MatchingProjectionBaselineProvider,
    MaterializationHandlerConfig,
    PreIngestTreeScope,
    RouteRiskFlags,
    _envelope,
    _handler,
    _snapshot,
    _snapshot_with_scoped_summary_cache,
    _two_tree_incremental_snapshot_with_fact_and_route_manifest,
)


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


async def build_artifact() -> dict[str, Any]:
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
    return {
        "schema_version": 1,
        "run_id": f"partial-hydration-shadow-evidence-{int(time.time())}",
        "mode": "deterministic_local_fake_stores_no_live_services",
        "gate": {
            "clean_case_no_under_selection": not cases[0]["pre_ingest_scope_prediction"]["under_selected_without_escalation"],
            "clean_case_covers_oracle": cases[0]["pre_ingest_scope_prediction"]["covers_oracle_canonical_input"],
            "clean_case_predicted_shadow_supported": cases[0]["predicted_subset_equivalence_shadow"]["supported"],
            "clean_case_scoped_equivalent": cases[0]["predicted_subset_equivalence_shadow"]["scoped_comparison"]["equivalent"],
            "clean_case_no_unassessed_kinds": cases[0]["predicted_subset_equivalence_shadow"]["scoped_comparison"]["unassessed_kinds"] == [],
            "under_selection_detected": cases[1]["pre_ingest_scope_prediction"]["under_selected_without_escalation"],
            "escalation_blocks_predicted_shadow": cases[2]["predicted_subset_equivalence_shadow"]["reason"] == "predicted_scope_requires_full_hydration",
            "full_commit_path_used": all(case["partial_residency_projection_safety"]["partial_resident"] is False for case in cases),
        },
        "cases": _stable(cases),
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run bounded local partial-hydration shadow evidence.")
    parser.add_argument("--output-root", default=str(Path("artifacts") / "incremental-shadow-smoke"))
    parser.add_argument("--no-fail", action="store_true", help="Write the artifact even if gates fail, but exit 0.")
    args = parser.parse_args()
    artifact = await build_artifact()
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
