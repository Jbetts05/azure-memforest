from __future__ import annotations

from scripts.partial_hydration_canary_gate import evaluate_artifact
from scripts.partial_hydration_live_canary_artifact import build_live_artifact, summarize_live_metadata


def _local_evidence() -> dict:
    return {
        "coverage": {
            "case_count": 3,
            "clean_case_actual_tree_count": 1,
            "clean_case_planned_tree_count": 1,
            "clean_case_assessed_kind_count": 4,
            "negative_case_missing_tree_count": 1,
            "commit_safety_store_count": 4,
        },
        "gate": {
            "clean_case_no_under_selection": True,
            "clean_case_covers_oracle": True,
            "clean_case_predicted_shadow_supported": True,
            "clean_case_scoped_equivalent": True,
            "clean_case_no_unassessed_kinds": True,
            "under_selection_detected": True,
            "escalation_blocks_predicted_shadow": True,
            "full_commit_path_used": True,
            "dual_commit_store_outputs_equal": True,
            "dual_commit_metadata_without_shadow_equal": True,
            "dual_commit_oracle_shadow_disabled": True,
            "dual_commit_predicted_shadow_supported": True,
        },
    }


def _live_metadata(*, under_selected: bool = False) -> dict:
    return {
        "pre_ingest_scope_prediction": {
            "supported": True,
            "under_selected_without_escalation": under_selected,
        },
        "predicted_subset_equivalence_shadow": {
            "supported": True,
            "scoped_comparison": {
                "equivalent": True,
                "unassessed_kinds": [],
            },
        },
        "partial_residency_projection_safety": {
            "partial_resident": False,
        },
    }


def test_gate_rejects_mode_only_live_artifact_without_live_canary_metrics() -> None:
    artifact = {
        "mode": "live_canary",
        "provenance": {
            "producer": "azure-memforest-live-canary",
            "run_channel": "trusted-ci",
            "live_services_used": True,
            "fake_stores_used": False,
        },
        **_local_evidence(),
    }

    report = evaluate_artifact(artifact, require_live_canary=True)

    assert report["promotion_allowed"] is False
    assert "live_canary:metadata_record_count" in report["promotion_blockers"]


def test_live_canary_collector_artifact_passes_gate_for_clean_metadata() -> None:
    artifact = build_live_artifact(
        local_evidence=_local_evidence(),
        live_canary=summarize_live_metadata([_live_metadata(), _live_metadata(), _live_metadata()]),
        producer="azure-memforest-live-canary",
        run_channel="trusted-ci",
    )

    report = evaluate_artifact(artifact, require_live_canary=True)

    assert report["promotion_allowed"] is True
    assert report["live_canary_checks_satisfied"] is True


def test_live_canary_collector_blocks_under_selection() -> None:
    artifact = build_live_artifact(
        local_evidence=_local_evidence(),
        live_canary=summarize_live_metadata([_live_metadata(under_selected=True), _live_metadata(), _live_metadata()]),
        producer="azure-memforest-live-canary",
        run_channel="trusted-ci",
    )

    report = evaluate_artifact(artifact, require_live_canary=True)

    assert report["promotion_allowed"] is False
    assert "live_canary:under_selection_count" in report["promotion_blockers"]
    assert "live_canary:zero_under_selection" in report["promotion_blockers"]
    assert report["live_canary_satisfied"] is False


def test_live_canary_collector_blocks_unparsed_records() -> None:
    artifact = build_live_artifact(
        local_evidence=_local_evidence(),
        live_canary=summarize_live_metadata(
            [_live_metadata(), _live_metadata(), _live_metadata()],
            input_object_count=4,
            unparsed_record_count=1,
        ),
        producer="azure-memforest-live-canary",
        run_channel="trusted-ci",
    )

    report = evaluate_artifact(artifact, require_live_canary=True)

    assert report["promotion_allowed"] is False
    assert "live_canary:unparsed_record_count" in report["promotion_blockers"]
    assert "live_canary:no_unparsed_records" in report["promotion_blockers"]


def test_gate_handles_malformed_artifact_shapes_as_blockers() -> None:
    report = evaluate_artifact(
        {"mode": "live_canary", "gate": "spoofed", "coverage": "spoofed", "provenance": "spoofed"},
        require_live_canary=True,
    )

    assert report["promotion_allowed"] is False
    assert "coverage:case_count" in report["promotion_blockers"]
    assert "live_provenance:producer" in report["promotion_blockers"]
