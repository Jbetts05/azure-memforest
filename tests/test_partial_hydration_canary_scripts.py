from __future__ import annotations

from scripts.durable_ingest_smoke import _manifest_metadata_summary
from scripts.partial_hydration_authority_preflight import evaluate_authority_preflight
from scripts.partial_hydration_canary_gate import evaluate_artifact
from scripts.partial_hydration_live_canary_artifact import build_live_artifact, collect_metadata_records, summarize_live_metadata


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


def _confirmations() -> dict[str, bool]:
    return {
        "rollback_switch_confirmed": True,
        "guarded_switch_config_confirmed": True,
        "tree_scoped_projection_writes_confirmed": True,
        "live_canary_reviewed": True,
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
        "materialization_model_telemetry": {
            "schema_version": 1,
            "total_calls": 2,
            "total_failures": 1,
            "total_started_calls": 3,
            "total_latency_ms": 5.5,
        },
        "materialization_stage_timings_ms": {
            "hydrate": 1.0,
            "extract_build": 2.0,
            "project": 3.0,
            "write_projection": 4.0,
            "total_before_commit": 10.0,
        },
    }


def _clean_live_artifact(*, code_version: str = "abc123") -> dict:
    return build_live_artifact(
        local_evidence=_local_evidence(),
        live_canary=summarize_live_metadata([_live_metadata(), _live_metadata(), _live_metadata()]),
        producer="azure-memforest-live-canary",
        run_channel="trusted-ci",
        code_version=code_version,
    )


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
    artifact = _clean_live_artifact()

    report = evaluate_artifact(artifact, require_live_canary=True)

    assert report["promotion_allowed"] is True
    assert report["live_canary_checks_satisfied"] is True
    assert artifact["live_canary"]["model_call_totals"] == {
        "total_calls": 6,
        "total_failures": 3,
        "total_started_calls": 9,
        "total_latency_ms": 16.5,
    }
    assert artifact["live_canary"]["stage_timing_totals_ms"]["extract_build"] == 6.0


def test_authority_preflight_requires_clean_canary_and_confirmations() -> None:
    artifact = _clean_live_artifact()

    blocked = evaluate_authority_preflight(artifact, confirmations={})

    assert blocked["canary_promotion_allowed"] is True
    assert blocked["authority_allowed"] is False
    assert "confirmation:rollback_switch_confirmed" in blocked["authority_blockers"]

    allowed = evaluate_authority_preflight(
        artifact,
        confirmations={
            **_confirmations(),
        },
        expected_code_version="abc123",
        max_age_seconds=3600,
    )

    assert allowed["authority_allowed"] is True
    assert allowed["authority_blockers"] == []


def test_authority_preflight_rejects_unknown_confirmations() -> None:
    artifact = _clean_live_artifact()

    report = evaluate_authority_preflight(
        artifact,
        confirmations={
            **_confirmations(),
            "rollbak_switch_confirmed": True,
        },
        expected_code_version="abc123",
        max_age_seconds=3600,
    )

    assert report["authority_allowed"] is False
    assert report["unknown_confirmations"] == ["rollbak_switch_confirmed"]
    assert "unknown_confirmation:rollbak_switch_confirmed" in report["authority_blockers"]


def test_authority_preflight_blocks_local_fake_artifact() -> None:
    artifact = {
        "run_id": "local-only",
        "mode": "deterministic_local_fake_stores_no_live_services",
        "provenance": {
            "producer": "partial_hydration_shadow_evidence.py",
            "run_channel": "local_fake_store_harness",
            "live_services_used": False,
            "fake_stores_used": True,
        },
        **_local_evidence(),
    }

    report = evaluate_authority_preflight(
        artifact,
        confirmations={
            **_confirmations(),
        },
        expected_code_version="abc123",
        max_age_seconds=3600,
    )

    assert report["authority_allowed"] is False
    assert "canary:live_canary_required" in report["authority_blockers"]
    assert "canary:live_provenance:live_services_used" in report["authority_blockers"]


def test_authority_preflight_blocks_failed_canary_even_with_confirmations() -> None:
    artifact = build_live_artifact(
        local_evidence=_local_evidence(),
        live_canary=summarize_live_metadata([_live_metadata(under_selected=True), _live_metadata(), _live_metadata()]),
        producer="azure-memforest-live-canary",
        run_channel="trusted-ci",
        code_version="abc123",
    )

    report = evaluate_authority_preflight(
        artifact,
        confirmations={
            **_confirmations(),
        },
        expected_code_version="abc123",
        max_age_seconds=3600,
    )

    assert report["authority_allowed"] is False
    assert "canary:live_canary:under_selection_count" in report["authority_blockers"]


def test_authority_preflight_blocks_code_version_mismatch_and_stale_artifact() -> None:
    artifact = _clean_live_artifact(code_version="old-sha")
    artifact["provenance"]["created_at_unix"] = 1.0

    report = evaluate_authority_preflight(
        artifact,
        confirmations=_confirmations(),
        expected_code_version="new-sha",
        max_age_seconds=3600,
    )

    assert report["authority_allowed"] is False
    assert "code_version:code_version_mismatch" in report["authority_blockers"]
    assert "freshness:artifact_too_old" in report["authority_blockers"]


def test_authority_preflight_blocks_empty_expected_version_and_future_artifact() -> None:
    artifact = _clean_live_artifact()
    artifact["provenance"]["created_at_unix"] = 9_999_999_999.0

    report = evaluate_authority_preflight(
        artifact,
        confirmations=_confirmations(),
        expected_code_version="",
        max_age_seconds=3600,
    )

    assert report["authority_allowed"] is False
    assert "code_version:expected_code_version_missing" in report["authority_blockers"]
    assert "freshness:created_at_unix_in_future" in report["authority_blockers"]


def test_live_canary_collector_blocks_missing_timing_and_telemetry() -> None:
    bad_metadata = _live_metadata()
    bad_metadata.pop("materialization_model_telemetry")
    bad_metadata["materialization_stage_timings_ms"] = {"hydrate": 1.0}
    artifact = build_live_artifact(
        local_evidence=_local_evidence(),
        live_canary=summarize_live_metadata([bad_metadata, _live_metadata(), _live_metadata()]),
        producer="azure-memforest-live-canary",
        run_channel="trusted-ci",
        code_version="abc123",
    )

    report = evaluate_artifact(artifact, require_live_canary=True)

    assert report["promotion_allowed"] is False
    assert "live_canary:missing_model_telemetry_count" in report["promotion_blockers"]
    assert "live_canary:missing_stage_timing_count" in report["promotion_blockers"]
    assert "live_canary:all_model_telemetry_present" in report["promotion_blockers"]
    assert "live_canary:all_stage_timings_present" in report["promotion_blockers"]


def test_live_canary_collector_blocks_non_numeric_timing_and_telemetry() -> None:
    bad_metadata = _live_metadata()
    bad_metadata["materialization_model_telemetry"]["total_calls"] = "not-a-number"
    bad_metadata["materialization_stage_timings_ms"]["project"] = None
    artifact = build_live_artifact(
        local_evidence=_local_evidence(),
        live_canary=summarize_live_metadata([bad_metadata, _live_metadata(), _live_metadata()]),
        producer="azure-memforest-live-canary",
        run_channel="trusted-ci",
        code_version="abc123",
    )

    report = evaluate_artifact(artifact, require_live_canary=True)

    assert report["promotion_allowed"] is False
    assert "live_canary:missing_model_telemetry_count" in report["promotion_blockers"]
    assert "live_canary:missing_stage_timing_count" in report["promotion_blockers"]


def test_live_canary_collector_blocks_under_selection() -> None:
    artifact = build_live_artifact(
        local_evidence=_local_evidence(),
        live_canary=summarize_live_metadata([_live_metadata(under_selected=True), _live_metadata(), _live_metadata()]),
        producer="azure-memforest-live-canary",
        run_channel="trusted-ci",
        code_version="abc123",
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
        code_version="abc123",
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


def test_collector_parses_durable_smoke_manifest_metadata_wrapper(tmp_path) -> None:
    artifact_path = tmp_path / "durable_smoke.json"
    artifact_path.write_text(
        """
        {
          "manifest_metadata": {
            "records": [
              {"metadata": {"pre_ingest_scope_prediction": {"supported": true}}},
              {"unexpected": true}
            ]
          }
        }
        """,
        encoding="utf-8",
    )

    collected = collect_metadata_records([artifact_path])

    assert collected["input_object_count"] == 2
    assert len(collected["records"]) == 1
    assert collected["unparsed_record_count"] == 1


def test_durable_smoke_manifest_metadata_summary_counts_prediction_support() -> None:
    summary = _manifest_metadata_summary(
        {
            "records": [
                {"metadata": {"pre_ingest_scope_prediction": {"supported": True}}},
                {"metadata": {"pre_ingest_scope_prediction": {"supported": False, "reason": "no_committed_manifest"}}},
                {"metadata": {}},
            ]
        }
    )

    assert summary == {
        "record_count": 3,
        "pre_ingest_prediction_supported_count": 1,
        "pre_ingest_prediction_unsupported_count": 1,
        "pre_ingest_prediction_missing_count": 1,
    }
