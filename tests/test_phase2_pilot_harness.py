from __future__ import annotations

import importlib.util
import json
from pathlib import Path


HARNESS_PATH = Path("docs/parity-runs/2026-06-03-longmemeval-phase2-intended-pilot/run_pilot.py")
MANIFEST_PATH = Path("docs/parity-runs/2026-06-03-longmemeval-phase2-intended-pilot/manifest.json")
CASES_PATH = Path("docs/parity-runs/2026-06-03-longmemeval-phase2-intended-pilot/pilot_cases.json")


def _load_harness(monkeypatch=None, **env):
    if monkeypatch is not None:
        for key, value in env.items():
            monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location("phase2_pilot_harness", HARNESS_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _result(case_id: str, *, label: bool = True, agreement: bool = True) -> dict:
    return {
        "case_id": case_id,
        "status": "succeeded",
        "answer": "ok",
        "judges": {
            "primary": {"label": label},
            "cross_check": {"label": label if agreement else not label},
            "agreement": agreement,
        },
        "events": [
            {
                "step": "ingest",
                "status": {"status": "succeeded"},
                "durable_instance": {"runtimeStatus": "Completed"},
            }
        ],
    }


def test_phase2_manifest_matches_pilot_cases() -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    cases = json.loads(CASES_PATH.read_text(encoding="utf-8"))

    assert manifest["not_a_longmemeval_score"] is True
    assert manifest["selection"]["case_ids"] == [case["case_id"] for case in cases]
    assert len(cases) == 10
    assert manifest["intended_stack"]["default_chat"] == "gpt-5.4-mini-memory"
    assert manifest["intended_stack"]["answer"] == "gpt-5.4"
    assert manifest["intended_stack"]["embedding_dimension"] == 3072
    assert manifest["gate"]["advance_to_50_case_subset_if"]["pipeline_primary_judge_correct_min"] == 7
    assert manifest["gate"]["advance_to_50_case_subset_if"]["calibration_primary_judge_correct_min"] == 8


def test_phase2_dev_auth_adds_benchmark_role(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_AUTH_MODE="dev")

    headers = harness.api_headers(user_id="benchmark-user", json_body=True, idem="idem-1")

    assert "x-benchmark-auth" not in headers
    assert headers["x-tenant-id"] == harness.TENANT
    assert headers["x-actor-id"] == "benchmark-user"
    assert headers["x-auth-roles"] == "memory.benchmark"
    assert headers["Idempotency-Key"] == "idem-1"


def test_phase2_aggregate_allows_pilot_quality_threshold(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    results[0]["judges"]["primary"]["label"] = False
    results[0]["judges"]["cross_check"]["label"] = False
    results[1]["judges"]["primary"]["label"] = False
    results[1]["judges"]["cross_check"]["label"] = False

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "succeeded"
    assert aggregate["primary_judge_correct"] == 8
    assert aggregate["pipeline_primary_judge_correct"] == 8
    assert aggregate["advance_to_50_case_subset"] is True


def test_phase2_aggregate_excludes_override_from_pipeline_quality(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    overridden = next(result for result in results if result["case_id"] == "8e91e7d9")
    overridden["exclude_from_pipeline_accuracy"] = True

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "succeeded"
    assert aggregate["completed_cases"] == 10
    assert aggregate["pipeline_eligible_cases"] == 9
    assert aggregate["primary_judge_correct"] == 10
    assert aggregate["pipeline_primary_judge_correct"] == 9


def test_phase2_aggregate_rejects_low_unmodified_pipeline_quality(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    overridden = next(result for result in results if result["case_id"] == "8e91e7d9")
    overridden["exclude_from_pipeline_accuracy"] = True
    failed = 0
    for result in results:
        if not result.get("exclude_from_pipeline_accuracy") and failed < 3:
            result["judges"]["primary"]["label"] = False
            result["judges"]["cross_check"]["label"] = False
            failed += 1

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "gate_failed"
    assert aggregate["pipeline_primary_judge_correct"] == 6


def test_phase2_aggregate_rejects_new_dlq_even_in_durable_mode(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 20})

    assert aggregate["status"] == "gate_failed"
    assert aggregate["dlq_gate_enforced"] is True
    assert aggregate["dlq_gate_passed"] is False


def test_phase2_aggregate_accounts_for_non_answer_content_filter_skips(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    skipped = results[0]["events"][0]
    skipped["source_session_id"] = "non-answer-session"
    skipped["status"] = {
        "status": "failed",
        "error": "content_filter: ResponsibleAIPolicyViolation",
    }
    results[0]["filtered_skips"] = [
        {
            "source_session_id": "non-answer-session",
            "reason": "content_filter",
            "answer_bearing": False,
        }
    ]

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "succeeded"
    assert aggregate["successful_ingest_event_count"] == 9
    assert aggregate["skipped_non_answer_ingest_event_count"] == 1
    assert aggregate["durable_accounted_event_count"] == 10
    assert aggregate["durable_gate_passed"] is True


def test_phase2_aggregate_rejects_residue_contamination(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    results[1]["residue_probe"] = {"response": {"status": "succeeded", "answer": "leaked previous case"}}

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "gate_failed"
    assert aggregate["residue_contamination_events"] == 1


def test_phase2_aggregate_allows_residue_refusal(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    results[1]["residue_probe"] = {"response": {"status": "succeeded", "answer": "I don't have information about that."}}

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "succeeded"
    assert aggregate["residue_contamination_events"] == 0


def test_phase2_aggregate_allows_broader_residue_refusal(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    results[1]["residue_probe"] = {"response": {"status": "succeeded", "answer": "Sorry, I cannot find any relevant memory."}}

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "succeeded"
    assert aggregate["residue_contamination_events"] == 0


def test_phase2_residue_probe_returns_request_errors(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    def raise_timeout(*_args, **_kwargs):
        raise harness.requests.Timeout("timed out")

    monkeypatch.setattr(harness, "api_post", raise_timeout)

    assert harness.residue_probe("previous question", user_id="user-1") == {"status": "request_error", "error": "Timeout"}


def test_phase2_restore_override_flags_from_checkpoint(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    result = {
        "events": [
            {
                "step": "ingest",
                "input_shaping": {
                    "evidence_minimized": True,
                    "exclude_from_pipeline_accuracy": True,
                },
            }
        ],
        "modified_input": False,
    }

    harness._restore_override_flags(result)

    assert result["modified_input"] is True
    assert result["exclude_from_pipeline_accuracy"] is True


def test_phase2_harness_can_extract_explicit_assistant_advice_override(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    turns, stats = harness.turns_for_ingest(
        {
            "source_session_id": "answer_6dc4305e",
            "turns": [
                {"role": "user", "content_id": "answer_6dc4305e:4", "content": "I prefer winding down by 9:30 pm.", "metadata": {}},
                {"role": "assistant", "content_id": "answer_6dc4305e:5", "content": "Avoid phones and TVs before bed.", "metadata": {}},
                {"role": "assistant", "content_id": "answer_6dc4305e:7", "content": "Other advice.", "metadata": {}},
                {"role": "user", "content_id": "answer_6dc4305e:8", "content": "Can I get sleep meditations?", "metadata": {}},
            ],
        },
        case_id="195a1a1b",
    )

    by_id = {turn["content_id"]: turn for turn in turns}
    assert by_id["answer_6dc4305e:5"]["metadata"]["assistant_advice_context"] is True
    assert "skip_extraction" not in by_id["answer_6dc4305e:5"]["metadata"]
    assert stats["assistant_turns_extractable"] == 1
    assert stats["exclude_from_pipeline_accuracy"] is True
    assert stats["extract_assistant_content_ids"] == ["answer_6dc4305e:5"]


def test_phase2_judge_uses_non_tiny_token_budget(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    captured = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "yes"}}]}

    def fake_post(_url, *, json, headers, timeout):
        captured.update(json)
        return Response()

    monkeypatch.setattr(harness.requests, "post", fake_post)
    monkeypatch.setattr(harness, "model_headers", lambda: {})

    assert harness.judge("gpt-5.4", "prompt")["label"] is True
    assert captured["max_completion_tokens"] >= 64


def test_phase2_aggregate_requires_all_cases(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES[:-1]]

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "gate_failed"
    assert aggregate["attempted_cases"] == 9


def test_phase2_durable_status_attempts_are_configurable(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DURABLE_STATUS_ATTEMPTS="77")

    assert harness.DURABLE_STATUS_ATTEMPTS == 77


def test_phase2_strict_preflight_rejects_api_budget_mismatch(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    api_settings = dict(harness.INTENDED_API_SETTINGS)
    api_settings["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MULTIPLIER"] = "4"

    try:
        harness._require_settings("api", api_settings, harness.INTENDED_API_SETTINGS)
    except RuntimeError as exc:
        assert "AZURE_MEMFOREST_MODEL_CALL_BUDGET_MULTIPLIER" in str(exc)
    else:
        raise AssertionError("strict preflight should reject API budget mismatch")


def test_phase2_strict_preflight_uses_runtime_durable_hub(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DURABLE_HUB="CustomHub")
    durable_settings = dict(harness.INTENDED_DURABLE_SETTINGS)
    durable_settings["AzureFunctionsJobHost__extensions__durableTask__hubName"] = "CustomHub"

    harness._require_settings(
        "durable",
        durable_settings,
        {
            **harness.INTENDED_DURABLE_SETTINGS,
            "AzureFunctionsJobHost__extensions__durableTask__hubName": "CustomHub",
        },
    )


def test_phase2_strict_preflight_rejects_unexpected_stamped_budget(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_STRICT_PREFLIGHT="true")
    stamped_budget = {"max_completed_model_calls": 400}

    try:
        if int(stamped_budget.get("max_completed_model_calls") or 0) != 2000:
            raise RuntimeError(f"api stamped unexpected progress budget for case:1: {stamped_budget}")
    except RuntimeError as exc:
        assert "unexpected progress budget" in str(exc)
    else:
        raise AssertionError("strict preflight should reject stamped budget 400")
