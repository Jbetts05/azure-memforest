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
                "index": 1,
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
    assert manifest["gate"]["advance_to_50_case_subset_if"]["pipeline_eligible_cases_min"] == 8
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


def test_phase2_api_post_retries_idempotent_timeout(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_AUTH_MODE="dev", PHASE2_API_IDEMPOTENT_POST_ATTEMPTS="2")
    calls = {"count": 0}

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"job_id": "job-1"}

    def post(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise harness.requests.exceptions.ReadTimeout("timeout")
        return Response()

    monkeypatch.setattr(harness.requests, "post", post)
    monkeypatch.setattr(harness.time, "sleep", lambda seconds: None)

    assert harness.api_post("/path", {"ok": True}, user_id="user-1", idem="idem-1") == {"job_id": "job-1"}
    assert calls["count"] == 2


def test_phase2_api_post_does_not_retry_non_idempotent_timeout(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_AUTH_MODE="dev", PHASE2_API_IDEMPOTENT_POST_ATTEMPTS="2")
    calls = {"count": 0}

    def post(*args, **kwargs):
        calls["count"] += 1
        raise harness.requests.exceptions.ReadTimeout("timeout")

    monkeypatch.setattr(harness.requests, "post", post)

    try:
        harness.api_post("/path", {"ok": True}, user_id="user-1")
    except harness.requests.exceptions.ReadTimeout:
        assert calls["count"] == 1
    else:
        raise AssertionError("non-idempotent POST timeout should not be retried")


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


def test_phase2_aggregate_allows_two_documented_calibration_only_cases(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    for case_id in ("8e91e7d9", "195a1a1b"):
        next(result for result in results if result["case_id"] == case_id)["exclude_from_pipeline_accuracy"] = True

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "succeeded"
    assert aggregate["pipeline_eligible_cases"] == 8
    assert aggregate["pipeline_primary_judge_correct"] == 8


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


def test_phase2_aggregate_counts_latest_ingest_attempt_only(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    results[0]["events"] = [
        {
            "step": "ingest",
            "index": 1,
            "status": {"status": "failed", "error": "worker oom"},
            "durable_instance": {"runtimeStatus": "Completed"},
        },
        {
            "step": "ingest",
            "index": 1,
            "status": {"status": "succeeded"},
            "durable_instance": {"runtimeStatus": "Completed"},
        },
    ]

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["successful_ingest_event_count"] == 10
    assert aggregate["durable_instance_count"] == 10
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


def test_phase2_aggregate_allows_residue_refusal_with_retrieved_facts(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    results[1]["residue_probe"] = {
        "response": {
            "status": "succeeded",
            "answer": "I don't know",
            "facts": [{"text": "The current user likes minimalist necklaces."}],
        }
    }

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "succeeded"
    assert aggregate["residue_contamination_events"] == 0


def test_phase2_aggregate_allows_skipped_resume_residue_probe(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES]
    results[1]["residue_probe"] = {
        "question": "previous question",
        "skipped": "checkpoint_resume_user_may_already_be_populated",
    }

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "succeeded"
    assert aggregate["residue_contamination_events"] == 0


def test_phase2_residue_probe_returns_request_errors(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    def raise_timeout(*_args, **_kwargs):
        raise harness.requests.Timeout("timed out")

    monkeypatch.setattr(harness, "api_post", raise_timeout)

    assert harness.residue_probe("previous question", user_id="user-1") == {"status": "request_error", "error": "Timeout"}


def test_phase2_reconciles_durable_completed_timeout(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    refreshed_status = {"job_id": "job-1", "status": "succeeded", "stage": "commit"}
    captured = {}

    def fake_api_get(path, *, user_id):
        captured["path"] = path
        captured["user_id"] = user_id
        return refreshed_status

    monkeypatch.setattr(harness, "api_get", fake_api_get)

    status, wait_info = harness.reconcile_durable_completed_job(
        "job-1",
        user_id="user-1",
        status={"job_id": "job-1", "status": "running", "stage": "write_projection"},
        wait_info={"timeout_reason": "idle_timeout", "progress_events": []},
        durable_status={"runtimeStatus": "Completed"},
    )

    assert status is refreshed_status
    assert wait_info["timeout_reason"] is None
    assert wait_info["original_timeout_reason"] == "idle_timeout"
    assert wait_info["durable_completed_reconciled"] is True
    assert captured == {"path": "/v1/users/user-1/jobs/job-1", "user_id": "user-1"}


def test_phase2_does_not_reconcile_noncompleted_durable_timeout(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    def fail_api_get(*_args, **_kwargs):
        raise AssertionError("API should not be refreshed unless Durable completed")

    monkeypatch.setattr(harness, "api_get", fail_api_get)
    original_status = {"job_id": "job-1", "status": "running", "stage": "write_projection"}
    original_wait = {"timeout_reason": "idle_timeout", "progress_events": []}

    status, wait_info = harness.reconcile_durable_completed_job(
        "job-1",
        user_id="user-1",
        status=original_status,
        wait_info=original_wait,
        durable_status={"runtimeStatus": "Running"},
    )

    assert status is original_status
    assert wait_info is original_wait


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


def test_phase2_harness_can_extract_all_assistant_turns_for_experiment(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_EXTRACT_ALL_ASSISTANT_TURNS="true")

    turns, stats = harness.turns_for_ingest(
        {
            "source_session_id": "session-1",
            "turns": [
                {"role": "user", "content_id": "session-1:1", "content": "What backend language should I learn?", "metadata": {}},
                {"role": "assistant", "content_id": "session-1:2", "content": "Learn Python or Ruby.", "metadata": {}},
                {"role": "assistant", "content_id": "session-1:3", "content": "You can build APIs with them.", "metadata": {}},
            ],
        },
        case_id="case-1",
    )

    assistant_turns = [turn for turn in turns if turn["role"] == "assistant"]
    assert all("skip_extraction" not in turn["metadata"] for turn in assistant_turns)
    assert all("assistant_advice_context" not in turn["metadata"] for turn in assistant_turns)
    assert all(turn["metadata"]["assistant_extraction_context"] is True for turn in assistant_turns)
    assert {turn["metadata"]["override_reason"] for turn in assistant_turns} == {"extract_all_assistant_turns"}
    assert stats["assistant_turns_extractable"] == 2
    assert stats["assistant_turns_marked_skip_extraction"] == 0
    assert stats["extract_all_assistant_turns"] is True


def test_phase2_harness_selectively_extracts_concrete_assistant_answers(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_SELECTIVE_ASSISTANT_EXTRACTION="true")

    turns, stats = harness.turns_for_ingest(
        {
            "source_session_id": "session-1",
            "turns": [
                {"role": "user", "content_id": "session-1:1", "content": "Which restaurant should I remember near the Vatican?", "metadata": {}},
                {"role": "assistant", "content_id": "session-1:2", "content": "Roscioli.", "metadata": {}},
                {"role": "user", "content_id": "session-1:3", "content": "Can you list the refining processes?", "metadata": {}},
                {"role": "assistant", "content_id": "session-1:4", "content": "Atmospheric distillation, fluid catalytic cracking, alkylation, and hydrotreating.", "metadata": {}},
                {"role": "assistant", "content_id": "session-1:5", "content": "Happy to help, let me know if you need anything else.", "metadata": {}},
            ],
        },
        case_id="case-1",
    )

    by_id = {turn["content_id"]: turn for turn in turns}
    assert by_id["session-1:2"]["metadata"]["assistant_extraction_context"] is True
    assert by_id["session-1:2"]["metadata"]["override_reason"] == "selective_assistant_extraction"
    assert by_id["session-1:4"]["metadata"]["assistant_extraction_context"] is True
    assert by_id["session-1:5"]["metadata"]["skip_extraction"] is True
    assert stats["assistant_turns_extractable"] == 2
    assert stats["assistant_turns_selectively_extractable"] == 2
    assert stats["assistant_turns_marked_skip_extraction"] == 1
    assert stats["selective_assistant_extraction"] is True


def test_phase2_harness_selective_extraction_still_skips_generic_advice(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_SELECTIVE_ASSISTANT_EXTRACTION="true")

    turns, stats = harness.turns_for_ingest(
        {
            "source_session_id": "session-1",
            "turns": [
                {"role": "user", "content_id": "session-1:1", "content": "How can I sleep better?", "metadata": {}},
                {"role": "assistant", "content_id": "session-1:2", "content": "You should keep a consistent bedtime and avoid stressful activities.", "metadata": {}},
            ],
        },
        case_id="case-1",
    )

    assert turns[1]["metadata"]["skip_extraction"] is True
    assert stats["assistant_turns_extractable"] == 0
    assert stats["assistant_turns_selectively_extractable"] == 0


def test_phase2_harness_selective_extraction_does_not_extract_comma_filler(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_SELECTIVE_ASSISTANT_EXTRACTION="true")

    turns, stats = harness.turns_for_ingest(
        {
            "source_session_id": "session-1",
            "turns": [
                {"role": "user", "content_id": "session-1:1", "content": "What should I do?", "metadata": {}},
                {"role": "assistant", "content_id": "session-1:2", "content": "Yes, I think that, given your schedule, you should probably rest more.", "metadata": {}},
                {"role": "assistant", "content_id": "session-1:3", "content": "You might enjoy hiking, reading, and cooking on the weekend.", "metadata": {}},
            ],
        },
        case_id="case-1",
    )

    assert turns[1]["metadata"]["skip_extraction"] is True
    assert turns[2]["metadata"]["skip_extraction"] is True
    assert stats["assistant_turns_extractable"] == 0


def test_phase2_harness_selective_extraction_keeps_inline_named_recommendation(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_SELECTIVE_ASSISTANT_EXTRACTION="true")

    turns, stats = harness.turns_for_ingest(
        {
            "source_session_id": "session-1",
            "turns": [
                {"role": "user", "content_id": "session-1:1", "content": "Which city did you recommend for the trip?", "metadata": {}},
                {"role": "assistant", "content_id": "session-1:2", "content": "I would recommend Paris for the trip.", "metadata": {}},
            ],
        },
        case_id="case-1",
    )

    assert turns[1]["metadata"]["assistant_extraction_context"] is True
    assert turns[1]["metadata"]["override_reason"] == "selective_assistant_extraction"
    assert stats["assistant_turns_selectively_extractable"] == 1


def test_phase2_harness_selective_extraction_keeps_concrete_process_list(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_SELECTIVE_ASSISTANT_EXTRACTION="true")

    turns, stats = harness.turns_for_ingest(
        {
            "source_session_id": "session-1",
            "turns": [
                {"role": "user", "content_id": "session-1:1", "content": "What refining processes does CITGO use?", "metadata": {}},
                {
                    "role": "assistant",
                    "content_id": "session-1:2",
                    "content": (
                        "CITGO operates refineries in Lake Charles, Lemont, and Corpus Christi.\n"
                        "* Atmospheric distillation: first separation step.\n"
                        "* Fluid catalytic cracking (FCC): breaks down heavier fractions.\n"
                        "* Alkylation: creates high-octane gasoline components.\n"
                        "* Hydrotreating: removes impurities."
                    ),
                    "metadata": {},
                },
            ],
        },
        case_id="case-1",
    )

    assert turns[1]["metadata"]["assistant_extraction_context"] is True
    assert turns[1]["metadata"]["override_reason"] == "selective_assistant_extraction"
    assert stats["assistant_turns_selectively_extractable"] == 1


def test_phase2_turns_for_ingest_backfills_turn_timestamp_from_session_date(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    turns, _stats = harness.turns_for_ingest(
        {
            "source_session_id": "session-1",
            "metadata": {"date": "2023/04/25 (Tue) 10:21"},
            "turns": [
                {"role": "user", "content_id": "turn-1", "content": "I have 1250 followers.", "metadata": {}},
                {"role": "user", "content_id": "turn-2", "content": "I have 1300 now.", "timestamp": "2023/05/01 (Mon) 22:56"},
            ],
        },
        case_id="case-1",
    )

    assert turns[0]["timestamp"] == "2023/04/25 (Tue) 10:21"
    assert turns[1]["timestamp"] == "2023/05/01 (Mon) 22:56"


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


def test_phase2_query_body_includes_question_date_as_query_time(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    body = harness.query_body_for_case(
        {
            "question": "How many days ago?",
            "question_date": "2023/03/01 (Wed) 10:00",
        }
    )

    assert body["question"] == "How many days ago?"
    assert body["query_time"] == harness.parse_timestamp_to_unix("2023/03/01 (Wed) 10:00")


def test_phase2_query_body_omits_missing_question_date(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    body = harness.query_body_for_case({"question": "What happened?"})

    assert "query_time" not in body


def test_phase2_new_case_result_records_question_date(monkeypatch, tmp_path) -> None:
    harness = _load_harness(monkeypatch)
    monkeypatch.setattr(harness, "RESULTS", tmp_path)
    monkeypatch.setattr(harness, "save", lambda path, value: None)
    monkeypatch.setattr(harness, "residue_probe", lambda question, *, user_id: {})
    monkeypatch.setattr(harness, "api_post", lambda *args, **kwargs: {"job_id": "job-1"})
    monkeypatch.setattr(
        harness,
        "wait_job_progress",
        lambda *args, **kwargs: ({"status": "failed"}, 0.0, {"timeout_reason": None}),
    )

    result = harness.run_case(
        {
            "case_id": "case-1",
            "question_type": "temporal-reasoning",
            "question": "How many days ago?",
            "question_date": "2023/03/01 (Wed) 10:00",
            "gold_answer": "1 day",
            "selected_session_count": 0,
            "selected_turn_count": 0,
            "sessions": [],
        },
        1,
        None,
    )

    assert result["question_date"] == "2023/03/01 (Wed) 10:00"


def test_phase2_aggregate_requires_all_cases(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DISPATCH_MODE="durable")
    results = [_result(case["case_id"]) for case in harness.CASES[:-1]]

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "gate_failed"
    assert aggregate["attempted_cases"] == 9


def test_phase2_durable_status_attempts_are_configurable(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_DURABLE_STATUS_ATTEMPTS="77")

    assert harness.DURABLE_STATUS_ATTEMPTS == 77


def test_phase2_strict_preflight_defaults_on(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    assert harness.STRICT_PREFLIGHT is True


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


def test_phase2_strict_preflight_rejects_split_revision_budget_mismatch(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    first_revision = [{"name": key, "value": value} for key, value in harness.INTENDED_API_SETTINGS.items()]
    second_settings = dict(harness.INTENDED_API_SETTINGS)
    second_settings["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MULTIPLIER"] = "4"
    second_revision = [{"name": key, "value": value} for key, value in second_settings.items()]

    monkeypatch.setattr(
        harness.subprocess,
        "check_output",
        lambda *args, **kwargs: json.dumps([first_revision, second_revision]),
    )

    try:
        harness.api_containerapp_settings()
    except RuntimeError as exc:
        assert "active traffic revisions disagree" in str(exc)
    else:
        raise AssertionError("strict preflight should reject split active revision mismatches")


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


def test_phase2_expected_stamped_budget_comes_from_intended_settings(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    assert harness._expected_stamped_budget() == int(
        harness.INTENDED_API_SETTINGS["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN"]
    )


def test_phase2_expected_stamped_budget_is_configurable(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_MODEL_CALL_BUDGET_FIXED="3500")

    assert harness.INTENDED_API_SETTINGS["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN"] == "3500"
    assert harness.INTENDED_API_SETTINGS["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX"] == "3500"
    assert harness.INTENDED_DURABLE_SETTINGS["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MIN"] == "3500"
    assert harness.INTENDED_DURABLE_SETTINGS["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX"] == "3500"
    assert harness._expected_stamped_budget() == 3500


def test_phase2_strict_preflight_rejects_inverted_budget(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    settings = dict(harness.INTENDED_API_SETTINGS)
    settings["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX"] = "2000"

    try:
        harness._assert_budget_settings_consistent(settings)
    except RuntimeError as exc:
        assert "min 3000 exceeds max 2000" in str(exc)
    else:
        raise AssertionError("strict preflight should reject inverted min/max")


def test_phase2_strict_preflight_requires_fixed_stamped_budget(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    settings = dict(harness.INTENDED_API_SETTINGS)
    settings["AZURE_MEMFOREST_MODEL_CALL_BUDGET_MAX"] = "3500"

    try:
        harness._assert_budget_settings_consistent(settings)
    except RuntimeError as exc:
        assert "requires min 3000 to equal max 3500" in str(exc)
    else:
        raise AssertionError("strict preflight should require a fixed stamped budget")


def test_phase2_strict_preflight_rejects_unexpected_stamped_budget(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_STRICT_PREFLIGHT="true")
    stamped_budget = {"max_completed_model_calls": 400}

    try:
        if int(stamped_budget.get("max_completed_model_calls") or 0) != harness._expected_stamped_budget():
            raise RuntimeError(f"api stamped unexpected progress budget for case:1: {stamped_budget}")
    except RuntimeError as exc:
        assert "unexpected progress budget" in str(exc)
    else:
        raise AssertionError("strict preflight should reject stamped budget 400")
