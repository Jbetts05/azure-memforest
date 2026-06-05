from __future__ import annotations

import importlib.util
import json
from pathlib import Path


HARNESS_PATH = Path("docs/parity-runs/2026-06-05-longmemeval-phase2-50case-plan/run_50case.py")


def _load_harness(monkeypatch=None, **env):
    if monkeypatch is not None:
        for key, value in env.items():
            monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location("phase2_50case_harness", HARNESS_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_50case_harness_refuses_live_run_without_flag(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_50_SHARD="heldout_1")
    monkeypatch.setattr(harness, "load_cases", lambda: (_ for _ in ()).throw(AssertionError("should refuse before loading cases")))

    try:
        harness.main()
    except RuntimeError as exc:
        assert "Refusing live run" in str(exc)
    else:
        raise AssertionError("50-case harness should require explicit live-run approval")


def test_50case_harness_selects_shard_cases(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_50_SHARD="heldout_1")

    assert harness.selected_case_ids() == harness.CASE_INDEX["shards"]["heldout_1"]
    assert len(harness.selected_case_ids()) == 10


def test_50case_harness_selects_regression_cases(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_50_SHARD="regression")

    assert harness.selected_case_ids() == harness.CASE_INDEX["pilot_regression_case_ids"]
    assert len(harness.selected_case_ids()) == 10


def test_50case_pro_judge_sample_ids_are_deterministic(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_50_SHARD="heldout_1", PHASE2_50_PRO_JUDGE_SAMPLE_SIZE="2")
    cases = [{"case_id": case_id} for case_id in harness.CASE_INDEX["shards"]["heldout_1"]]

    assert harness.pro_judge_sample_ids(cases) == harness.pro_judge_sample_ids(list(reversed(cases)))
    assert len(harness.pro_judge_sample_ids(cases)) == 2


def test_50case_harness_loads_normalized_cases(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_50_SHARD="heldout_1")
    cases = harness.load_cases()

    assert len(cases) == 10
    assert cases[0]["case_id"] == harness.CASE_INDEX["shards"]["heldout_1"][0]
    assert cases[0]["sessions"]
    assert cases[0]["sessions"][0]["turns"][0]["content_id"]
    assert cases[0]["selected_session_count"] == len(cases[0]["sessions"])


def test_50case_pro_judge_parses_responses_output(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    captured = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "id": "resp-1",
                "status": "completed",
                "output": [
                    {"type": "reasoning", "summary": []},
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": "Yes"}],
                    },
                ],
            }

    def fake_post(url, *, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(harness.pilot, "model_headers", lambda: {"Authorization": "Bearer token"})
    monkeypatch.setattr(harness.pilot.requests, "post", fake_post)

    result = harness.pro_judge("prompt")

    assert result["label"] is True
    assert result["response"] == "Yes"
    assert "openai/responses?api-version=2025-04-01-preview" in captured["url"]
    assert captured["json"]["model"] == "gpt-5.4-pro-judge"
    assert captured["json"]["max_output_tokens"] >= 1024


def test_50case_yes_no_label_requires_leading_verdict(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    assert harness.yes_no_label("Yes.") is True
    assert harness.yes_no_label("No.") is False
    assert harness.yes_no_label("Yesterday, no.") is False


def test_50case_pro_judge_rejects_incomplete_response(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "incomplete", "output": []}

    monkeypatch.setattr(harness.pilot, "model_headers", lambda: {"Authorization": "Bearer token"})
    monkeypatch.setattr(harness.pilot.requests, "post", lambda *_args, **_kwargs: Response())

    try:
        harness.pro_judge("prompt")
    except RuntimeError as exc:
        assert "did not complete" in str(exc)
    else:
        raise AssertionError("incomplete pro judge responses should fail loudly")


def test_50case_pro_judge_rejects_empty_output_text(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "completed", "output": [{"type": "reasoning", "summary": []}]}

    monkeypatch.setattr(harness.pilot, "model_headers", lambda: {"Authorization": "Bearer token"})
    monkeypatch.setattr(harness.pilot.requests, "post", lambda *_args, **_kwargs: Response())

    try:
        harness.pro_judge("prompt")
    except RuntimeError as exc:
        assert "visible output_text" in str(exc)
    else:
        raise AssertionError("empty pro judge responses should fail loudly")


def test_50case_residue_provenance_flags_previous_forest_version(monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    results = [
        {
            "case_id": "previous",
            "question": "previous question",
            "events": [
                {
                    "step": "query",
                    "response": {
                        "facts": [
                            {"metadata": {"forest_version": "g1-previous"}},
                        ]
                    },
                }
            ],
        },
        {
            "case_id": "current",
            "benchmark_user_id": "user-current",
            "question": "current question",
            "events": [],
        },
    ]

    monkeypatch.setattr(
        harness.pilot,
        "residue_probe",
        lambda question, *, user_id: {
            "answer": "Previous answer",
            "facts": [{"metadata": {"forest_version": "g1-previous"}}],
        },
    )

    summary = harness.run_residue_reprobe(results)

    assert summary["raw_nonrefusal_events"] == 1
    assert summary["provenance_contamination_events"] == 1


def test_50case_per_shard_aggregate_status_is_diagnostic(monkeypatch) -> None:
    harness = _load_harness(monkeypatch, PHASE2_50_SHARD="heldout_1")
    results = [
        {
            "case_id": case_id,
            "status": "succeeded",
            "answer": "ok",
            "judges": {
                "primary": {"label": True},
                "cross_check": {"label": True},
                "agreement": True,
            },
            "events": [
                {
                    "step": "ingest",
                    "status": {"status": "succeeded"},
                    "durable_instance": {"runtimeStatus": "Completed"},
                }
            ],
        }
        for case_id in harness.CASE_INDEX["shards"]["heldout_1"]
    ]

    aggregate = harness.aggregate(results, {"deadletter": 19}, {"deadletter": 19})

    assert aggregate["status"] == "shard_completed"
    assert aggregate["pilot_gate_status_ignored"] is True


def test_50case_aggregate_shards_succeeds_for_heldout_gate(tmp_path, monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    shard_dirs = []
    for index in range(5):
        shard = tmp_path / f"shard-{index}"
        shard.mkdir()
        (shard / "aggregate.json").write_text(
            json.dumps(
                {
                    "cohort": "heldout_50",
                    "attempted_cases": 10,
                    "completed_cases": 10,
                    "primary_judge_correct": 8,
                    "judge_disagreements": 1,
                    "filtered_skip_count": 0,
                    "new_dlq_count_delta": 0,
                    "durable_instance_failed_count": 0,
                    "blocked_by_content_filter": 0,
                    "residue_postrun_raw_nonrefusal_events": 0,
                    "residue_contamination_events": 3,
                    "residue_postrun_provenance_events": 0,
                }
            ),
            encoding="utf-8",
        )
        shard_dirs.append(shard)

    aggregate = harness.aggregate_shards(shard_dirs)

    assert aggregate["status"] == "succeeded"
    assert aggregate["attempted_cases"] == 50
    assert aggregate["primary_judge_correct"] == 40
    assert aggregate["judge_disagreements"] == 5
    assert aggregate["residue_raw_nonrefusal_events"] == 0
    assert aggregate["residue_provenance_contamination_events"] == 0


def test_50case_aggregate_shards_rejects_low_correct_count(tmp_path, monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    shard_dirs = []
    for index in range(5):
        shard = tmp_path / f"shard-{index}"
        shard.mkdir()
        (shard / "aggregate.json").write_text(
            json.dumps(
                {
                    "cohort": "heldout_50",
                    "attempted_cases": 10,
                    "completed_cases": 10,
                    "primary_judge_correct": 7,
                    "judge_disagreements": 0,
                    "filtered_skip_count": 0,
                    "new_dlq_count_delta": 0,
                    "durable_instance_failed_count": 0,
                    "blocked_by_content_filter": 0,
                    "residue_postrun_raw_nonrefusal_events": 0,
                    "residue_postrun_provenance_events": 0,
                }
            ),
            encoding="utf-8",
        )
        shard_dirs.append(shard)

    aggregate = harness.aggregate_shards(shard_dirs)

    assert aggregate["status"] == "gate_failed"
    assert aggregate["primary_judge_correct"] == 35


def test_50case_aggregate_shards_rejects_raw_residue_nonrefusal(tmp_path, monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    shard_dirs = []
    for index in range(5):
        shard = tmp_path / f"shard-{index}"
        shard.mkdir()
        (shard / "aggregate.json").write_text(
            json.dumps(
                {
                    "cohort": "heldout_50",
                    "attempted_cases": 10,
                    "completed_cases": 10,
                    "primary_judge_correct": 8,
                    "judge_disagreements": 0,
                    "filtered_skip_count": 0,
                    "new_dlq_count_delta": 0,
                    "durable_instance_failed_count": 0,
                    "blocked_by_content_filter": 0,
                    "residue_postrun_raw_nonrefusal_events": 1 if index == 0 else 0,
                    "residue_postrun_provenance_events": 0,
                }
            ),
            encoding="utf-8",
        )
        shard_dirs.append(shard)

    aggregate = harness.aggregate_shards(shard_dirs)

    assert aggregate["status"] == "gate_failed"
    assert aggregate["residue_raw_nonrefusal_events"] == 1


def test_50case_aggregate_shards_requires_raw_residue_key(tmp_path, monkeypatch) -> None:
    harness = _load_harness(monkeypatch)
    shard_dirs = []
    for index in range(5):
        shard = tmp_path / f"shard-{index}"
        shard.mkdir()
        payload = {
            "cohort": "heldout_50",
            "attempted_cases": 10,
            "completed_cases": 10,
            "primary_judge_correct": 8,
            "judge_disagreements": 0,
            "filtered_skip_count": 0,
            "new_dlq_count_delta": 0,
            "durable_instance_failed_count": 0,
            "blocked_by_content_filter": 0,
            "residue_postrun_provenance_events": 0,
        }
        if index != 0:
            payload["residue_postrun_raw_nonrefusal_events"] = 0
        (shard / "aggregate.json").write_text(json.dumps(payload), encoding="utf-8")
        shard_dirs.append(shard)

    try:
        harness.aggregate_shards(shard_dirs)
    except RuntimeError as exc:
        assert "missing residue_postrun_raw_nonrefusal_events" in str(exc)
    else:
        raise AssertionError("missing raw residue key should fail loudly")
