from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_harness():
    path = Path("docs/parity-runs/2026-05-31-longmemeval-phase1-checkpoint-resume/run_calibration.py")
    spec = importlib.util.spec_from_file_location("phase1_checkpoint_harness", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_harness_with_env(monkeypatch, **env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return _load_harness()


def test_phase1_harness_marks_only_assistant_turns_skip_extraction() -> None:
    harness = _load_harness()
    turns, stats = harness.turns_for_ingest(
        {
            "turns": [
                {"role": "user", "content": "I have 3 sisters.", "metadata": {"source": "fixture"}},
                {"role": "assistant", "content": "Here is generic advice.", "metadata": {"source": "fixture"}},
            ]
        }
    )

    assert turns[0]["metadata"] == {"source": "fixture"}
    assert turns[1]["metadata"] == {"source": "fixture", "skip_extraction": True}
    assert stats["assistant_turns_marked_skip_extraction"] == 1
    assert stats["user_turns_extractable"] == 1
    assert stats["evidence_minimized"] is False


def test_phase1_harness_applies_declarative_evidence_override() -> None:
    harness = _load_harness()
    turns, stats = harness.turns_for_ingest(
        {
            "source_session_id": "answer_477ae455_1",
            "turns": [
                {"role": "user", "content_id": "answer_477ae455_1:0", "content": "noise", "metadata": {}},
                {"role": "user", "content_id": "answer_477ae455_1:2", "content": "I have 3 sisters.", "metadata": {}},
                {"role": "assistant", "content_id": "answer_477ae455_1:3", "content": "advice", "metadata": {}},
            ],
        },
        case_id="8e91e7d9",
    )

    assert [turn["content_id"] for turn in turns] == ["answer_477ae455_1:2"]
    assert stats["evidence_minimized"] is True
    assert stats["exclude_from_pipeline_accuracy"] is True
    assert stats["original_turn_count"] == 3
    assert stats["retained_turn_count"] == 1


def test_phase1_harness_applies_synthetic_override_turns() -> None:
    harness = _load_harness()
    turns, stats = harness.turns_for_ingest(
        {
            "source_session_id": "answer_477ae455_2",
            "turns": [
                {"role": "user", "content_id": "answer_477ae455_2:0", "content": "I have a brother.", "metadata": {}},
                {"role": "assistant", "content_id": "answer_477ae455_2:1", "content": "advice", "metadata": {}},
            ],
        },
        case_id="8e91e7d9",
    )

    assert [turn["content_id"] for turn in turns] == [
        "answer_477ae455_2:0",
        "phase1_override_8e91e7d9_sibling_total",
    ]
    assert stats["synthetic_turn_count"] == 1
    assert stats["synthetic_content_ids"] == ["phase1_override_8e91e7d9_sibling_total"]


def test_phase1_harness_dev_auth_adds_benchmark_role(monkeypatch) -> None:
    harness = _load_harness_with_env(monkeypatch, PHASE1_AUTH_MODE="dev")

    headers = harness.api_headers(user_id="benchmark-user", json_body=True, idem="idem-1")

    assert "x-benchmark-auth" not in headers
    assert headers["x-tenant-id"] == harness.TENANT
    assert headers["x-actor-id"] == "benchmark-user"
    assert headers["x-auth-roles"] == "memory.benchmark"
    assert headers["Idempotency-Key"] == "idem-1"


def test_phase1_harness_durable_aggregate_observes_but_does_not_gate_dlq(monkeypatch) -> None:
    harness = _load_harness_with_env(monkeypatch, PHASE1_DISPATCH_MODE="durable")
    result = {
        "status": "succeeded",
        "case_id": "case-1",
        "answer": "ok",
        "judges": {"primary": {"label": True}, "cross_check": {"label": True}, "agreement": True},
        "events": [
            {
                "step": "ingest",
                "status": {"status": "succeeded"},
                "durable_instance": {"runtimeStatus": "Completed"},
            }
        ],
    }

    aggregate = harness.aggregate([result], {"deadletter": 1}, {"deadletter": 3})

    assert aggregate["status"] == "succeeded"
    assert aggregate["dispatch_mode"] == "durable"
    assert aggregate["new_dlq_count_delta"] == 2
    assert aggregate["dlq_gate_enforced"] is False
    assert aggregate["durable_instance_count"] == 1
    assert aggregate["durable_instance_completed_count"] == 1
    assert aggregate["durable_gate_passed"] is True


def test_phase1_harness_durable_aggregate_requires_completed_instances(monkeypatch) -> None:
    harness = _load_harness_with_env(monkeypatch, PHASE1_DISPATCH_MODE="durable")
    result = {
        "status": "succeeded",
        "case_id": "case-1",
        "answer": "ok",
        "judges": {"primary": {"label": True}, "cross_check": {"label": True}, "agreement": True},
        "events": [{"step": "ingest", "status": {"status": "succeeded"}}],
    }

    aggregate = harness.aggregate([result], {"deadletter": 0}, {"deadletter": 0})

    assert aggregate["status"] == "gate_failed"
    assert aggregate["durable_gate_passed"] is False


def test_phase1_harness_servicebus_aggregate_gates_dlq(monkeypatch) -> None:
    harness = _load_harness_with_env(monkeypatch, PHASE1_DISPATCH_MODE="servicebus")
    result = {
        "status": "succeeded",
        "case_id": "case-1",
        "answer": "ok",
        "judges": {"primary": {"label": True}, "cross_check": {"label": True}, "agreement": True},
        "events": [],
    }

    aggregate = harness.aggregate([result], {"deadletter": 1}, {"deadletter": 2})

    assert aggregate["status"] == "gate_failed"
    assert aggregate["dlq_gate_enforced"] is True
