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
