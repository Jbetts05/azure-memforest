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
    assert stats == {"assistant_turns_marked_skip_extraction": 1, "user_turns_extractable": 1}

