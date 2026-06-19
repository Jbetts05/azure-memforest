from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIAGNOSTICS_PATH = ROOT / "docs/parity-runs/2026-06-05-longmemeval-phase2-50case-plan/quality_diagnostics.py"


def _load_diagnostics():
    spec = importlib.util.spec_from_file_location("phase2_quality_diagnostics", DIAGNOSTICS_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_quality_diagnostics_classifies_assistant_skip_policy_miss() -> None:
    diagnostics = _load_diagnostics()

    case = {
        "case_id": "assistant-case",
        "question_type": "single-session-assistant",
        "status": "succeeded",
        "gold_answer": "Roscioli",
        "answer": "I don't know",
        "judges": {"primary": {"label": False}, "agreement": True},
        "events": [
            {
                "step": "ingest",
                "input_shaping": {
                    "assistant_turns_extractable": 0,
                    "assistant_turns_marked_skip_extraction": 3,
                },
            },
            {
                "step": "query",
                "response": {"facts": [{"fact_text": "The user asked about restaurants.", "time_start": 1.0}]},
            },
        ],
    }

    result = diagnostics.diagnose_case(case)

    assert result["miss_class"] == "assistant_extraction_policy_miss"
    assert result["assistant_turns_marked_skip_extraction"] == 3
    assert result["gold_in_retrieved_topk"] is False


def test_quality_diagnostics_classifies_answerer_miss_when_gold_fact_was_retrieved() -> None:
    diagnostics = _load_diagnostics()

    case = {
        "case_id": "temporal-case",
        "question_type": "temporal-reasoning",
        "status": "succeeded",
        "gold_answer": "The Hate U Give",
        "answer": "The Nightingale.",
        "judges": {"primary": {"label": False}, "agreement": True},
        "events": [
            {
                "step": "query",
                "response": {
                    "facts": [
                        {
                            "fact_text": "The user finished The Hate U Give two weeks before the meeting.",
                            "time_start": 1685450520.0,
                            "time_end": 1685450520.0,
                        }
                    ]
                },
            }
        ],
    }

    result = diagnostics.diagnose_case(case)

    assert result["miss_class"] == "answerer_or_judge_miss"
    assert result["gold_in_retrieved_topk"] is True
    assert result["all_retrieved_facts_timed"] is True


def test_quality_diagnostics_classifies_answerer_over_refusal_when_gold_fact_was_retrieved() -> None:
    diagnostics = _load_diagnostics()

    case = {
        "case_id": "over-refusal",
        "question_type": "single-session-user",
        "status": "succeeded",
        "gold_answer": "500",
        "answer": "The information provided is not enough to answer this question.",
        "judges": {"primary": {"label": False}, "agreement": True},
        "events": [
            {
                "step": "query",
                "response": {
                    "facts": [
                        {
                            "fact_text": "The user donated 500 dollars.",
                            "time_start": 1685450520.0,
                            "time_end": 1685450520.0,
                        }
                    ]
                },
            }
        ],
    }

    result = diagnostics.diagnose_case(case)

    assert result["miss_class"] == "answerer_over_refusal"
    assert result["gold_in_retrieved_topk"] is True
    assert result["zero_retrieved_facts"] is False


def test_quality_diagnostics_classifies_zero_retrieved_fact_miss() -> None:
    diagnostics = _load_diagnostics()

    case = {
        "case_id": "zero-facts",
        "question_type": "temporal-reasoning",
        "status": "succeeded",
        "gold_answer": "Galaxy S22",
        "answer": "",
        "judges": {"primary": {"label": False}, "agreement": True},
        "events": [{"step": "query", "response": {"facts": []}}],
    }

    result = diagnostics.diagnose_case(case)

    assert result["miss_class"] == "zero_retrieved_fact_miss"
    assert result["retrieved_fact_count"] == 0
    assert result["zero_retrieved_facts"] is True


def test_quality_diagnostics_summarizes_counts_by_question_type() -> None:
    diagnostics = _load_diagnostics()

    summary = diagnostics.diagnose_results(
        [
            {
                "case_id": "ok",
                "question_type": "single-session-user",
                "status": "succeeded",
                "gold_answer": "500",
                "answer": "500",
                "judges": {"primary": {"label": True}, "agreement": True},
                "events": [],
            },
            {
                "case_id": "miss",
                "question_type": "multi-session",
                "status": "succeeded",
                "gold_answer": "four",
                "answer": "one",
                "judges": {"primary": {"label": False}, "agreement": True},
                "events": [{"step": "query", "response": {"facts": []}}],
            },
        ]
    )

    assert summary["case_count"] == 2
    assert summary["correct_count"] == 1
    assert summary["miss_class_counts"] == {"zero_retrieved_fact_miss": 1}
    assert summary["by_question_type"]["single-session-user"] == {"correct": 1}


def test_quality_diagnostics_requires_gold_hit_within_one_fact() -> None:
    diagnostics = _load_diagnostics()

    facts = [
        {"fact_text": "The user finished The Hate U Give.", "time_start": 1.0},
        {"fact_text": "The other book was mentioned two weeks later.", "time_start": 2.0},
    ]

    assert diagnostics._gold_hit("The Hate U Give two weeks before meeting", facts) is False


def test_quality_diagnostics_short_answer_does_not_substring_match() -> None:
    diagnostics = _load_diagnostics()

    assert diagnostics._text_hit("yes", "The user visited yesterday.") is False
    assert diagnostics._text_hit("4", "The user attended 4 events.") is True
