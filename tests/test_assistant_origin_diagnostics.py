from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIAGNOSTICS_PATH = ROOT / "docs/parity-runs/2026-06-05-longmemeval-phase2-50case-plan/assistant_origin_diagnostics.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("assistant_origin_diagnostics", DIAGNOSTICS_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_assistant_origin_diagnostics_classifies_store_not_topk() -> None:
    diagnostics = _load_module()
    result = {
        "case_id": "8464fc84",
        "benchmark_user_id": "user-1",
        "answer": "I don't know",
        "judges": {"primary": {"label": False}},
        "events": [
            {"step": "query", "response": {"facts": [{"fact_text": "The assistant mentioned a deli."}]}},
        ],
    }

    row = diagnostics.diagnose_case(
        result=result,
        needles=["roscioli"],
        topk_facts=diagnostics._query_facts(result),
        store_facts=[{"fact_text": "The assistant recommended Roscioli near the Vatican."}],
        store_inspected=True,
    )

    assert row["classification"] == "gold_evidence_in_store_not_topk"
    assert row["store_hits"] == {"roscioli": True}
    assert row["topk_hits"] == {"roscioli": False}


def test_assistant_origin_diagnostics_classifies_absent_from_store() -> None:
    diagnostics = _load_module()
    result = {
        "case_id": "dc439ea3",
        "benchmark_user_id": "user-1",
        "answer": "I don't know",
        "judges": {"primary": {"label": False}},
        "events": [{"step": "query", "response": {"facts": []}}],
    }

    row = diagnostics.diagnose_case(
        result=result,
        needles=["hoop"],
        topk_facts=[],
        store_facts=[{"fact_text": "The assistant discussed powwows."}],
        store_inspected=True,
    )

    assert row["classification"] == "gold_evidence_not_lexically_found_in_store"


def test_assistant_origin_diagnostics_classifies_partial_store_hit() -> None:
    diagnostics = _load_module()
    result = {
        "case_id": "6ae235be",
        "benchmark_user_id": "user-1",
        "answer": "I don't know",
        "judges": {"primary": {"label": False}},
        "events": [{"step": "query", "response": {"facts": []}}],
    }

    row = diagnostics.diagnose_case(
        result=result,
        needles=["atmospheric distillation", "fluid catalytic", "alkylation", "hydrotreating"],
        topk_facts=[],
        store_facts=[{"fact_text": "The assistant mentioned atmospheric distillation and alkylation."}],
        store_inspected=True,
    )

    assert row["classification"] == "partial_gold_evidence_in_store"
    assert row["store_hit_count"] == 2
    assert row["relaxed_store_hit_count"] == 2


def test_assistant_origin_diagnostics_classifies_relaxed_store_hit() -> None:
    diagnostics = _load_module()
    result = {
        "case_id": "6ae235be",
        "benchmark_user_id": "user-1",
        "answer": "I don't know",
        "judges": {"primary": {"label": False}},
        "events": [{"step": "query", "response": {"facts": []}}],
    }

    row = diagnostics.diagnose_case(
        result=result,
        needles=["fluid catalytic"],
        topk_facts=[],
        store_facts=[{"fact_text": "The assistant mentioned catalytic processes and fluid treatment separately."}],
        store_inspected=True,
    )

    assert row["classification"] == "relaxed_gold_evidence_in_store"
    assert row["store_hits"] == {"fluid catalytic": False}
    assert row["relaxed_store_hits"] == {"fluid catalytic": True}


def test_assistant_origin_diagnostics_matches_accents_and_metadata() -> None:
    diagnostics = _load_module()
    result = {
        "case_id": "70b3e69b",
        "benchmark_user_id": "user-1",
        "answer": "I don't know",
        "judges": {"primary": {"label": False}},
        "events": [{"step": "query", "response": {"facts": []}}],
    }

    row = diagnostics.diagnose_case(
        result=result,
        needles=["garcia"],
        topk_facts=[],
        store_facts=[{"metadata": {"alternate_fact_texts": ["The assistant mentioned Manolo García."]}}],
        store_inspected=True,
    )

    assert row["classification"] == "gold_evidence_in_store_not_topk"


def test_assistant_origin_diagnostics_runs_artifact_only(tmp_path: Path) -> None:
    diagnostics = _load_module()
    case = {
        "case_id": "cc539528",
        "benchmark_user_id": "user-1",
        "answer": "Ruby, Python, and PHP.",
        "judges": {"primary": {"label": True}},
        "events": [
            {
                "step": "query",
                "response": {"facts": [{"fact_text": "The assistant advised learning Ruby, Python, or PHP."}]},
            }
        ],
    }
    (tmp_path / "01_cc539528.json").write_text(__import__("json").dumps(case), encoding="utf-8")

    summary = diagnostics.diagnose_artifact_cases(tmp_path)

    assert summary["case_count"] == 1
    assert summary["classification_counts"] == {"gold_evidence_in_topk": 1}
