from __future__ import annotations

import ast
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = ROOT / "src"
OFFLINE_ALLOWED = {
    ROOT / "src" / "evaluation",
    ROOT / "src" / "extraction" / "runner.py",
}
FORBIDDEN_RUNTIME_MARKERS = {
    "gold_answer",
    "gold answer",
    "gold date",
}
FORBIDDEN_DATASET_ACCESSORS = [
    re.compile(r"\b(?:item|case|row|record)\s*\.\s*get\(\s*['\"](?:answer|question_type)['\"]", re.IGNORECASE),
    re.compile(r"\b(?:item|case|row|record)\s*\[\s*['\"](?:answer|question_type)['\"]\s*\]", re.IGNORECASE),
]
FORBIDDEN_GOLD_ACCESSORS = [
    re.compile(r"\b(?:item|case|row|record)\s*\.\s*get\(\s*['\"]answer['\"]", re.IGNORECASE),
    re.compile(r"\b(?:item|case|row|record)\s*\[\s*['\"]answer['\"]\s*\]", re.IGNORECASE),
]


def test_runtime_code_has_no_gold_answer_markers() -> None:
    offenders: list[str] = []
    for path in RUNTIME_ROOT.rglob("*.py"):
        if _is_offline_allowed(path):
            continue
        text = path.read_text(encoding="utf-8").lower()
        hits = [marker for marker in FORBIDDEN_RUNTIME_MARKERS if marker in text]
        hits.extend(pattern.pattern for pattern in FORBIDDEN_DATASET_ACCESSORS if pattern.search(text))
        if hits:
            rel = path.relative_to(ROOT)
            offenders.append(f"{rel}: {', '.join(sorted(hits))}")

    assert offenders == []


def test_offline_extraction_runner_does_not_feed_gold_into_requests() -> None:
    runner_path = ROOT / "src" / "extraction" / "runner.py"
    source = runner_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func_name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        if func_name != "ExtractionRequest":
            continue
        segment = ast.get_source_segment(source, node) or ""
        lowered = segment.lower()
        if "gold" in lowered or any(pattern.search(segment) for pattern in FORBIDDEN_GOLD_ACCESSORS):
            offenders.append(segment)

    assert offenders == []


def _is_offline_allowed(path: Path) -> bool:
    return any(path == allowed or allowed in path.parents for allowed in OFFLINE_ALLOWED)


def test_quality_validation_protocol_requires_clean_no_leak_runs() -> None:
    protocol_path = ROOT / "docs" / "parity-runs" / "2026-06-05-longmemeval-phase2-50case-plan" / "quality_validation_protocol.json"

    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))

    assert protocol["runtime_firewall"]["benchmark_question_type_forbidden_for_runtime_routing"] is True
    assert "src runtime retrieval" in protocol["runtime_firewall"]["gold_answers_forbidden_in"]
    assert protocol["clean_run_requirements"]["single_code_hash"] is True
    assert protocol["clean_run_requirements"]["no_mid_run_remediation"] is True
    assert protocol["clean_run_requirements"]["quality_diagnostics_required"] is True
    assert protocol["miss_classification"]["durable_store_check_required_before_extraction_prompt_changes"] is True
    assert protocol["parity_claim_gate"]["requires_4_8_review"] is True
