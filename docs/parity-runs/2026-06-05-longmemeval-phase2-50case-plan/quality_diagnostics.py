from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


_WORD = re.compile(r"[a-z0-9]+", re.IGNORECASE)
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "i",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
    "you",
}


def load_results(results_dir: Path) -> list[dict[str, Any]]:
    results_path = results_dir / "results.json"
    if results_path.exists():
        return json.loads(results_path.read_text(encoding="utf-8"))
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(results_dir.glob("[0-9][0-9]_*.json"))
    ]


def diagnose_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    cases = [diagnose_case(result) for result in results]
    miss_counts = Counter(
        case["miss_class"]
        for case in cases
        if case["miss_class"] != "correct"
    )
    by_type: dict[str, Counter[str]] = {}
    for case in cases:
        by_type.setdefault(case["question_type"], Counter())[case["miss_class"]] += 1
    return {
        "case_count": len(cases),
        "correct_count": sum(1 for case in cases if case["miss_class"] == "correct"),
        "miss_class_counts": dict(sorted(miss_counts.items())),
        "by_question_type": {
            question_type: dict(sorted(counts.items()))
            for question_type, counts in sorted(by_type.items())
        },
        "cases": cases,
    }


def diagnose_case(result: dict[str, Any]) -> dict[str, Any]:
    query_event = _query_event(result)
    query_response = query_event.get("response") or {}
    facts = query_response.get("facts") or []
    primary_correct = bool(((result.get("judges") or {}).get("primary") or {}).get("label"))
    gold_answer = str(result.get("gold_answer") or "")
    answer = str(result.get("answer") or "")
    gold_in_topk = _gold_hit(gold_answer, facts)
    answer_contains_gold = _text_hit(gold_answer, answer)
    input_shaping = [event.get("input_shaping") or {} for event in result.get("events") or [] if event.get("input_shaping")]
    assistant_extractable = sum(int(item.get("assistant_turns_extractable") or 0) for item in input_shaping)
    assistant_selective = sum(int(item.get("assistant_turns_selectively_extractable") or 0) for item in input_shaping)
    assistant_skipped = sum(int(item.get("assistant_turns_marked_skip_extraction") or 0) for item in input_shaping)
    fact_time_count = sum(1 for fact in facts if fact.get("time_start") is not None or fact.get("time_end") is not None)
    query_blob = json.dumps(query_event, ensure_ascii=False).lower()
    miss_class = _miss_class(
        result=result,
        primary_correct=primary_correct,
        gold_in_topk=gold_in_topk,
        answer_contains_gold=answer_contains_gold,
        retrieved_fact_count=len(facts),
        answer=answer,
        assistant_extractable=assistant_extractable,
        assistant_skipped=assistant_skipped,
    )
    return {
        "case_id": result.get("case_id"),
        "question_type": result.get("question_type") or "unknown",
        "status": result.get("status"),
        "primary_correct": primary_correct,
        "judge_agreement": (result.get("judges") or {}).get("agreement"),
        "miss_class": miss_class,
        "answer": answer,
        "gold_in_retrieved_topk": gold_in_topk,
        "answer_contains_gold": answer_contains_gold,
        "retrieved_fact_count": len(facts),
        "zero_retrieved_facts": len(facts) == 0,
        "retrieved_facts_with_time": fact_time_count,
        "all_retrieved_facts_timed": bool(facts) and fact_time_count == len(facts),
        "query_time_present_in_artifact": "query_time" in query_blob,
        "question_date_present_in_artifact": "question_date" in query_blob,
        "as_of_trace_present_in_artifact": "as_of_filtered_fact_count" in query_blob,
        "assistant_turns_extractable": assistant_extractable,
        "assistant_turns_selectively_extractable": assistant_selective,
        "assistant_turns_marked_skip_extraction": assistant_skipped,
        "selected_session_count": result.get("selected_session_count"),
        "selected_turn_count": result.get("selected_turn_count"),
    }


def _query_event(result: dict[str, Any]) -> dict[str, Any]:
    for event in result.get("events") or []:
        if event.get("step") == "query":
            return event
    return {}


def _miss_class(
    *,
    result: dict[str, Any],
    primary_correct: bool,
    gold_in_topk: bool,
    answer_contains_gold: bool,
    retrieved_fact_count: int,
    answer: str,
    assistant_extractable: int,
    assistant_skipped: int,
) -> str:
    if primary_correct:
        return "correct"
    question_type = str(result.get("question_type") or "")
    if answer_contains_gold and not ((result.get("judges") or {}).get("agreement") is True):
        return "judge_or_partial_credit"
    if gold_in_topk:
        if _is_abstention(answer):
            return "answerer_over_refusal"
        return "answerer_or_judge_miss"
    if question_type == "single-session-assistant" and assistant_extractable == 0 and assistant_skipped > 0:
        return "assistant_extraction_policy_miss"
    if retrieved_fact_count == 0:
        return "zero_retrieved_fact_miss"
    return "not_in_retrieved_topk"


def _gold_hit(gold_answer: str, facts: list[dict[str, Any]]) -> bool:
    return any(_text_hit(gold_answer, _fact_text(fact)) for fact in facts)


def _fact_text(fact: dict[str, Any]) -> str:
    return " ".join(
        str(fact.get(key) or "")
        for key in ("fact_text", "text", "content", "fact", "summary")
    )


def _text_hit(needle: str, haystack: str) -> bool:
    needle_norm = _normalize(needle)
    haystack_norm = _normalize(haystack)
    if not needle_norm:
        return False
    if f" {needle_norm} " in f" {haystack_norm} ":
        return True
    needle_tokens = _tokens(needle_norm)
    haystack_tokens = _tokens(haystack_norm)
    if not needle_tokens:
        return False
    overlap = needle_tokens & haystack_tokens
    if len(needle_tokens) >= 2 and needle_tokens <= haystack_tokens:
        return True
    return len(needle_tokens) >= 4 and len(overlap) / len(needle_tokens) >= 0.5


def _normalize(text: str) -> str:
    return " ".join(match.group(0).lower() for match in _WORD.finditer(text))


def _tokens(text: str) -> set[str]:
    return {token for token in text.split() if token not in _STOP_WORDS and len(token) >= 2}


def _is_abstention(answer: str) -> bool:
    normalized = _normalize(answer)
    return (
        not normalized
        or "information provided is not enough" in normalized
        or "not enough information" in normalized
        or normalized in {"i don t know", "unknown"}
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize quality diagnostics for a phase2 results directory.")
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    diagnostics = diagnose_results(load_results(args.results_dir))
    rendered = json.dumps(diagnostics, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
