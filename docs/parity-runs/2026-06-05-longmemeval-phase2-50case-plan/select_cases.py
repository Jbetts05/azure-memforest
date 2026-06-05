from __future__ import annotations

import hashlib
import json
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


SOURCE_URL = "https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json"
SOURCE_SHA256 = "d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442"
SELECTION_SEED = "20260530"
PILOT_CASES_PATH = Path("../2026-06-03-longmemeval-phase2-intended-pilot/pilot_cases.json")
OUTPUT_PATH = Path("cases_index.json")

HELDOUT_TARGETS = {
    "knowledge-update": 8,
    "multi-session": 13,
    "single-session-assistant": 6,
    "single-session-preference": 3,
    "single-session-user": 7,
    "temporal-reasoning": 13,
}


def _case_id(item: dict[str, Any]) -> str:
    return str(item["question_id"])


def _score(item: dict[str, Any]) -> str:
    return hashlib.sha256((SELECTION_SEED + _case_id(item)).encode("utf-8")).hexdigest()


def _load_source() -> list[dict[str, Any]]:
    raw = urllib.request.urlopen(SOURCE_URL, timeout=120).read()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != SOURCE_SHA256:
        raise RuntimeError(f"source SHA mismatch: expected {SOURCE_SHA256}, got {digest}")
    return json.loads(raw)


def _load_pilot_ids() -> list[str]:
    cases = json.loads(PILOT_CASES_PATH.read_text(encoding="utf-8"))
    return [str(case["case_id"]) for case in cases]


def _select_heldout(source: list[dict[str, Any]], pilot_ids: set[str]) -> list[dict[str, Any]]:
    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in source:
        if _case_id(item) not in pilot_ids:
            by_type[str(item["question_type"])].append(item)
    for rows in by_type.values():
        rows.sort(key=_score)

    selected: list[dict[str, Any]] = []
    for question_type in sorted(HELDOUT_TARGETS):
        rows = by_type[question_type]
        target = HELDOUT_TARGETS[question_type]
        if len(rows) < target:
            raise RuntimeError(f"not enough {question_type} cases to select {target}")
        selected.extend(rows[:target])
    return selected


def _assign_shards(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    indexed = [
        {
            "case_id": _case_id(item),
            "question_type": str(item["question_type"]),
            "cohort": "heldout_50",
        }
        for item in rows
    ]
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in indexed:
        groups[item["question_type"]].append(item)

    shards: list[list[dict[str, Any]]] = [[] for _ in range(5)]
    for question_type, items in sorted(groups.items(), key=lambda entry: (-len(entry[1]), entry[0])):
        for item in items:
            shard_index = sorted(
                range(5),
                key=lambda index: (
                    len(shards[index]),
                    sum(1 for existing in shards[index] if existing["question_type"] == question_type),
                    index,
                ),
            )[0]
            item["shard"] = shard_index + 1
            shards[shard_index].append(item)

    for shard in shards:
        shard.sort(key=lambda item: (item["question_type"], item["case_id"]))
    return [item for shard in shards for item in shard]


def main() -> None:
    source = _load_source()
    pilot_ids = _load_pilot_ids()
    heldout = _assign_shards(_select_heldout(source, set(pilot_ids)))
    output = {
        "source_url": SOURCE_URL,
        "source_sha256": SOURCE_SHA256,
        "selection_seed": int(SELECTION_SEED),
        "pilot_regression_case_ids": pilot_ids,
        "heldout_targets": HELDOUT_TARGETS,
        "heldout_counts": dict(Counter(item["question_type"] for item in heldout)),
        "heldout_case_count": len(heldout),
        "heldout_case_ids": [item["case_id"] for item in heldout],
        "cases": heldout,
        "shards": {
            f"heldout_{index}": [item["case_id"] for item in heldout if item["shard"] == index]
            for index in range(1, 6)
        },
    }
    OUTPUT_PATH.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(json.dumps({key: output[key] for key in ("heldout_case_count", "heldout_counts")}, indent=2))


if __name__ == "__main__":
    main()
