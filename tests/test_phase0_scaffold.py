from __future__ import annotations

import csv
from pathlib import Path

from src.build.node_index import NodeEntry, NodeIndex
from src.cloud import FactLocation, RecallCandidate
from src.evaluation.benchmark_summary import (
    default_benchmark_paths,
    parse_judge_value,
    summarize_benchmark_file,
)
from src.observability import InMemoryTraceSink, TraceEvent, TraceEventType


ROOT = Path(__file__).resolve().parents[1]


def test_license_is_mit() -> None:
    text = (ROOT / "LICENSE").read_text(encoding="utf-8")

    assert "MIT License" in text
    assert "Permission is hereby granted, free of charge" in text


def test_benchmark_headers_are_present() -> None:
    expected_columns = {
        "method",
        "model_size",
        "question_type",
        "qid",
        "question",
        "gold_answer",
        "answer_1",
        "judge_1",
    }
    benchmark_files = [
        ROOT / "benchmark" / "locomo_per_question_30b.csv",
        ROOT / "benchmark" / "locomo_per_question_4b.csv",
        ROOT / "benchmark" / "longmemeval_per_question_30b.csv",
        ROOT / "benchmark" / "longmemeval_per_question_4b.csv",
    ]

    for path in benchmark_files:
        with path.open(encoding="utf-8", newline="") as handle:
            header = next(csv.reader([handle.readline()]))

        assert expected_columns.issubset(set(header)), path


def test_cloud_contract_dataclasses_are_forward_compatible() -> None:
    candidate = RecallCandidate(tree_id="tree-1", node_id="node-1", score=0.9)
    location = FactLocation(
        fact_id="fact-1",
        tree_id="tree-1",
        node_id="node-1",
        tree_type="session",
        level="L0",
    )

    assert candidate.tenant_id is None
    assert candidate.user_id is None
    assert location.forest_version is None


def test_in_memory_trace_sink_filters_by_query() -> None:
    sink = InMemoryTraceSink()
    sink.emit(TraceEvent(query_id="q1", event_type=TraceEventType.QUERY_STARTED))
    sink.emit(TraceEvent(query_id="q2", event_type=TraceEventType.QUERY_STARTED))
    sink.emit(TraceEvent(query_id="q1", event_type=TraceEventType.QUERY_COMPLETED))

    q1_events = sink.events(query_id="q1")

    assert len(sink.events()) == 3
    assert [event.event_type for event in q1_events] == [
        TraceEventType.QUERY_STARTED,
        TraceEventType.QUERY_COMPLETED,
    ]


def test_node_index_supports_basic_local_lookup_and_search(tmp_path: Path) -> None:
    index = NodeIndex(index_dir=tmp_path, vector_dim=2)
    index.add_node(NodeEntry(node_id="node-a", tree_id="tree-a", level="root"), [1.0, 0.0])
    index.add_node(NodeEntry(node_id="node-b", tree_id="tree-b", level="root"), [0.0, 1.0])

    results = index.search([1.0, 0.0], top_n=2)

    assert index.get_embedding("node-a") == [1.0, 0.0]
    assert [entry.node_id for entry in results] == ["node-a", "node-b"]


def test_benchmark_summary_groups_pass_at_by_method_and_question_type(tmp_path: Path) -> None:
    path = tmp_path / "locomo_per_question_test.csv"
    path.write_text(
        "\n".join(
            [
                "method,model_size,question_type,qid,question,gold_answer,answer_1,judge_1,answer_2,judge_2,answer_3,judge_3",
                "memforest,30b,single,q1,Q,G,A,CORRECT,A,WRONG,A,WRONG",
                "memforest,30b,single,q2,Q,G,A,WRONG,A,CORRECT,A,WRONG",
                "memforest,30b,multi,q3,Q,G,A,WRONG,A,WRONG,A,WRONG",
                "baseline,30b,single,q4,Q,G,A,WRONG,A,WRONG,A,CORRECT",
            ]
        ),
        encoding="utf-8",
    )

    summary = summarize_benchmark_file(path)
    groups = {
        (group.method, group.question_type): group
        for group in summary.groups
    }

    memforest_single = groups[("memforest", "single")]
    assert memforest_single.total_rows == 2
    assert memforest_single.cumulative_pass_at[1] == 0.5
    assert memforest_single.cumulative_pass_at[2] == 1.0
    assert memforest_single.cumulative_pass_at[3] == 1.0
    assert memforest_single.missing_or_invalid_judges == 0

    baseline_single = groups[("baseline", "single")]
    assert baseline_single.cumulative_pass_at[1] == 0.0
    assert baseline_single.cumulative_pass_at[3] == 1.0

    rollups = {group.method: group for group in summary.method_rollups}
    assert rollups["memforest"].question_type == "ALL"
    assert rollups["memforest"].total_rows == 3
    assert rollups["memforest"].cumulative_pass_at[1] == 1 / 3
    assert rollups["memforest"].cumulative_pass_at[2] == 2 / 3


def test_judge_value_parsing_matches_released_csv_values() -> None:
    assert parse_judge_value("CORRECT") is True
    assert parse_judge_value("WRONG") is False
    assert parse_judge_value("1") is True
    assert parse_judge_value("0") is False
    assert parse_judge_value("") is None
    assert parse_judge_value("unexpected") is None


def test_default_benchmark_paths_are_stable() -> None:
    names = [path.name for path in default_benchmark_paths(ROOT)]

    assert names == [
        "locomo_per_question_30b.csv",
        "locomo_per_question_4b.csv",
        "longmemeval_per_question_30b.csv",
        "longmemeval_per_question_4b.csv",
    ]
