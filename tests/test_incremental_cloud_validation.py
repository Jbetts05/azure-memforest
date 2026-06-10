from __future__ import annotations

from src.evaluation.incremental_cloud_validation import (
    ProjectionWriteCounts,
    QueryParityCase,
    compare_projection_write_reduction,
    compare_query_parity,
    evaluate_validation_payload,
)


def test_projection_reduction_gate_passes_when_incremental_writes_only_changed_projection_records() -> None:
    result = compare_projection_write_reduction(
        baseline_full=ProjectionWriteCounts(
            stored_nodes=12,
            fact_locations=8,
            recall_documents=4,
            browse_embeddings=12,
            summary_cache_entries=6,
        ),
        candidate_incremental=ProjectionWriteCounts(
            stored_nodes=3,
            fact_locations=2,
            recall_documents=1,
            browse_embeddings=3,
            summary_cache_entries=1,
        ),
        min_total_reduction_ratio=0.5,
    )

    assert result.ok
    assert result.total_reduction_ratio is not None
    assert result.total_reduction_ratio > 0.5


def test_projection_reduction_gate_fails_when_candidate_rewrites_full_projection() -> None:
    result = compare_projection_write_reduction(
        baseline_full=ProjectionWriteCounts(stored_nodes=10, fact_locations=4, recall_documents=2),
        candidate_incremental=ProjectionWriteCounts(stored_nodes=10, fact_locations=4, recall_documents=2),
        min_total_reduction_ratio=0.25,
    )

    assert not result.ok
    assert result.failures == ["total:reduction_below_threshold:0.000000<0.250000"]


def test_query_parity_gate_accepts_normalized_answers_and_exact_retrieval_order() -> None:
    result = compare_query_parity(
        [
            QueryParityCase(
                case_id="q1",
                baseline_answer="Alice prefers tea.",
                candidate_answer="  alice   prefers tea. ",
                baseline_retrieved_fact_ids=("fact-1", "fact-2"),
                candidate_retrieved_fact_ids=("fact-1", "fact-2"),
                baseline_recalled_tree_ids=("entity:alice", "session:s1"),
                candidate_recalled_tree_ids=("entity:alice", "session:s1"),
            )
        ]
    )

    assert result.ok
    assert result.checked_cases == 1


def test_query_parity_gate_reports_answer_and_retrieval_mismatches() -> None:
    result = compare_query_parity(
        [
            QueryParityCase(
                case_id="q1",
                baseline_answer="Alice prefers tea.",
                candidate_answer="Alice prefers coffee.",
                baseline_retrieved_fact_ids=("fact-1", "fact-2"),
                candidate_retrieved_fact_ids=("fact-2", "fact-1"),
                baseline_recalled_tree_ids=("entity:alice",),
                candidate_recalled_tree_ids=("session:s1",),
            )
        ]
    )

    assert result.failures == [
        "q1:answer_mismatch",
        "q1:retrieved_fact_ids_mismatch",
        "q1:recalled_tree_ids_mismatch",
    ]


def test_payload_harness_combines_projection_reduction_and_query_parity() -> None:
    result = evaluate_validation_payload(
        {
            "projection": {
                "baseline_full": {"stored_nodes": 9, "fact_locations": 3, "recall_documents": 3},
                "candidate_incremental": {"stored_nodes": 3, "fact_locations": 1, "recall_documents": 1},
                "min_total_reduction_ratio": 0.5,
            },
            "query_cases": [
                {
                    "case_id": "q1",
                    "baseline": {
                        "answer": "Needle fact",
                        "retrieved_fact_ids": ["fact-needle"],
                        "recalled_tree_ids": ["entity:needle"],
                    },
                    "candidate": {
                        "answer": "needle fact",
                        "retrieved_fact_ids": ["fact-needle"],
                        "recalled_tree_ids": ["entity:needle"],
                    },
                }
            ],
        }
    )

    assert result.ok
