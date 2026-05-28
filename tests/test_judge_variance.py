from __future__ import annotations

from pathlib import Path

from src.evaluation.judge_variance import (
    build_judge_user_prompt,
    compute_existing_label_variance,
    sample_judged_answers,
)


def test_existing_label_variance_counts_disagreement(tmp_path: Path) -> None:
    path = tmp_path / "bench.csv"
    path.write_text(
        "method,model_size,question_type,qid,question,gold_answer,answer_1,judge_1,answer_2,judge_2\n"
        "m,4b,type,q1,Q?,G,A,1,B,0\n"
        "m,4b,type,q2,Q?,G,A,1,B,1\n",
        encoding="utf-8",
    )

    variance = compute_existing_label_variance([path])

    assert variance.row_count == 2
    assert variance.answer_label_count == 4
    assert variance.rows_with_disagreement == 1
    assert variance.answer_label_disagreement_rate == 0.25
    assert variance.max_row_minority_rate == 0.5


def test_sample_judged_answers_builds_prompts_without_random_shape(tmp_path: Path) -> None:
    path = tmp_path / "bench.csv"
    path.write_text(
        "method,model_size,question_type,qid,question,gold_answer,answer_1,judge_1\n"
        "m,4b,type,q1,Where?,Gold,Answer,correct\n",
        encoding="utf-8",
    )

    sample = sample_judged_answers([path], max_samples=1, seed=1)[0]
    prompt = build_judge_user_prompt(sample)

    assert sample.original_judge is True
    assert "Question: Where?" in prompt
    assert "Gold answer: Gold" in prompt
    assert "Candidate answer: Answer" in prompt
