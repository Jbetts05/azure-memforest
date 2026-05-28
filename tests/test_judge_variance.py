from __future__ import annotations

from pathlib import Path

from src.evaluation.judge_variance import (
    build_judge_user_prompt,
    compute_existing_label_variance,
    replay_samples_with_client,
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


def test_replay_samples_with_client_counts_disagreement() -> None:
    class FakeChoices:
        def __init__(self):
            self.choices = [type("Choice", (), {"message": type("Message", (), {"content": '{"correct": false}'})()})]

    class FakeCompletions:
        def create(self, **kwargs):
            return FakeChoices()

    class FakeClient:
        chat = type("Chat", (), {"completions": FakeCompletions()})()

    samples = [
        {
            "source_file": "bench.csv",
            "row_index": 0,
            "method": "m",
            "model_size": "4b",
            "question_type": "type",
            "qid": "q1",
            "answer_column": "answer_1",
            "judge_column": "judge_1",
            "original_judge": True,
            "judge_prompt": {"system": "s", "user": "u"},
        }
    ]

    result, rows = replay_samples_with_client(samples, FakeClient(), deployment="judge")

    assert result.valid_count == 1
    assert result.disagreement_count == 1
    assert result.disagreement_rate == 1.0
    assert rows[0]["deployed_judge"] is False
