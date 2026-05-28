"""Judge variance tooling for benchmark parity gates."""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from src.evaluation.benchmark_summary import default_benchmark_paths, parse_judge_value

JUDGE_SYSTEM_PROMPT = (
    "You are an impartial answer judge. Decide whether the candidate answer is "
    "semantically correct for the question given the gold answer. Ignore formatting "
    "and extra reasoning. Return JSON only: {\"correct\": true|false, \"reason\": \"short rationale\"}."
)


@dataclass(frozen=True)
class JudgeSample:
    source_file: str
    row_index: int
    method: str
    model_size: str
    question_type: str
    qid: str
    question: str
    gold_answer: str
    answer_column: str
    judge_column: str
    answer: str
    original_judge: bool | None


@dataclass(frozen=True)
class ExistingLabelVariance:
    row_count: int
    answer_label_count: int
    rows_with_disagreement: int
    answer_label_disagreement_rate: float
    mean_row_minority_rate: float
    max_row_minority_rate: float
    recommended_min_tolerance: float


def sample_judged_answers(
    paths: Iterable[Path],
    *,
    max_samples: int = 200,
    seed: int = 13,
) -> list[JudgeSample]:
    samples: list[JudgeSample] = []
    for path in paths:
        with Path(path).open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                continue
            judge_columns = sorted(
                [name for name in reader.fieldnames if name.startswith("judge_")],
                key=lambda name: int(name.split("_", 1)[1]),
            )
            for row_index, row in enumerate(reader):
                for judge_column in judge_columns:
                    suffix = judge_column.split("_", 1)[1]
                    answer_column = f"answer_{suffix}"
                    answer = str(row.get(answer_column) or "").strip()
                    if not answer:
                        continue
                    samples.append(
                        JudgeSample(
                            source_file=Path(path).as_posix(),
                            row_index=row_index,
                            method=str(row.get("method") or ""),
                            model_size=str(row.get("model_size") or ""),
                            question_type=str(row.get("question_type") or ""),
                            qid=str(row.get("qid") or ""),
                            question=str(row.get("question") or ""),
                            gold_answer=str(row.get("gold_answer") or ""),
                            answer_column=answer_column,
                            judge_column=judge_column,
                            answer=answer,
                            original_judge=parse_judge_value(str(row.get(judge_column) or "")),
                        )
                    )
    random.Random(seed).shuffle(samples)
    return samples[:max_samples]


def compute_existing_label_variance(paths: Iterable[Path]) -> ExistingLabelVariance:
    row_count = 0
    answer_label_count = 0
    rows_with_disagreement = 0
    total_minority = 0.0
    max_minority = 0.0
    disagreement_labels = 0
    for path in paths:
        with Path(path).open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                continue
            judge_columns = sorted(
                [name for name in reader.fieldnames if name.startswith("judge_")],
                key=lambda name: int(name.split("_", 1)[1]),
            )
            for row in reader:
                labels = [
                    parsed
                    for column in judge_columns
                    if (parsed := parse_judge_value(str(row.get(column) or ""))) is not None
                ]
                if not labels:
                    continue
                row_count += 1
                answer_label_count += len(labels)
                positives = sum(1 for value in labels if value)
                negatives = len(labels) - positives
                minority = min(positives, negatives)
                if minority:
                    rows_with_disagreement += 1
                    disagreement_labels += minority
                minority_rate = minority / len(labels)
                total_minority += minority_rate
                max_minority = max(max_minority, minority_rate)
    answer_label_disagreement_rate = disagreement_labels / answer_label_count if answer_label_count else 0.0
    mean_row_minority_rate = total_minority / row_count if row_count else 0.0
    return ExistingLabelVariance(
        row_count=row_count,
        answer_label_count=answer_label_count,
        rows_with_disagreement=rows_with_disagreement,
        answer_label_disagreement_rate=answer_label_disagreement_rate,
        mean_row_minority_rate=mean_row_minority_rate,
        max_row_minority_rate=max_minority,
        recommended_min_tolerance=answer_label_disagreement_rate,
    )


def build_judge_user_prompt(sample: JudgeSample) -> str:
    return (
        f"Question: {sample.question}\n\n"
        f"Gold answer: {sample.gold_answer}\n\n"
        f"Candidate answer: {sample.answer}\n"
    )


def variance_to_dict(value: ExistingLabelVariance) -> dict[str, Any]:
    return {
        "row_count": value.row_count,
        "answer_label_count": value.answer_label_count,
        "rows_with_disagreement": value.rows_with_disagreement,
        "answer_label_disagreement_rate": value.answer_label_disagreement_rate,
        "mean_row_minority_rate": value.mean_row_minority_rate,
        "max_row_minority_rate": value.max_row_minority_rate,
        "recommended_min_tolerance": value.recommended_min_tolerance,
    }


def sample_to_dict(sample: JudgeSample) -> dict[str, Any]:
    return {
        "source_file": sample.source_file,
        "row_index": sample.row_index,
        "method": sample.method,
        "model_size": sample.model_size,
        "question_type": sample.question_type,
        "qid": sample.qid,
        "answer_column": sample.answer_column,
        "judge_column": sample.judge_column,
        "original_judge": sample.original_judge,
        "judge_prompt": {
            "system": JUDGE_SYSTEM_PROMPT,
            "user": build_judge_user_prompt(sample),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Measure or prepare MemForest judge variance studies.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--max-samples", type=int, default=200)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--samples-json", type=Path, default=None, help="Optional path to write sampled judge prompts.")
    parser.add_argument("paths", nargs="*", type=Path)
    args = parser.parse_args(argv)

    paths = args.paths or default_benchmark_paths(args.repo_root)
    variance = compute_existing_label_variance(paths)
    result = {
        "mode": "existing_label_disagreement",
        "variance": variance_to_dict(variance),
        "sample_count": int(args.max_samples),
        "seed": int(args.seed),
    }
    if args.samples_json is not None:
        samples = sample_judged_answers(paths, max_samples=args.max_samples, seed=args.seed)
        args.samples_json.parent.mkdir(parents=True, exist_ok=True)
        args.samples_json.write_text(
            json.dumps([sample_to_dict(sample) for sample in samples], indent=2, sort_keys=True),
            encoding="utf-8",
        )
        result["samples_json"] = str(args.samples_json)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
