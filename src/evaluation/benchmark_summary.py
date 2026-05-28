"""Streaming summaries for checked-in benchmark result CSVs.

The research artifact reports pass@1 and pass@1--8 from answer/judge columns.
This module recomputes those deterministic summaries without model calls.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


TRUTHY_JUDGE_VALUES = {"1", "true", "yes", "correct"}
FALSEY_JUDGE_VALUES = {"0", "false", "no", "wrong", "incorrect"}


@dataclass(frozen=True)
class BenchmarkGroupSummary:
    """Metrics for one method/model/question-type group."""

    benchmark: str
    model_size: str
    method: str
    question_type: str
    total_rows: int
    cumulative_pass_at: dict[int, float]
    unbiased_pass_at: dict[int, float | None]
    unbiased_denominators: dict[int, int]
    correct_counts: dict[int, int]
    missing_or_invalid_judges: int = 0


@dataclass(frozen=True)
class BenchmarkFileSummary:
    """Summary for one benchmark CSV file."""

    path: str
    benchmark: str
    model_sizes: list[str]
    methods: list[str]
    question_types: list[str]
    judge_columns: list[str]
    total_rows: int
    judge_values: list[str]
    missing_or_invalid_judges: int
    sha256: str
    method_rollups: list[BenchmarkGroupSummary] = field(default_factory=list)
    groups: list[BenchmarkGroupSummary] = field(default_factory=list)


@dataclass(frozen=True)
class BenchmarkSuiteSummary:
    """Summary for a collection of benchmark files."""

    total_rows: int
    files: list[BenchmarkFileSummary] = field(default_factory=list)


def default_benchmark_paths(repo_root: Path) -> list[Path]:
    """Return the benchmark CSV paths shipped with the artifact."""

    benchmark_dir = repo_root / "benchmark"
    return [
        benchmark_dir / "locomo_per_question_30b.csv",
        benchmark_dir / "locomo_per_question_4b.csv",
        benchmark_dir / "longmemeval_per_question_30b.csv",
        benchmark_dir / "longmemeval_per_question_4b.csv",
    ]


def summarize_benchmark_suite(paths: Iterable[Path]) -> BenchmarkSuiteSummary:
    """Summarize multiple benchmark CSV files."""

    files = [summarize_benchmark_file(path) for path in paths]
    return BenchmarkSuiteSummary(
        total_rows=sum(file.total_rows for file in files),
        files=files,
    )


def summarize_benchmark_file(path: Path) -> BenchmarkFileSummary:
    """Summarize one benchmark CSV in a streaming pass."""

    path = Path(path)
    benchmark = _benchmark_name_from_path(path)
    group_accumulators: dict[tuple[str, str, str], _GroupAccumulator] = {}
    model_sizes: set[str] = set()
    methods: set[str] = set()
    question_types: set[str] = set()
    judge_values: set[str] = set()
    total_rows = 0
    missing_or_invalid_judges = 0

    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"Benchmark CSV has no header: {path}")
        judge_columns = _judge_columns(reader.fieldnames)
        if not judge_columns:
            raise ValueError(f"Benchmark CSV has no judge columns: {path}")

        for row in reader:
            total_rows += 1
            method = (row.get("method") or "").strip()
            model_size = (row.get("model_size") or "").strip()
            question_type = (row.get("question_type") or "").strip()
            methods.add(method)
            model_sizes.add(model_size)
            question_types.add(question_type)

            parsed_judges: list[bool | None] = []
            for column in judge_columns:
                raw_value = (row.get(column) or "").strip()
                judge_values.add(raw_value)
                parsed = parse_judge_value(raw_value)
                if parsed is None:
                    missing_or_invalid_judges += 1
                parsed_judges.append(parsed)

            key = (method, model_size, question_type)
            accumulator = group_accumulators.setdefault(
                key,
                _GroupAccumulator(
                    benchmark=benchmark,
                    method=method,
                    model_size=model_size,
                    question_type=question_type,
                    max_k=len(judge_columns),
                ),
            )
            accumulator.add(parsed_judges)

    groups = [
        accumulator.to_summary()
        for _, accumulator in sorted(group_accumulators.items(), key=lambda item: item[0])
    ]
    method_rollups = _rollup_by_method(groups)
    return BenchmarkFileSummary(
        path=path.as_posix(),
        benchmark=benchmark,
        model_sizes=sorted(model_sizes),
        methods=sorted(methods),
        question_types=sorted(question_types),
        judge_columns=judge_columns,
        total_rows=total_rows,
        judge_values=sorted(judge_values),
        missing_or_invalid_judges=missing_or_invalid_judges,
        sha256=_normalized_file_sha256(path),
        method_rollups=method_rollups,
        groups=groups,
    )


def parse_judge_value(value: str) -> bool | None:
    """Parse a judge value into pass/fail/unknown."""

    normalized = str(value or "").strip().lower()
    if normalized in TRUTHY_JUDGE_VALUES:
        return True
    if normalized in FALSEY_JUDGE_VALUES:
        return False
    return None


def suite_to_dict(summary: BenchmarkSuiteSummary) -> dict:
    """Convert a suite summary to a JSON-serializable dictionary."""

    return {
        "total_rows": summary.total_rows,
        "files": [file_to_dict(file) for file in summary.files],
    }


def file_to_dict(summary: BenchmarkFileSummary) -> dict:
    return {
        "path": summary.path,
        "benchmark": summary.benchmark,
        "model_sizes": summary.model_sizes,
        "methods": summary.methods,
        "question_types": summary.question_types,
        "judge_columns": summary.judge_columns,
        "total_rows": summary.total_rows,
        "judge_values": summary.judge_values,
        "missing_or_invalid_judges": summary.missing_or_invalid_judges,
        "sha256": summary.sha256,
        "method_rollups": [group_to_dict(group) for group in summary.method_rollups],
        "groups": [group_to_dict(group) for group in summary.groups],
    }


def group_to_dict(summary: BenchmarkGroupSummary) -> dict:
    return {
        "benchmark": summary.benchmark,
        "model_size": summary.model_size,
        "method": summary.method,
        "question_type": summary.question_type,
        "total_rows": summary.total_rows,
        "cumulative_pass_at": _stringify_keys(summary.cumulative_pass_at),
        "unbiased_pass_at": _stringify_keys(summary.unbiased_pass_at),
        "unbiased_denominators": _stringify_keys(summary.unbiased_denominators),
        "correct_counts": _stringify_keys(summary.correct_counts),
        "missing_or_invalid_judges": summary.missing_or_invalid_judges,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize MemForest benchmark CSV results.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a compact table.")
    parser.add_argument("paths", nargs="*", type=Path, help="Optional benchmark CSV paths.")
    args = parser.parse_args(argv)

    paths = args.paths or default_benchmark_paths(args.repo_root)
    summary = summarize_benchmark_suite(paths)

    if args.json:
        print(json.dumps(suite_to_dict(summary), indent=2, sort_keys=True))
    else:
        _print_table(summary)
    return 0


class _GroupAccumulator:
    def __init__(
        self,
        *,
        benchmark: str,
        method: str,
        model_size: str,
        question_type: str,
        max_k: int,
    ) -> None:
        self.benchmark = benchmark
        self.method = method
        self.model_size = model_size
        self.question_type = question_type
        self.max_k = max_k
        self.total_rows = 0
        self.cumulative_hits: dict[int, int] = defaultdict(int)
        self.correct_counts: dict[int, int] = defaultdict(int)
        self.unbiased_sums: dict[int, float] = defaultdict(float)
        self.unbiased_denominators: dict[int, int] = defaultdict(int)
        self.missing_or_invalid_judges = 0

    def add(self, judges: list[bool | None]) -> None:
        self.total_rows += 1
        valid_judges = [judge for judge in judges if judge is not None]
        correct_count = sum(1 for judge in valid_judges if judge)
        for judge in judges:
            if judge is None:
                self.missing_or_invalid_judges += 1

        seen_correct = False
        for k in range(1, self.max_k + 1):
            prefix = judges[:k]
            if any(judge is True for judge in prefix):
                seen_correct = True
            if seen_correct:
                self.cumulative_hits[k] += 1
            if k <= len(valid_judges):
                self.unbiased_sums[k] += _unbiased_pass_at(
                    sample_count=len(valid_judges),
                    correct_count=correct_count,
                    k=k,
                )
                self.unbiased_denominators[k] += 1
            if k <= len(judges) and judges[k - 1] is True:
                self.correct_counts[k] += 1

    def to_summary(self) -> BenchmarkGroupSummary:
        return BenchmarkGroupSummary(
            benchmark=self.benchmark,
            model_size=self.model_size,
            method=self.method,
            question_type=self.question_type,
            total_rows=self.total_rows,
            cumulative_pass_at={
                k: self.cumulative_hits[k] / self.total_rows
                for k in range(1, self.max_k + 1)
            },
            unbiased_pass_at={
                k: (
                    self.unbiased_sums[k] / self.unbiased_denominators[k]
                    if self.unbiased_denominators[k]
                    else None
                )
                for k in range(1, self.max_k + 1)
            },
            unbiased_denominators={
                k: self.unbiased_denominators[k] for k in range(1, self.max_k + 1)
            },
            correct_counts={k: self.correct_counts[k] for k in range(1, self.max_k + 1)},
            missing_or_invalid_judges=self.missing_or_invalid_judges,
        )


def _judge_columns(fieldnames: list[str]) -> list[str]:
    columns = [name for name in fieldnames if name.startswith("judge_")]
    return sorted(columns, key=lambda name: int(name.split("_", 1)[1]))


def _benchmark_name_from_path(path: Path) -> str:
    name = path.stem.lower()
    if name.startswith("longmemeval"):
        return "longmemeval"
    if name.startswith("locomo"):
        return "locomo"
    return path.stem


def _normalized_file_sha256(path: Path) -> str:
    content = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(content).hexdigest()


def _unbiased_pass_at(*, sample_count: int, correct_count: int, k: int) -> float:
    if correct_count <= 0:
        return 0.0
    if sample_count - correct_count < k:
        return 1.0
    return 1.0 - math.comb(sample_count - correct_count, k) / math.comb(sample_count, k)


def _rollup_by_method(groups: list[BenchmarkGroupSummary]) -> list[BenchmarkGroupSummary]:
    grouped: dict[tuple[str, str, str], list[BenchmarkGroupSummary]] = defaultdict(list)
    for group in groups:
        grouped[(group.method, group.model_size, group.benchmark)].append(group)

    rollups: list[BenchmarkGroupSummary] = []
    for (method, model_size, benchmark), parts in sorted(grouped.items()):
        total_rows = sum(part.total_rows for part in parts)
        all_ks = sorted(parts[0].cumulative_pass_at)
        unbiased_denominators = {
            k: sum(part.unbiased_denominators.get(k, 0) for part in parts)
            for k in all_ks
        }
        rollups.append(
            BenchmarkGroupSummary(
                benchmark=benchmark,
                model_size=model_size,
                method=method,
                question_type="ALL",
                total_rows=total_rows,
                cumulative_pass_at={
                    k: sum(part.cumulative_pass_at[k] * part.total_rows for part in parts) / total_rows
                    for k in all_ks
                },
                unbiased_pass_at={
                    k: (
                        sum(
                            (part.unbiased_pass_at[k] or 0.0)
                            * part.unbiased_denominators.get(k, 0)
                            for part in parts
                        )
                        / unbiased_denominators[k]
                        if unbiased_denominators[k]
                        else None
                    )
                    for k in all_ks
                },
                unbiased_denominators=unbiased_denominators,
                correct_counts={
                    k: sum(part.correct_counts.get(k, 0) for part in parts)
                    for k in all_ks
                },
                missing_or_invalid_judges=sum(part.missing_or_invalid_judges for part in parts),
            )
        )
    return rollups


def _stringify_keys(values: dict[int, float | int | None]) -> dict[str, float | int | None]:
    return {str(key): value for key, value in values.items()}


def _print_table(summary: BenchmarkSuiteSummary) -> None:
    print("benchmark,model_size,method,question_type,rows,cumulative_pass@1,cumulative_pass@8")
    for file_summary in summary.files:
        for group in file_summary.method_rollups:
            pass_at_1 = group.cumulative_pass_at.get(1, 0.0)
            pass_at_8 = group.cumulative_pass_at.get(8, 0.0)
            print(
                f"{group.benchmark},{group.model_size},{group.method},{group.question_type},"
                f"{group.total_rows},{pass_at_1:.4f},{pass_at_8:.4f}"
            )


if __name__ == "__main__":
    raise SystemExit(main())
