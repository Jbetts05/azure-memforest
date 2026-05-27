"""Benchmark diff gate for Azure-MemForest parity checks."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from src.evaluation.benchmark_summary import (
    BenchmarkFileSummary,
    BenchmarkSuiteSummary,
    default_benchmark_paths,
    file_to_dict,
    summarize_benchmark_suite,
    suite_to_dict,
)


DEFAULT_K_VALUES = (1, 8)
DEFAULT_TOLERANCE = 1e-9


@dataclass(frozen=True)
class BenchmarkMetricDelta:
    benchmark: str
    model_size: str
    method: str
    question_type: str
    metric: str
    k: int
    baseline: float | None
    candidate: float | None
    delta: float | None
    tolerance: float
    status: str


@dataclass(frozen=True)
class BenchmarkDiffResult:
    deltas: list[BenchmarkMetricDelta] = field(default_factory=list)
    missing_baseline_keys: list[str] = field(default_factory=list)
    missing_candidate_keys: list[str] = field(default_factory=list)
    missing_candidate_files: list[str] = field(default_factory=list)

    @property
    def failed_count(self) -> int:
        return sum(1 for delta in self.deltas if delta.status == "fail") + len(self.missing_candidate_keys) + len(self.missing_candidate_files)

    @property
    def passed_count(self) -> int:
        return sum(1 for delta in self.deltas if delta.status == "pass")

    @property
    def ok(self) -> bool:
        return self.failed_count == 0


def diff_benchmark_summaries(
    baseline: BenchmarkSuiteSummary | BenchmarkFileSummary | dict[str, Any],
    candidate: BenchmarkSuiteSummary | BenchmarkFileSummary | dict[str, Any],
    *,
    metric: str = "cumulative_pass_at",
    k_values: Iterable[int] = DEFAULT_K_VALUES,
    tolerance: float = DEFAULT_TOLERANCE,
    include_question_types: bool = True,
) -> BenchmarkDiffResult:
    """Compare candidate metrics against a frozen baseline summary.

    A metric fails only when candidate - baseline is less than ``-tolerance``.
    Additive candidate-only keys are reported but do not fail the gate.
    Missing candidate keys fail because they usually indicate a partial run.
    """

    ks = tuple(int(k) for k in k_values)
    baseline_map = _metric_map(
        _summary_to_dict(baseline),
        metric=metric,
        k_values=ks,
        include_question_types=include_question_types,
    )
    candidate_map = _metric_map(
        _summary_to_dict(candidate),
        metric=metric,
        k_values=ks,
        include_question_types=include_question_types,
    )

    deltas: list[BenchmarkMetricDelta] = []
    missing_candidate_keys: list[str] = []
    for key in sorted(baseline_map):
        if key not in candidate_map:
            missing_candidate_keys.append(_key_to_string(key))
            continue
        baseline_value = baseline_map[key]
        candidate_value = candidate_map[key]
        delta = candidate_value - baseline_value
        status = "fail" if delta < -float(tolerance) else "pass"
        benchmark, model_size, method, question_type, metric_name, k = key
        deltas.append(
            BenchmarkMetricDelta(
                benchmark=benchmark,
                model_size=model_size,
                method=method,
                question_type=question_type,
                metric=metric_name,
                k=k,
                baseline=baseline_value,
                candidate=candidate_value,
                delta=delta,
                tolerance=float(tolerance),
                status=status,
            )
        )

    missing_baseline_keys = [
        _key_to_string(key)
        for key in sorted(candidate_map)
        if key not in baseline_map
    ]
    return BenchmarkDiffResult(
        deltas=deltas,
        missing_baseline_keys=missing_baseline_keys,
        missing_candidate_keys=missing_candidate_keys,
    )


def result_to_dict(result: BenchmarkDiffResult) -> dict[str, Any]:
    return {
        "ok": result.ok,
        "failed_count": result.failed_count,
        "passed_count": result.passed_count,
        "missing_baseline_keys": result.missing_baseline_keys,
        "missing_candidate_keys": result.missing_candidate_keys,
        "missing_candidate_files": result.missing_candidate_files,
        "deltas": [
            {
                "benchmark": delta.benchmark,
                "model_size": delta.model_size,
                "method": delta.method,
                "question_type": delta.question_type,
                "metric": delta.metric,
                "k": delta.k,
                "baseline": delta.baseline,
                "candidate": delta.candidate,
                "delta": delta.delta,
                "tolerance": delta.tolerance,
                "status": delta.status,
            }
            for delta in result.deltas
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare benchmark CSV results against a baseline summary.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--baseline-json", type=Path, default=None, help="Frozen baseline summary JSON.")
    parser.add_argument("--baseline-dir", type=Path, default=None, help="Directory containing baseline CSVs.")
    parser.add_argument("--candidate-dir", type=Path, required=True, help="Directory containing candidate CSVs with benchmark filenames.")
    parser.add_argument("--k", type=int, action="append", dest="k_values", help="pass@k value to compare. Repeatable. Defaults to 1 and 8.")
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE, help="Allowed absolute regression on rate metrics.")
    parser.add_argument("--rollups-only", action="store_true", help="Compare method/model rollups only, not per-question-type groups.")
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    args = parser.parse_args(argv)

    baseline = _load_baseline(args)
    candidate_paths = _paths_from_dir(args.candidate_dir)
    missing_files = [str(path) for path in candidate_paths if not path.exists()]
    if missing_files:
        result = BenchmarkDiffResult(missing_candidate_files=missing_files)
        _print_result(result, emit_json=args.json)
        return 1

    candidate = summarize_benchmark_suite(candidate_paths)
    result = diff_benchmark_summaries(
        baseline,
        candidate,
        k_values=args.k_values or list(DEFAULT_K_VALUES),
        tolerance=args.tolerance,
        include_question_types=not args.rollups_only,
    )
    _print_result(result, emit_json=args.json)
    return 0 if result.ok else 1


def _load_baseline(args: argparse.Namespace) -> dict[str, Any] | BenchmarkSuiteSummary:
    if args.baseline_json is not None:
        return json.loads(args.baseline_json.read_text(encoding="utf-8"))
    if args.baseline_dir is not None:
        return summarize_benchmark_suite(_paths_from_dir(args.baseline_dir))
    snapshot = args.repo_root / "benchmark" / "baseline_summary.json"
    if snapshot.exists():
        return json.loads(snapshot.read_text(encoding="utf-8"))
    return summarize_benchmark_suite(default_benchmark_paths(args.repo_root))


def _paths_from_dir(directory: Path) -> list[Path]:
    return [directory / path.name for path in default_benchmark_paths(Path("."))]


def _summary_to_dict(summary: BenchmarkSuiteSummary | BenchmarkFileSummary | dict[str, Any]) -> dict[str, Any]:
    if isinstance(summary, BenchmarkSuiteSummary):
        return suite_to_dict(summary)
    if isinstance(summary, BenchmarkFileSummary):
        return {"total_rows": summary.total_rows, "files": [file_to_dict(summary)]}
    return summary


def _metric_map(
    summary: dict[str, Any],
    *,
    metric: str,
    k_values: tuple[int, ...],
    include_question_types: bool,
) -> dict[tuple[str, str, str, str, str, int], float]:
    out: dict[tuple[str, str, str, str, str, int], float] = {}
    for file_summary in summary.get("files", []):
        groups = list(file_summary.get("method_rollups", []))
        if include_question_types:
            groups.extend(file_summary.get("groups", []))
        for group in groups:
            metrics = group.get(metric)
            if not isinstance(metrics, dict):
                raise ValueError(f"Metric {metric!r} missing for group {group!r}")
            for k in k_values:
                key = str(k)
                if key not in metrics or metrics[key] is None:
                    raise ValueError(
                        f"Requested {metric}@{k} is missing for "
                        f"{group.get('benchmark')}/{group.get('model_size')}/"
                        f"{group.get('method')}/{group.get('question_type')}"
                    )
                out[
                    (
                        str(group.get("benchmark", "")),
                        str(group.get("model_size", "")),
                        str(group.get("method", "")),
                        str(group.get("question_type", "")),
                        metric,
                        k,
                    )
                ] = float(metrics[key])
    return out


def _key_to_string(key: tuple[str, str, str, str, str, int]) -> str:
    benchmark, model_size, method, question_type, metric, k = key
    return f"{benchmark}/{model_size}/{method}/{question_type}/{metric}@{k}"


def _print_result(result: BenchmarkDiffResult, *, emit_json: bool) -> None:
    if emit_json:
        print(json.dumps(result_to_dict(result), indent=2, sort_keys=True))
        return
    print("benchmark,model_size,method,question_type,metric,k,baseline,candidate,delta,tolerance,status")
    for delta in result.deltas:
        print(
            f"{delta.benchmark},{delta.model_size},{delta.method},{delta.question_type},"
            f"{delta.metric},{delta.k},{delta.baseline:.6f},{delta.candidate:.6f},"
            f"{delta.delta:.6f},{delta.tolerance:.6f},{delta.status}"
        )
    for key in result.missing_candidate_keys:
        print(f"missing_candidate_key,{key}")
    for key in result.missing_baseline_keys:
        print(f"missing_baseline_key,{key}")
    for path in result.missing_candidate_files:
        print(f"missing_candidate_file,{path}")


if __name__ == "__main__":
    raise SystemExit(main())
