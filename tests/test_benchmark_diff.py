from __future__ import annotations

from src.evaluation.benchmark_diff import _baseline_checksum_failures, diff_benchmark_summaries
from src.evaluation.benchmark_summary import _normalized_file_sha256


def _summary(method: str = "memforest", score: float = 1.0) -> dict:
    return {
        "total_rows": 1,
        "files": [
            {
                "path": "benchmark/example.csv",
                "benchmark": "locomo",
                "method_rollups": [
                    {
                        "benchmark": "locomo",
                        "model_size": "4b",
                        "method": method,
                        "question_type": "ALL",
                        "cumulative_pass_at": {"1": score, "8": score},
                    }
                ],
                "groups": [],
            }
        ],
    }


def test_diff_allows_candidate_only_keys_by_default() -> None:
    baseline = _summary(method="memforest")
    candidate = _summary(method="memforest")
    candidate["files"][0]["method_rollups"].append(
        {
            "benchmark": "locomo",
            "model_size": "4b",
            "method": "cloud-extra",
            "question_type": "ALL",
            "cumulative_pass_at": {"1": 1.0, "8": 1.0},
        }
    )

    result = diff_benchmark_summaries(baseline, candidate, include_question_types=False)

    assert result.ok is True
    assert result.missing_baseline_keys


def test_diff_can_fail_candidate_only_keys() -> None:
    baseline = _summary(method="memforest")
    candidate = _summary(method="memforest")
    candidate["files"][0]["method_rollups"].append(
        {
            "benchmark": "locomo",
            "model_size": "4b",
            "method": "cloud-extra",
            "question_type": "ALL",
            "cumulative_pass_at": {"1": 1.0, "8": 1.0},
        }
    )

    result = diff_benchmark_summaries(
        baseline,
        candidate,
        include_question_types=False,
        fail_on_candidate_only=True,
    )

    assert result.ok is False
    assert result.failed_count == 2


def test_baseline_checksum_verification_passes_for_matching_file(tmp_path) -> None:
    benchmark_dir = tmp_path / "benchmark"
    benchmark_dir.mkdir()
    path = benchmark_dir / "example.csv"
    path.write_text("a,b\r\n1,2\r\n", encoding="utf-8")
    summary = {"files": [{"path": "benchmark/example.csv", "sha256": _normalized_file_sha256(path)}]}

    assert _baseline_checksum_failures(summary, repo_root=tmp_path) == []


def test_baseline_checksum_verification_fails_for_mismatch(tmp_path) -> None:
    benchmark_dir = tmp_path / "benchmark"
    benchmark_dir.mkdir()
    path = benchmark_dir / "example.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    summary = {"files": [{"path": "benchmark/example.csv", "sha256": "not-the-hash"}]}

    assert _baseline_checksum_failures(summary, repo_root=tmp_path) == ["benchmark/example.csv:sha256_mismatch"]
