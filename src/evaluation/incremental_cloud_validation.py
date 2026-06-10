"""Deterministic P1 validation gates for incremental cloud-state work.

The helpers in this module intentionally operate on small local fixtures or
already-materialized projections. They do not call model endpoints or run the
benchmark suite.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping


PROJECTION_SURFACES = (
    "stored_nodes",
    "fact_locations",
    "recall_documents",
    "browse_embeddings",
    "summary_cache_entries",
)


@dataclass(frozen=True)
class ProjectionWriteCounts:
    stored_nodes: int = 0
    fact_locations: int = 0
    recall_documents: int = 0
    browse_embeddings: int = 0
    summary_cache_entries: int = 0

    @property
    def total(self) -> int:
        return sum(getattr(self, surface) for surface in PROJECTION_SURFACES)

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "ProjectionWriteCounts":
        return cls(**{surface: int(values.get(surface, 0) or 0) for surface in PROJECTION_SURFACES})

    @classmethod
    def from_projection(cls, projection: Any) -> "ProjectionWriteCounts":
        values = {
            "stored_nodes": len(getattr(projection, "stored_nodes", []) or []),
            "fact_locations": len(getattr(projection, "fact_locations", []) or []),
            "recall_documents": len(getattr(projection, "recall_documents", []) or []),
            "browse_embeddings": len(getattr(projection, "browse_embeddings", {}) or {}),
            "summary_cache_entries": len(getattr(projection, "summary_cache_entries", []) or []),
        }
        return cls.from_mapping(values)

    def to_dict(self) -> dict[str, int]:
        return {surface: getattr(self, surface) for surface in PROJECTION_SURFACES} | {"total": self.total}


@dataclass(frozen=True)
class ProjectionReductionResult:
    baseline_full: ProjectionWriteCounts
    candidate_incremental: ProjectionWriteCounts
    min_total_reduction_ratio: float
    surface_reductions: dict[str, float | None]
    total_reduction_ratio: float | None
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


@dataclass(frozen=True)
class QueryParityCase:
    case_id: str
    baseline_answer: str
    candidate_answer: str
    baseline_retrieved_fact_ids: tuple[str, ...] = ()
    candidate_retrieved_fact_ids: tuple[str, ...] = ()
    baseline_recalled_tree_ids: tuple[str, ...] = ()
    candidate_recalled_tree_ids: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "QueryParityCase":
        baseline = values.get("baseline", {}) or {}
        candidate = values.get("candidate", {}) or {}
        return cls(
            case_id=str(values.get("case_id") or values.get("id") or ""),
            baseline_answer=str(baseline.get("answer", "")),
            candidate_answer=str(candidate.get("answer", "")),
            baseline_retrieved_fact_ids=tuple(str(item) for item in baseline.get("retrieved_fact_ids", []) or []),
            candidate_retrieved_fact_ids=tuple(str(item) for item in candidate.get("retrieved_fact_ids", []) or []),
            baseline_recalled_tree_ids=tuple(str(item) for item in baseline.get("recalled_tree_ids", []) or []),
            candidate_recalled_tree_ids=tuple(str(item) for item in candidate.get("recalled_tree_ids", []) or []),
        )


@dataclass(frozen=True)
class QueryParityResult:
    checked_cases: int
    failures: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.failures


@dataclass(frozen=True)
class P1ValidationResult:
    projection: ProjectionReductionResult | None
    query_parity: QueryParityResult

    @property
    def ok(self) -> bool:
        return (self.projection is None or self.projection.ok) and self.query_parity.ok


def compare_projection_write_reduction(
    *,
    baseline_full: ProjectionWriteCounts,
    candidate_incremental: ProjectionWriteCounts,
    min_total_reduction_ratio: float = 0.25,
) -> ProjectionReductionResult:
    """Gate that proves incremental projection writes fewer derived records."""

    failures: list[str] = []
    reductions: dict[str, float | None] = {}
    for surface in PROJECTION_SURFACES:
        baseline_count = getattr(baseline_full, surface)
        candidate_count = getattr(candidate_incremental, surface)
        if candidate_count > baseline_count:
            failures.append(f"{surface}:candidate_exceeds_full:{candidate_count}>{baseline_count}")
        reductions[surface] = None if baseline_count == 0 else 1.0 - (candidate_count / baseline_count)

    total_reduction = None if baseline_full.total == 0 else 1.0 - (candidate_incremental.total / baseline_full.total)
    if baseline_full.total == 0:
        if candidate_incremental.total != 0:
            failures.append("total:candidate_writes_when_full_is_zero")
    elif total_reduction is not None and total_reduction < min_total_reduction_ratio:
        failures.append(f"total:reduction_below_threshold:{total_reduction:.6f}<{min_total_reduction_ratio:.6f}")

    return ProjectionReductionResult(
        baseline_full=baseline_full,
        candidate_incremental=candidate_incremental,
        min_total_reduction_ratio=float(min_total_reduction_ratio),
        surface_reductions=reductions,
        total_reduction_ratio=total_reduction,
        failures=failures,
    )


def compare_query_parity(cases: Iterable[QueryParityCase]) -> QueryParityResult:
    """Gate exact local answer and retrieval parity for curated smoke cases."""

    failures: list[str] = []
    checked = 0
    for case in cases:
        checked += 1
        case_id = case.case_id or f"case-{checked}"
        if _normalize_answer(case.baseline_answer) != _normalize_answer(case.candidate_answer):
            failures.append(f"{case_id}:answer_mismatch")
        if case.baseline_retrieved_fact_ids != case.candidate_retrieved_fact_ids:
            failures.append(f"{case_id}:retrieved_fact_ids_mismatch")
        if case.baseline_recalled_tree_ids != case.candidate_recalled_tree_ids:
            failures.append(f"{case_id}:recalled_tree_ids_mismatch")
    return QueryParityResult(checked_cases=checked, failures=failures)


def evaluate_validation_payload(payload: Mapping[str, Any]) -> P1ValidationResult:
    projection_payload = payload.get("projection")
    projection_result = None
    if projection_payload:
        projection_result = compare_projection_write_reduction(
            baseline_full=ProjectionWriteCounts.from_mapping(projection_payload.get("baseline_full", {}) or {}),
            candidate_incremental=ProjectionWriteCounts.from_mapping(projection_payload.get("candidate_incremental", {}) or {}),
            min_total_reduction_ratio=float(projection_payload.get("min_total_reduction_ratio", 0.25)),
        )
    query_cases = [QueryParityCase.from_mapping(case) for case in payload.get("query_cases", []) or []]
    return P1ValidationResult(projection=projection_result, query_parity=compare_query_parity(query_cases))


def result_to_dict(result: P1ValidationResult) -> dict[str, Any]:
    projection = None
    if result.projection is not None:
        projection = {
            "ok": result.projection.ok,
            "baseline_full": result.projection.baseline_full.to_dict(),
            "candidate_incremental": result.projection.candidate_incremental.to_dict(),
            "min_total_reduction_ratio": result.projection.min_total_reduction_ratio,
            "surface_reductions": result.projection.surface_reductions,
            "total_reduction_ratio": result.projection.total_reduction_ratio,
            "failures": result.projection.failures,
        }
    return {
        "ok": result.ok,
        "projection": projection,
        "query_parity": {
            "ok": result.query_parity.ok,
            "checked_cases": result.query_parity.checked_cases,
            "failures": result.query_parity.failures,
        },
    }


def _normalize_answer(answer: str) -> str:
    return re.sub(r"\s+", " ", answer.casefold().strip())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run deterministic P1 incremental cloud-state validation gates.")
    parser.add_argument("fixture", type=Path, help="JSON fixture with projection counts and query parity cases.")
    args = parser.parse_args(argv)
    result = evaluate_validation_payload(json.loads(args.fixture.read_text(encoding="utf-8")))
    print(json.dumps(result_to_dict(result), indent=2, sort_keys=True))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
