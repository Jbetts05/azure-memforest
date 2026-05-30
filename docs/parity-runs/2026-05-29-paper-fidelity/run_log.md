# Parity run log

Run date: 2026-05-29

## Goal

Assess how closely Azure-MemForest matches the local MemForest implementation and checked-in benchmark artifacts, while avoiding overclaiming formal paper reproduction.

## Todo slices

| Slice | Status | Evidence |
| --- | --- | --- |
| Define goal and scoring dimensions | Done | `assessment.md` Scope, Goal, and Feature fidelity matrix. |
| Extract local MemForest feature inventory | Done | Parallel code-reading subagent; summarized in `assessment.md`. |
| Map Azure implementation to local features | Done | Parallel Azure-mapping subagent; summarized in `assessment.md`. |
| Inventory benchmark/eval tooling | Done | Parallel parity-test subagent; benchmark files and tests identified. |
| Run deterministic benchmark summary/diff | Done | `benchmark_summary_current.json`, `benchmark_diff_vs_frozen_baseline.json`. |
| Run targeted fidelity tests | Done | `targeted_tests.log`; 64 tests passed. |
| Rubber-duck critique | Done | See `rubber_duck_validation.md`. |
| Final report | Done | `assessment.md`. |

## Commands

```powershell
python -m src.evaluation.benchmark_summary --repo-root . --json > docs\parity-runs\2026-05-29-paper-fidelity\benchmark_summary_current.json
python -m src.evaluation.benchmark_diff --repo-root . --candidate-dir benchmark --json > docs\parity-runs\2026-05-29-paper-fidelity\benchmark_diff_vs_frozen_baseline.json
python -m pytest tests/test_cloud_query_service.py tests/test_worker_materialization.py tests/test_snapshot_hydration.py tests/test_cosmos_facts.py tests/test_cosmos_source.py tests/test_query_tracing.py tests/test_benchmark_diff.py tests/test_phase0_scaffold.py tests/test_memory_quality_prompts.py tests/test_entity_user_profile_label.py tests/test_ui_read_service.py -q
```

## Outcome

- Benchmark diff stability check passed.
- Targeted tests passed.
- Report explicitly scopes the result as local implementation to Azure implementation fidelity, not a formal paper reproduction proof.
