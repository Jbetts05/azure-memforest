# Azure-MemForest benchmark baseline

Azure-MemForest must preserve the MemForest paper behavior while replacing the research runtime assumptions with enterprise Azure infrastructure.

## Checked-in benchmark artifacts

The repository includes the benchmark CSVs used by the research artifact:

| Benchmark | Model size | File |
| --- | --- | --- |
| LoCoMo | Qwen3-30B | `benchmark/locomo_per_question_30b.csv` |
| LoCoMo | Qwen3-4B | `benchmark/locomo_per_question_4b.csv` |
| LongMemEval-S | Qwen3-30B | `benchmark/longmemeval_per_question_30b.csv` |
| LongMemEval-S | Qwen3-4B | `benchmark/longmemeval_per_question_4b.csv` |

Expected core columns:

```text
method, model_size, question_type, qid, question, gold_answer,
answer_1, ..., answer_8,
judge_1, ..., judge_8
```

## Parity rule

Use the existing MemForest benchmark artifacts and local implementation behavior as the baseline. Do not substitute synthetic demo data for parity claims.

Before any cloud-backed store, index, or model path replaces local behavior, define and record:

- exact benchmark subset or full benchmark run;
- model and embedding deployment versions;
- retrieval metrics such as pass@1, pass@1--8, recall@k, and raw evidence counts;
- latency and cost counters for query, ingestion, summarization, and embedding;
- accepted tolerance for behavior drift.

## Current Phase 0 gate

Phase 0 only verifies that the checked-in artifacts are present and structurally usable. Full benchmark replays require configured OpenAI-compatible model endpoints and are not run by default in CI.

Run the deterministic CSV summary locally with:

```powershell
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m src.evaluation.benchmark_summary --repo-root .
```

The default table reports cumulative pass@k rollups by `method` and `model_size`, matching the README's `judge_1..judge_k` aggregation pattern. JSON output also includes per-`question_type` groups and an order-invariant unbiased pass@k value with denominators for future Azure parity analysis.

## Provenance and license

The checked-in `LICENSE` file is MIT. Preserve the upstream copyright and permission notice in copies or substantial portions of the software.
