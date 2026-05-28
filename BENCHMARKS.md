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

## Baseline capture protocol

The frozen paper baseline is the checked-in benchmark CSV set plus `benchmark/baseline_summary.json`. Do not regenerate `baseline_summary.json` unless intentionally updating the paper baseline and reviewing the resulting pass@k changes.

For every local or cloud capture, write run artifacts under:

```text
benchmark/baseline-captures/<yyyy-mm-dd>-<git-sha>/
```

This directory is ignored by git by default. Do not commit generated capture outputs unless explicitly approved; they can be large and may contain model answers, judge outputs, or trace-derived diagnostics.

Each capture must include a small manifest alongside the generated CSVs:

- git SHA and container image tag or digest, when applicable;
- input benchmark dataset source and checksums for the actual question/gold files used by the replay;
- checked-in scoring CSV filenames and checksums when they are used as the baseline scoring artifact;
- `baseline_summary.json` checksum;
- config files, query configuration hash, prompt/config snapshot hash, and random seed plan;
- system-under-test model deployments and versions;
- separate judge model deployment and version;
- embedding deployment, model version, dimension, batching policy, and retry policy;
- recall/search configuration, including ANN settings, tie-break rules, and top-k values;
- worker concurrency settings that can affect chunk/order assembly;
- output CSV filenames, row counts, method labels, and method-to-baseline mapping;
- summary JSON, diff command, tolerance, and pass/fail status.

Candidate cloud outputs must use the same filenames and columns as the checked-in per-question CSVs:

```text
method, model_size, question_type, qid, question, gold_answer,
answer_1, ..., answer_8,
judge_1, ..., judge_8
```

`benchmark_diff` compares pass-rate deltas over the summarized rows; it is not a per-question equality check. The identity tolerance `0.000000001` is only for re-summarizing the same checked-in CSVs or proving scoring code did not drift. Cloud parity tolerance is blocked until a judge-variance study is recorded by replaying the frozen answers through the pinned judge deployment and setting a tolerance at or above observed judge variance.

The cloud replay harness must declare which baseline methods it is attempting to cover. Candidate-only method names are not sufficient for parity claims; the capture is invalid if no declared baseline method is exercised.

Partial cloud outputs must use a `.partial` suffix and must not be passed to the diff gate. A parity capture is valid only when expected row counts match the source fixture manifest.

Cloud parity replay is gated on:

1. materialization worker implementation;
2. hosted worker receive loop;
3. committed manifests for replay users;
4. cloud query recall, browse, rerank, and compose implementation;
5. one-session ingest smoke success with no DLQ growth.

Do not upload or share new benchmark answers, cloud traces, or replay artifacts outside the repository workspace and the approved Azure tenant. The checked-in paper CSVs are already part of this repository; the additional restriction applies to newly generated cloud-tenant outputs and traces.

## Judge variance study

Before approving a non-zero cloud parity tolerance, prepare judge samples and measure label variance:

```powershell
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m src.evaluation.judge_variance --repo-root . --samples-json benchmark\baseline-captures\judge-samples.json
```

The command reports existing disagreement across the checked-in `judge_1..judge_8` labels and writes sampled prompts for replay through the pinned judge deployment. The existing-label result is not a substitute for the deployment replay, but it sets a lower-bound sanity check for the final approved tolerance.

## Cloud parity policy

Cloud parity claims must be made against a predeclared policy, not after seeing replay results.

Current frozen-label variance baseline:

- `answer_label_count`: `238328`
- `row_count`: `29791`
- `answer_label_disagreement_rate`: `0.041719814709140345`
- `rows_with_disagreement`: `4871`

Policy for the first cloud replay:

- **Judge deployment replay gate:** replay the sampled judge prompts through the pinned judge deployment before cloud benchmark replay. The first approved-prompt replay produced `0.205` disagreement against the frozen labels, so the frozen labels must not be treated as same-judge ground truth for this approved prompt. Cloud parity must compare local and cloud candidate outputs judged by the same approved prompt/deployment.
- **Benchmark pass-rate gate:** cloud candidate `cumulative_pass_at@1` and `cumulative_pass_at@8` must not regress by more than `0.05` absolute against a local baseline regenerated with the approved judge prompt/deployment for method/model rollups or per-question-type groups.
- **Candidate-shape gate:** candidate CSVs must use the checked-in benchmark filenames and columns; candidate-only methods fail under `--strict-cloud-parity`.
- **Rebaseline triggers:** any change to embedding deployment/model/version/dimension, chat/judge deployment/model/version, prompt templates, tree build configuration, extraction configuration, or materialization fingerprint format requires a new baseline capture and policy review before parity claims.
- **Failure action:** any failed gate blocks production-readiness claims. It does not block development deployments marked as non-production smoke environments.

The policy should be tightened after deterministic judge behavior is available or after a larger judge replay establishes a narrower confidence interval.

### Current judge replay status

An initial 20-sample replay through `gpt-4-1-mini-dev` produced:

- `valid_count`: `20`
- `disagreement_count`: `7`
- `disagreement_rate`: `0.35`
- `invalid_count`: `0`

The full approved-prompt 200-sample replay through `gpt-4-1-mini-dev` produced:

- `valid_count`: `200`
- `disagreement_count`: `41`
- `disagreement_rate`: `0.205`
- `invalid_count`: `0`

This confirms the approved prompt is not directly comparable to the frozen labels at the prior `0.05` gate. Cloud parity must therefore compare local and cloud candidates under the same approved judge prompt/deployment.

The original judge prompt used to create the checked-in `judge_1..judge_8` labels is not present in this repository snapshot. The approved calibration path for this repository is to use the judge prompt in `src/evaluation/judge_variance.py` and treat deployment replay disagreement against the frozen labels as a policy calibration signal, not as proof of original-judge reproducibility.

## Cloud replay plan

The first cloud replay must run as a staged, resumable workflow:

1. **Fixture selection:** use the checked-in benchmark CSVs as the source of questions/gold answers and retain their `method`, `model_size`, `question_type`, and `qid` keys.
2. **Local baseline artifact:** use the latest `benchmark/baseline-captures/local-*` capture as the deterministic scoring-code baseline.
3. **Cloud ingest:** for each benchmark subject/session fixture, submit ordered Service Bus jobs through the API or worker command surface and wait for committed manifests with snapshot hashes.
4. **Cloud query:** issue cloud query requests with the same question order, `top_k`, and `max_facts` policy; persist returned answer, facts, trace id, forest version, and manifest fingerprint.
5. **Judge replay:** judge local and cloud candidate answers with the same approved prompt/deployment in `src/evaluation/judge_variance.py`; do not compare new-prompt labels directly to the frozen labels except as a calibration diagnostic.
6. **Candidate CSV:** emit per-question CSVs with the existing benchmark columns and cloud method labels mapped explicitly to the baseline method labels.
7. **Diff gate:** run `benchmark_diff --strict-cloud-parity` over the candidate directory and fail on missing baseline keys, candidate-only keys, baseline checksum mismatch, or pass-rate regressions above policy tolerance.
8. **Audit bundle:** keep manifest fingerprints, model deployment versions, judge replay outputs, trace ids, queue/DLQ counts, and cost/latency counters in the ignored capture directory.

Cloud replay is not a production-readiness claim until every stage has a complete artifact and the policy gate passes.

## Current Phase 0 gate

Phase 0 only verifies that the checked-in artifacts are present and structurally usable. Full benchmark replays require configured OpenAI-compatible model endpoints and are not run by default in CI.

Run the deterministic CSV summary locally with:

```powershell
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m src.evaluation.benchmark_summary --repo-root .
```

The default table reports cumulative pass@k rollups by `method` and `model_size`, matching the README's `judge_1..judge_k` aggregation pattern. JSON output also includes per-`question_type` groups and an order-invariant unbiased pass@k value with denominators for future Azure parity analysis.

Run the parity diff gate with:

```powershell
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m src.evaluation.benchmark_diff --repo-root . --candidate-dir benchmark --k 1 --k 8 --tolerance 0.000000001
```

By default the gate compares candidate CSVs with the frozen `benchmark/baseline_summary.json` snapshot, checks pass@1 and pass@8, includes per-question-type rows, and fails only when a candidate regresses beyond tolerance. Candidate-only methods are reported; missing candidate files or baseline keys fail.

Regenerate the frozen snapshot only when intentionally updating the paper baseline:

```powershell
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m src.evaluation.benchmark_summary --repo-root . --json > benchmark\baseline_summary.json
```

## Provenance and license

The checked-in `LICENSE` file is MIT. Preserve the upstream copyright and permission notice in copies or substantial portions of the software.
