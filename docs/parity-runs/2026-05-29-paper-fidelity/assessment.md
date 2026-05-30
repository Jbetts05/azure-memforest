# Azure-MemForest fidelity and parity assessment

Run date: 2026-05-29

## Scope

This run assesses **local MemForest implementation to Azure-MemForest implementation fidelity**. It is not a formal paper reproduction proof because this run did not extract an atomic claim registry directly from the MemForest paper PDF. The assessment uses the checked-in README, local implementation, Azure implementation, test suite, benchmark CSVs, and deterministic benchmark tooling.

## Goal

Evaluate whether Azure-MemForest preserves the important MemForest behavior contract:

- extracted canonical facts from ordered conversation turns
- session/entity/scene MemTrees
- temporal provenance and committed memory generations
- incremental ingest through the local core
- tree-centric recall, browse, fact loading, rerank, and answer generation
- durable cloud persistence and explainability traces
- benchmark tooling parity against frozen checked-in results

## Evidence produced in this run

Artifacts in this folder:

| File | Purpose |
| --- | --- |
| `benchmark_summary_current.json` | Deterministic summary of the checked-in benchmark CSVs. |
| `benchmark_diff_vs_frozen_baseline.json` | Diff of current benchmark CSV summary against `benchmark/baseline_summary.json`. |
| `targeted_tests.log` | Targeted implementation/parity test run output. |
| `assessment.md` | This analysis and findings. |

Commands run:

```powershell
python -m src.evaluation.benchmark_summary --repo-root . --json > docs\parity-runs\2026-05-29-paper-fidelity\benchmark_summary_current.json
python -m src.evaluation.benchmark_diff --repo-root . --candidate-dir benchmark --json > docs\parity-runs\2026-05-29-paper-fidelity\benchmark_diff_vs_frozen_baseline.json
python -m pytest tests/test_cloud_query_service.py tests/test_worker_materialization.py tests/test_snapshot_hydration.py tests/test_cosmos_facts.py tests/test_cosmos_source.py tests/test_query_tracing.py tests/test_benchmark_diff.py tests/test_phase0_scaffold.py tests/test_memory_quality_prompts.py tests/test_entity_user_profile_label.py tests/test_ui_read_service.py -q
```

Results:

- Benchmark tooling/diff stability check: **ok=true**, **312 passed comparisons**, **0 failures**, **0 missing candidate files**, **0 checksum failures**. This compares the checked-in benchmark CSVs against the frozen checked-in baseline summary; it is not a fresh local-vs-cloud replay.
- Targeted tests: **64 passed**, **1 warning**.

## Feature fidelity matrix

| Dimension | Azure fidelity | Confidence | Finding |
| --- | --- | --- | --- |
| Canonical extraction and facts | Medium-High | Medium | Azure uses the local `UserForest`/extraction/fact-manager path in the worker and persists facts in Cosmos. Recent prompt guardrails improve date/acronym behavior, but live LLM compliance still needs a fresh adversarial eval. |
| Session/entity/scene trees | High | Medium-High | Azure materialization runs the local tree builder and projects session/entity/scene trees to Cosmos. The same fact can appear under multiple tree views as an access-path strategy. |
| Temporal semantics | Medium-High | Medium | Turns, cells, facts, nodes, manifests, and traces retain time/provenance. Prior observation-date bugs were found and fixed; fresh live verification remains important. |
| Incremental update and summary reuse | Medium | Medium | Worker hydrates the latest committed snapshot, applies new ingest, and commits a new generation. Summary cache exists and is carried into projection, but this run did not separately measure cache-hit rates. Projection is broader than an ideal minimal delta system. |
| Retrieval | Medium | Medium | Azure preserves the shape: Search recall -> Cosmos browse -> fact load -> cosine rerank -> Azure OpenAI answer. It is not exact local TD/BU/union parity; Azure primarily uses managed root recall plus tree browse. |
| Persistence/isolation/consistency | High | Medium-High | Tenant/user partitioning, write leases, Service Bus session ordering, manifests, Cosmos stores, Search docs, and Blob snapshots are implemented. Clear-memory now has a real backend workflow; partial-failure retry behavior is still a known operational concern. |
| Explainability traces | High | Medium-High | Cloud query traces are persisted in Cosmos and available to the UI. Historical per-message trace UX is still limited. |
| Benchmark/eval parity tooling | Medium | Medium | Frozen CSVs and baseline diff tooling are present and deterministic diff passed against the frozen baseline. This does not prove live cloud-vs-local model parity because no new cloud replay was run. |

## What Azure-MemForest preserves well

Azure-MemForest is architecturally faithful to the local implementation for the core memory substrate:

1. **Fact source of truth**: extracted and canonicalized facts remain the durable atomic memory units.
2. **Multiple tree views**: session/entity/scene trees act as overlapping access paths over the canonical facts.
3. **Temporal generations**: queries read the latest committed manifest, which represents the full accumulated forest state, not only the current session.
4. **Tree-centric retrieval**: Azure AI Search is used as the recall front door, but Cosmos-backed tree nodes and browse embeddings preserve the multi-level browse step.
5. **Explainability**: cloud query traces record recall, fact loading, rerank, and answer steps.

## Important deviations and gaps

### 1. Retrieval mode parity is incomplete

Local MemForest has explicit retrieval modes such as top-down, bottom-up, and union-style recall in the local query stack. Azure-MemForest currently uses Azure AI Search root recall plus Cosmos browse and fact rerank. This is faithful to the *shape* of tree-centric retrieval, but not exact local mode parity.

### 2. Benchmark parity is tooling-validated, not live-cloud validated

The deterministic CSV diff passed against the frozen baseline, which validates that checked-in benchmark artifacts and diff logic are stable. This run did not execute a fresh local-vs-cloud replay through live Azure OpenAI/Search/Cosmos.

### 3. LLM behavior needs adversarial eval

Recent bugs showed the model could:

- infer observation dates as validity dates
- expand acronyms such as `FDE`
- produce confusing summaries

Prompt guardrails and tests were added, but a model-output eval set is needed to verify behavior under live extraction and summarization.

### 4. Incremental projection is not minimal-delta optimized

The worker hydrates latest state and commits coherent generations, but cloud projection is still broad/full-generation. This is semantically consistent but less efficient than a fully delta-projected system.

### 5. UI/product behavior is not a paper claim

Features such as chat UI, Memory Explorer, assistant transcript persistence, clear-memory workflows, and factless no-op behavior are product decisions. They should be judged for usability and safety, not treated as paper claims.

## Interpretation

Azure-MemForest should be described as:

> A cloud implementation that preserves the local MemForest core architecture and maps it onto Azure services, with strong fidelity for extraction, canonical facts, tree construction, temporal manifests, and explainable retrieval shape.

It should **not** yet be described as:

> A formal reproduction of every paper claim under benchmark conditions.

## Recommended next experiments

1. **Paper claim registry**
   - Extract numbered claims from the paper itself.
   - Map each claim to code, tests, benchmark evidence, or gap.

2. **Fresh local-vs-cloud replay**
   - Use identical conversation/query cases against local MemForest and Azure-MemForest.
   - Compare extracted facts, tree roots, retrieved facts, and final answers.

3. **Adversarial memory-quality eval**
   - Include cases for observation date vs validity date, acronyms, role changes, conflicting facts, phatic turns, and assistant transcript turns.

4. **Retrieval mode parity test**
   - Compare local TD/BU/union results with Azure Search+Cosmos recall/browse outputs.

5. **Incremental update audit**
   - Measure how many Cosmos/Search/Blob artifacts are rewritten per ingest and identify delta-projection opportunities.

## Conclusion

The current Azure-MemForest implementation is **architecturally aligned with the local MemForest implementation**, especially for the memory substrate and cloud consistency model. Overall confidence from this run is medium because the strongest remaining parity risks are retrieval-mode differences, lack of fresh benchmark replay, and live LLM-output fidelity. The deterministic benchmark diff and targeted tests passed, so the checked-in artifacts and core cloud contracts are stable, but formal paper-faithfulness requires a stricter paper-claim and live-replay evaluation.
