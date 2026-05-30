# Paper-claim registry and replay attempt

Run date: 2026-05-29

## Goal

Extend the prior local-to-Azure fidelity assessment with:

1. a paper/local claim registry, and
2. a fresh replay attempt comparing local feasibility and live Azure behavior.

## Artifacts

| File | Purpose |
| --- | --- |
| `MemForest_with_Appendix.txt` | Text extracted from `paper/MemForest_with_Appendix.pdf`. |
| `MemForest_VLDB_submit.txt` | Text extracted from `paper/MemForest_VLDB_submit.pdf`. |
| `claim_registry.md` | Human-readable claim registry. |
| `claim_registry.json` | Machine-readable claim registry. |
| `local_replay_feasibility.json` | Local replay feasibility probe. |
| `cloud_dev_auth_probe.json` | Cloud auth probe for dev-header replay isolation. |
| `cloud_replay_result.json` | Live cloud replay transcript/results. |
| `outcome_summary.json` | Compact outcome summary. |
| `report.md` | This report. |

## Claim registry outcome

The registry contains **17 atomic claims** across:

- extraction and MemCells
- canonical fact management and deduplication
- session/entity/scene trees
- temporal properties
- incremental updates and summary reuse
- retrieval pipeline
- evaluation/benchmark artifacts
- system properties

Important limitation: although PDF text extraction succeeded for this run, the claim registry still uses a mixture of extracted-paper snippets and in-repo code/docs evidence. It is a practical registry, not a manually curated formal paper claim table.

## Replay design

The intended replay case was:

1. Clear the current user memory.
2. Ingest: `My name is Alex. I work at Example Corp in the Example Research organization as a data scientist.`
3. Query: `What role does Alex have?`
4. Ingest update: `Update: I now work at Example Labs as an FDE.`
5. Query: `Where does Alex work now and what role code did he give?`
6. Inspect latest manifest and tree roots.

This case specifically tests:

- canonical fact extraction
- recency-sensitive role retrieval behavior
- acronym preservation (`FDE`)
- current-state retrieval
- tree materialization after multiple ingests

## Local replay outcome

Local replay was **blocked** by missing local model endpoints:

- LLM endpoint configured: `http://127.0.0.1:8001/v1`
- Embedding endpoint configured: `http://127.0.0.1:8002/v1`
- Both endpoints were unavailable.

Outcome stored in `local_replay_feasibility.json`.

This means no fresh local-vs-cloud output comparison was possible in this environment without starting compatible local model services.

## Cloud replay outcome

Cloud replay completed against the deployed Azure-MemForest API.

API version:

```text
eec2e9fac3ae
```

Observed answers:

| Step | Answer |
| --- | --- |
| After first ingest | `Alex is a data scientist at Example Corp in the Example Research organization.` |
| After role update | `Alex works at Example Labs as an FDE.` |

Latest forest root count after replay: **7**.

The second answer is important because it preserved the acronym **FDE** rather than expanding it incorrectly.

Outcome stored in `cloud_replay_result.json`.

## Interpretation

The replay provides useful evidence that the deployed cloud path can:

- clear stale memory state
- ingest fresh user facts
- answer from committed memory
- preserve user-provided role code `FDE`
- answer with the most recent role/employer after a later ingest

The replay does **not** prove hard supersession semantics. The earlier Example Corp fact remained present after the OpenAI/FDE update; the answer preferred the newer OpenAI/FDE fact rather than deleting or tombstoning the older fact.

Observed anomaly: the replay root set included two employment scene roots that referenced the same Example Corp fact. This may indicate scene-router duplication or over-splitting and should be investigated in a follow-up scene-routing eval.

It does **not** prove full local-vs-cloud parity because local replay could not run without model endpoints.

## Remaining gaps

1. Start local OpenAI-compatible chat and embedding endpoints and rerun the same replay locally.
2. Compare local facts, tree roots, retrieved facts, and answers to the cloud replay artifacts. A local run could use local model endpoints or be explicitly configured to call the same Azure OpenAI deployments.
3. Expand replay cases to include:
   - explicit dates vs observation dates
   - ambiguous acronyms
   - phatic/factless turns
   - assistant transcript turns marked `skip_extraction`
   - conflicting/superseding facts
   - follow-up queries such as “Where did Alex used to work?” to separate coexistence from supersession
4. Add a manual paper-claim review pass with page/section references from the extracted PDF text.

## Conclusion

This run improves the evidence base beyond the prior deterministic benchmark diff by adding a claim registry and a live cloud replay. The cloud replay result is encouraging for the recent memory-correctness fixes, especially acronym preservation and role update retrieval. However, formal paper parity remains unproven until the same replay can be executed against a local MemForest instance with compatible model endpoints and compared directly.
