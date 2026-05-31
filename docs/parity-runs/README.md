# Parity run experiment tracking

Use these folders for lightweight experiment tracking until the benchmark workflow is large enough to justify Azure ML or Foundry evaluation orchestration.

Each run folder should include:

- `manifest.json` — structured run metadata, inputs, model stack, metrics, artifacts, and caveats.
- `report.md` — human-readable analysis and conclusion.
- Raw artifacts — case files, queue/DLQ snapshots, logs, model env, query outputs, and scoring outputs.

## Manifest workflow

1. Copy `manifest.template.json` into the new run folder as `manifest.json`.
2. Fill it before running where possible:
   - run ID/date/goal
   - git SHA and deployed image
   - model stack
   - dataset/case IDs
   - scoring method
3. Append results after the run:
   - status
   - latency and quality metrics
   - failures/DLQ changes
   - artifact paths
   - caveats and next gate

This keeps experiments reproducible and comparable without requiring Azure ML setup yet. Move to Azure ML or Foundry evals when runs become scheduled, multi-user, or large enough to need managed lineage and dashboards.
