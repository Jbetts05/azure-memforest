# UI rendering architecture

The Azure-MemForest UI uses a staged rendering strategy.

## MVP renderer

- Use React for the app shell and state boundaries.
- Use SVG/D3 for MemTree rendering up to roughly `1000` visible nodes.
- Fetch tree data lazily through capped subtree APIs.
- Merge multiple subtree responses client-side into a local graph cache keyed by `forest_version/tree_id/node_id`.
- Render root-to-leaf focus paths first; show surrounding siblings only when expanded.
- Prefer semantic SVG groups for accessibility until scale requires Canvas.

## Scale strategy

When a tree exceeds the SVG budget:

- collapse internal nodes by level;
- show summary labels only on selected paths;
- request additional descendants on demand;
- use level-of-detail thresholds for labels, facts, and scores;
- move to Canvas/WebGL only after profiling proves SVG is insufficient.

## Trace overlay contract

Trace highlighting depends on IDs, not geometry:

- `tree_id` and `node_id` highlight tree nodes;
- `fact_id` highlights fact chips and inspector rows;
- `forest_version` pins the trace to the committed generation;
- future `path_from_root` support will allow deterministic path animation without recomputing ancestors client-side.

If a trace references a pruned `forest_version`, the UI must show the trace event list and explain that tree highlighting is unavailable.

