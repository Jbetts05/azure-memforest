import type { Manifest, TreeNode } from '../api';

export function MemoryForestViz({ manifest, nodes }: { manifest?: Manifest | null; nodes: TreeNode[] }) {
  const visible = nodes.slice(0, 10);
  return (
    <section className="treePanel" aria-label="Memory forest visualization">
      <strong>Memory forest</strong>
      <p className="tiny">{manifest ? `${manifest.forest_version} · generation ${manifest.generation}` : 'No committed forest loaded'}</p>
      {!nodes.length ? <p className="tiny">No tree roots loaded.</p> : null}
      <p className="sr-only">Memory tree visualization. Each point represents a loaded tree root or node from the latest committed forest.</p>
      <svg viewBox="0 0 420 220" role="img" aria-label="MemTree roots">
        <line x1="210" y1="32" x2="100" y2="150" />
        <line x1="210" y1="32" x2="210" y2="160" />
        <line x1="210" y1="32" x2="320" y2="150" />
        {visible.map((node, index) => {
          const x = 80 + (index % 5) * 64;
          const y = index === 0 ? 32 : 140 + (index % 2) * 34;
          return <g key={`${node.tree_id}:${node.node_id}`}><circle cx={x} cy={y} r="12" /><text x={x + 16} y={y + 4}>{node.metadata.tree_type ? String(node.metadata.tree_type) : node.level}</text></g>;
        })}
      </svg>
    </section>
  );
}
