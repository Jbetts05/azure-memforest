import type { Manifest, TreeNode } from '../api';

export function MemoryForestViz({ manifest, nodes }: { manifest?: Manifest | null; nodes: TreeNode[] }) {
  const visible = nodes.slice(0, 8);
  const types = visible.reduce<Record<string, number>>((acc, node) => {
    const type = String(node.metadata.tree_type ?? node.level ?? 'tree');
    acc[type] = (acc[type] ?? 0) + 1;
    return acc;
  }, {});
  const factCount = visible.reduce((total, node) => total + node.fact_ids.length, 0);
  const childCount = visible.reduce((total, node) => total + node.child_ids.length, 0);
  return (
    <section className="treePanel" aria-label="Memory forest visualization">
      <strong>Memory forest</strong>
      <p className="tiny">{manifest ? `${manifest.forest_version} · generation ${manifest.generation}` : 'No committed forest loaded'}</p>
      {!nodes.length ? <p className="tiny">No durable memory has been extracted yet.</p> : null}
      {nodes.length > 0 && factCount === 0 ? <p className="tiny">This forest has structure but no extracted facts yet. Future factless turns will stay out of the forest.</p> : null}
      <div className="forestStats" aria-label="Latest forest stats">
        <div><b>{nodes.length}</b><span>trees</span></div>
        <div><b>{factCount}</b><span>root facts</span></div>
        <div><b>{childCount}</b><span>children</span></div>
      </div>
      <div className="typeStrip">
        {Object.entries(types).map(([type, count]) => <span key={type}>{type}: {count}</span>)}
      </div>
      <div className="treeList">
        {visible.map((node) => {
          const type = String(node.metadata.tree_type ?? node.level ?? 'tree');
          return (
            <article key={`${node.tree_id}:${node.node_id}`} className="treeItem">
              <div>
                <span>{type}</span>
                <small>{node.level} · {node.fact_ids.length} fact(s) · {node.child_ids.length} child node(s)</small>
              </div>
              <p>{_summaryBody(node.summary) || 'No summary generated for this tree yet.'}</p>
              <small>{_treeLabel(node)}</small>
            </article>
          );
        })}
      </div>
    </section>
  );
}

function _treeLabel(node: TreeNode): string {
  return String(node.metadata.tree_label || (node.tree_id === 'entity:user' ? 'User profile' : node.tree_id));
}

function _summaryBody(summary: string): string {
  const withoutHeader = summary.replace(/^(\s*\[[^\]]*\]\s*)+/, '').trim();
  const cleaned = withoutHeader
    .replace(/,\s*starting\s+[A-Z][a-z]+\s+\d{1,2},\s+\d{4}/g, '')
    .replace(/\s+starting\s+[A-Z][a-z]+\s+\d{1,2},\s+\d{4}/g, '')
    .replace(/On that same date,\s*/g, '')
    .split(/(?<=[.!?])\s+/)
    .filter((sentence) => !/no other documented|timeline of facts spans|latest known user state/i.test(sentence))
    .join(' ')
    .trim();
  const sentences = cleaned.match(/[^.!?]+[.!?]+/g);
  const compact = sentences ? sentences.slice(0, 2).join(' ').trim() : cleaned;
  if (compact.length <= 260) return compact;
  const slice = compact.slice(0, 257);
  return `${slice.slice(0, Math.max(slice.lastIndexOf(' '), 180)).trimEnd()}...`;
}
