import { useEffect, useMemo, useState } from 'react';
import { api, type ApiConfig, type FactDetail, type JobEvent, type JobStatus, type Manifest, type QueryResult, type QueryTrace, type TreeNode } from '../api';

type Tab = 'forest' | 'facts' | 'trace' | 'jobs';

export function MemoryExplorerDrawer({
  open,
  initialTab,
  config,
  manifest,
  roots,
  forestFacts,
  query,
  trace,
  job,
  jobEvents,
  extracting,
  onClose
}: {
  open: boolean;
  initialTab: Tab;
  config: ApiConfig | null;
  manifest: Manifest | null;
  roots: TreeNode[];
  forestFacts: FactDetail[];
  query: QueryResult;
  trace: QueryTrace | null;
  job: JobStatus | null;
  jobEvents: JobEvent[];
  extracting: boolean;
  onClose: () => void;
}) {
  const [tab, setTab] = useState<Tab>(initialTab);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [loaded, setLoaded] = useState<Record<string, boolean>>({});
  const [nodesByKey, setNodesByKey] = useState<Record<string, TreeNode>>({});
  const [selected, setSelected] = useState<TreeNode | null>(null);
  const factLookup = useMemo(() => new Map(forestFacts.map((fact) => [fact.fact_id, fact])), [forestFacts]);
  const forestVersion = manifest?.forest_version ?? '';

  useEffect(() => { setTab(initialTab); }, [initialTab]);
  useEffect(() => {
    const next = Object.fromEntries(roots.map((root) => [_nodeKey(root), root]));
    setNodesByKey(next);
    setLoaded({});
    setExpanded({});
    setSelected(null);
  }, [forestVersion, roots]);
  useEffect(() => {
    if (!open) return;
    const onKey = (event: KeyboardEvent) => { if (event.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose, open]);

  async function toggle(node: TreeNode) {
    const key = _nodeKey(node);
    setExpanded((current) => ({ ...current, [key]: !current[key] }));
    setSelected(node);
    if (!config || !manifest || loaded[key] || node.child_ids.length === 0) return;
    const subtree = await api.subtree(config, manifest.forest_version, node.tree_id, node.node_id);
    setNodesByKey((current) => ({ ...current, ...Object.fromEntries(subtree.nodes.map((item) => [_nodeKey(item), item])) }));
    setLoaded((current) => ({ ...current, [key]: true }));
  }

  return (
    <div className={`drawerLayer ${open ? 'open' : ''}`} aria-hidden={!open}>
      <button className="drawerScrim" type="button" onClick={onClose} aria-label="Close memory explorer" />
      <aside className="memoryDrawer" role="dialog" aria-modal="true" aria-labelledby="memoryExplorerTitle" aria-label="Memory Explorer">
        <header className="drawerHeader">
          <div>
            <p className="kicker">Memory Explorer</p>
            <h2 id="memoryExplorerTitle">{manifest ? `Generation ${manifest.generation}` : 'No committed forest'}</h2>
          </div>
          <button className="ghostButton" type="button" onClick={onClose}>Close</button>
        </header>
        <nav className="drawerTabs" role="tablist" aria-label="Memory explorer tabs">
          {(['forest', 'facts', 'trace', 'jobs'] as Tab[]).map((item) => (
            <button key={item} role="tab" aria-selected={tab === item} className={tab === item ? 'active' : ''} type="button" onClick={() => setTab(item)}>
              {item}
            </button>
          ))}
        </nav>
        {tab === 'forest' ? (
          <section className="drawerPane">
            <MemoryLegend />
            <div className="drawerStats">
              <div><b>{roots.length}</b><span>trees</span></div>
              <div><b>{forestFacts.length}</b><span>facts</span></div>
              <div><b>{extracting ? 'live' : 'idle'}</b><span>memory job</span></div>
            </div>
            <div className="explorerGrid">
              <div className="branchList">
                {roots.length ? roots.map((root) => (
                  <TreeBranch
                    key={_nodeKey(root)}
                    node={root}
                    depth={0}
                    expanded={expanded}
                    nodesByKey={nodesByKey}
                    selected={selected}
                    onToggle={toggle}
                  />
                )) : <p className="tiny">No durable memory tree yet. Send a memory-bearing message to grow the forest.</p>}
              </div>
              <NodeDetail node={selected ?? roots[0] ?? null} facts={factLookup} />
            </div>
          </section>
        ) : null}
        {tab === 'facts' ? (
          <section className="drawerPane factShelf">
            <h3>Facts in latest forest</h3>
            {forestFacts.length ? forestFacts.map((fact) => <article key={fact.fact_id}><span>{fact.fact_id}</span><p>{fact.text}</p><small>{fact.locations.length} location(s)</small></article>) : <p className="tiny">No extracted facts yet.</p>}
          </section>
        ) : null}
        {tab === 'trace' ? (
          <section className="drawerPane traceShelf">
            <h3>Latest reply trace</h3>
            <p className="tiny">{trace ? `${trace.event_count} events · ${trace.forest_version}` : query.reason}</p>
            <ol>
              {(trace?.event_summaries ?? []).map((event, index) => <li key={index}><span>{String(event.event_type ?? 'event')}</span><small>{String(event.tree_id ?? event.node_id ?? event.fact_id ?? '')}</small></li>)}
            </ol>
          </section>
        ) : null}
        {tab === 'jobs' ? (
          <section className="drawerPane jobShelf">
            <h3>Background memory job</h3>
            <p className="tiny">{extracting ? 'Extraction and materialization are running in the background.' : job ? `Last job ${job.status}` : 'No memory job has run in this session.'}</p>
            {job ? <article><span>{job.job_id}</span><p>{job.status}</p>{job.error ? <small>{job.error}</small> : null}</article> : null}
            <ol>{jobEvents.map((event, index) => <li key={index}>{event.event_type} · {event.status}</li>)}</ol>
          </section>
        ) : null}
      </aside>
    </div>
  );
}

function MemoryLegend() {
  const items = [
    ['Session', 'Conversation memory in time order.'],
    ['Entity', 'Facts grouped around a person, org, tool, place, or concept.'],
    ['Scene', 'Facts grouped around a topic, activity, or recurring context.'],
    ['Fact', 'A durable memory extracted from conversation.'],
    ['Level', 'L0 is closest to facts; higher levels summarize children.'],
    ['Trace', 'The retrieval path used to answer a question.']
  ];
  return (
    <details className="memoryLegend">
      <summary>Memory key</summary>
      <div>
        {items.map(([term, text]) => <p key={term}><b>{term}</b><span>{text}</span></p>)}
      </div>
    </details>
  );
}

function TreeBranch({ node, depth, expanded, nodesByKey, selected, onToggle }: { node: TreeNode; depth: number; expanded: Record<string, boolean>; nodesByKey: Record<string, TreeNode>; selected: TreeNode | null; onToggle: (node: TreeNode) => void }) {
  const key = _nodeKey(node);
  const children = node.child_ids.map((childId) => nodesByKey[`${node.tree_id}:${childId}`]).filter(Boolean);
  const type = String(node.metadata.tree_type ?? node.level ?? 'tree');
  const label = _treeLabel(node);
  return (
    <div className="branchGroup" style={{ '--depth': depth } as React.CSSProperties}>
      <button className={`branchNode ${selected && _nodeKey(selected) === key ? 'selected' : ''}`} type="button" onClick={() => void onToggle(node)}>
        <span className="branchRail" />
        <span className="branchType">{type}</span>
        <span className="branchSummary">{_compact(node.summary) || label}</span>
        <span className="branchMeta">{node.level} · {node.fact_ids.length} facts · {node.child_ids.length} children</span>
      </button>
      {expanded[key] && children.length ? children.map((child) => (
        <TreeBranch key={_nodeKey(child)} node={child} depth={depth + 1} expanded={expanded} nodesByKey={nodesByKey} selected={selected} onToggle={onToggle} />
      )) : null}
    </div>
  );
}

function NodeDetail({ node, facts }: { node: TreeNode | null; facts: Map<string, FactDetail> }) {
  if (!node) return <aside className="nodeDetail"><p className="tiny">Select a tree node to inspect it.</p></aside>;
  const attached = node.fact_ids.map((id) => facts.get(id)).filter(Boolean) as FactDetail[];
  return (
    <aside className="nodeDetail">
      <span>{node.level}</span>
      <h3>{_treeLabel(node)}</h3>
      <p>{_compact(node.summary) || 'No summary generated yet.'}</p>
      <div className="nodeMetrics"><b>{node.child_ids.length}</b><small>children</small><b>{node.fact_ids.length}</b><small>facts</small></div>
      {attached.map((fact) => <article key={fact.fact_id}><small>{fact.fact_id}</small><p>{fact.text}</p></article>)}
    </aside>
  );
}

function _nodeKey(node: TreeNode): string {
  return `${node.tree_id}:${node.node_id}`;
}

function _treeLabel(node: TreeNode): string {
  return String(node.metadata.tree_label || (node.tree_id === 'entity:user' ? 'User profile' : node.tree_id));
}

function _compact(text: string): string {
  const stripped = text.replace(/^(\s*\[[^\]]*\]\s*)+/, '').trim();
  return stripped.length > 180 ? `${stripped.slice(0, 177).trimEnd()}...` : stripped;
}
