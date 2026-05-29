import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { motion } from 'framer-motion';
import { api, type ApiConfig, type FactDetail, type Manifest, type QueryResult, type QueryTrace, type Session, type TreeNode, type Turn } from './api';
import { getApiConfig } from './auth';
import { ChatWorkspace } from './components/ChatWorkspace';
import { FactInspector } from './components/FactInspector';
import { MemoryForestViz } from './components/MemoryForestViz';
import { OpsStatus } from './components/OpsStatus';
import { QueryTraceViz } from './components/QueryTraceViz';
import { SessionBrowser } from './components/SessionBrowser';
import './styles.css';

function App() {
  const [config, setConfig] = useState<ApiConfig | null>(null);
  const [sessions, setSessions] = useState<Session[]>([]);
  const [turns, setTurns] = useState<Turn[]>([]);
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [nodes, setNodes] = useState<TreeNode[]>([]);
  const [query, setQuery] = useState<QueryResult | null>(null);
  const [trace, setTrace] = useState<QueryTrace | null>(null);
  const [factDetail, setFactDetail] = useState<FactDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const visibleQuery: QueryResult = query ?? {
    query_id: '',
    answer: 'No query run yet.',
    reason: 'Ask a question to see grounded memory.',
    facts: []
  };

  async function refresh(cfg: ApiConfig): Promise<Manifest> {
    const [loadedSessions, latest] = await Promise.all([api.sessions(cfg), api.latestForest(cfg)]);
    setSessions(loadedSessions);
    setManifest(latest);
    if (loadedSessions[0]) setTurns(await api.turns(cfg, loadedSessions[0].session_id));
    const roots = await api.treeRoots(cfg, latest.forest_version);
    setNodes(roots.roots);
    return latest;
  }

  async function loadQuery(cfg: ApiConfig, forestVersion: string, result?: QueryResult) {
    const queryResult = result ?? await api.query(cfg, 'Where did Alice move?');
    setQuery(queryResult);
    const loadedTrace = await api.trace(cfg, queryResult.query_id);
    setTrace(loadedTrace);
    const firstFact = queryResult.facts[0];
    if (firstFact) setFactDetail(await api.fact(cfg, forestVersion, firstFact.fact_id));
  }

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const cfg = await getApiConfig();
        if (cancelled) return;
        setConfig(cfg);
        const latest = await refresh(cfg);
        if (!cancelled) await loadQuery(cfg, latest.forest_version);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      }
    }
    void load();
    return () => { cancelled = true; };
  }, []);

  return (
    <main className="shell">
      <section className="hero">
        <motion.p initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} className="eyebrow">
          Azure-MemForest UI
        </motion.p>
        <motion.h1 initial={{ opacity: 0, y: 18 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.08 }}>
          Watch memory become a forest.
        </motion.h1>
        <motion.p initial={{ opacity: 0, y: 18 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.16 }} className="lede">
          A workspace for conversations, committed MemTrees, query traces, and grounded answers.
        </motion.p>
      </section>
      <section className="workspace" aria-label="UI scaffold">
        <nav>
          <span>Sessions</span>
          <span>Forest</span>
          <span>Trace</span>
          <span>Facts</span>
          <span>Ops</span>
        </nav>
        <div className="canvas">
          <SessionBrowser sessions={sessions} turns={turns} />
          <MemoryForestViz manifest={manifest} nodes={nodes} />
          <aside aria-label="Trace and fact inspector">
            <strong>{visibleQuery.answer}</strong>
            <p>{visibleQuery.reason}</p>
            <p className="tiny">Trace events: {trace?.event_count ?? 0} · {trace?.forest_version ?? 'no trace'}</p>
            <p className="tiny">Fact: {visibleQuery.facts[0]?.text ?? 'No fact selected'}</p>
            {error ? <p className="error">Live API: {error}</p> : null}
          </aside>
        </div>
        <ChatWorkspace
          config={config}
          onQuery={(result) => {
            setQuery(result);
            if (config && manifest) void loadQuery(config, manifest.forest_version, result);
          }}
          onIngestComplete={async () => {
            if (config) await refresh(config);
          }}
        />
        <div className="lower">
          <FactInspector result={visibleQuery} detail={factDetail} />
          <OpsStatus manifest={manifest} authMode={config?.devMode ? 'Dev headers' : 'Entra'} error={error} />
          <QueryTraceViz trace={trace} />
        </div>
      </section>
    </main>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
