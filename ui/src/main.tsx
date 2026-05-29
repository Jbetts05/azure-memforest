import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { api, ApiError, type ApiConfig, type ChatResult, type FactDetail, type Manifest, type QueryResult, type QueryTrace, type Session, type TreeNode, type Turn } from './api';
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
  const [forestFacts, setForestFacts] = useState<FactDetail[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [emptyForest, setEmptyForest] = useState(false);
  const forestFactsRequestRef = useRef(0);
  const visibleQuery: QueryResult = query ?? {
    query_id: '',
    answer: undefined,
    reason: 'Memory used in replies will appear here.',
    facts: []
  };

  async function refresh(cfg: ApiConfig): Promise<Manifest> {
    setEmptyForest(false);
    const loadedSessions = await api.sessions(cfg).catch(() => []);
    setSessions(loadedSessions);
    setTurns(loadedSessions[0] ? await api.turns(cfg, loadedSessions[0].session_id).catch(() => []) : []);
    try {
      const latest = await api.latestForest(cfg);
      setEmptyForest(false);
      setManifest(latest);
      const roots = await api.treeRoots(cfg, latest.forest_version);
      setNodes(roots.roots);
      void loadForestFacts(cfg, latest.forest_version, roots.roots);
      return latest;
    } catch (err) {
      if (_isForestNotFound(err)) {
        setEmptyForest(true);
        setManifest(null);
        setNodes([]);
        setTrace(null);
        setFactDetail(null);
        setForestFacts([]);
      }
      throw err;
    }
  }

  async function loadQuery(cfg: ApiConfig, forestVersion: string, result: QueryResult) {
    setQuery(result);
    if (result.query_id) {
      const loadedTrace = await api.trace(cfg, result.query_id);
      setTrace(loadedTrace);
    }
    const firstFact = result.facts[0];
    if (firstFact) setFactDetail(await api.fact(cfg, forestVersion, firstFact.fact_id));
    else setFactDetail(null);
  }

  async function loadForestFacts(cfg: ApiConfig, forestVersion: string, roots: TreeNode[]) {
    const requestId = forestFactsRequestRef.current + 1;
    forestFactsRequestRef.current = requestId;
    const subtrees = await Promise.allSettled(
      roots.slice(0, 6).map((root) => api.subtree(cfg, forestVersion, root.tree_id, root.node_id))
    );
    const factIds = new Set<string>();
    for (const result of subtrees) {
      if (result.status !== 'fulfilled') continue;
      for (const node of result.value.nodes) {
        for (const factId of node.fact_ids) {
          if (factIds.size < 24) factIds.add(factId);
        }
      }
    }
    const facts = await Promise.allSettled([...factIds].map((factId) => api.fact(cfg, forestVersion, factId)));
    if (forestFactsRequestRef.current !== requestId) return;
    setForestFacts(facts.flatMap((result) => result.status === 'fulfilled' ? [result.value] : []));
  }

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const cfg = await getApiConfig();
        if (cancelled) return;
        setConfig(cfg);
        try {
          await refresh(cfg);
        } catch (err) {
          if (!cancelled && !_isForestNotFound(err)) setError(err instanceof Error ? err.message : String(err));
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      }
    }
    void load();
    return () => { cancelled = true; };
  }, []);

  async function handleChatResult(result: ChatResult) {
    const queryResult: QueryResult = {
      query_id: result.query_id ?? '',
      answer: result.reply,
      reason: result.reason ?? _memoryStatusCopy(result.memory_status),
      facts: result.facts
    };
    setQuery(queryResult);
    if (config && manifest && result.query_id) {
      try {
        await loadQuery(config, manifest.forest_version, queryResult);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
    }
  }

  return (
    <main className="appShell">
      <header className="topbar">
        <div>
          <strong>MemForest</strong>
          <span>Persistent memory for ongoing conversations</span>
        </div>
        <div className="statusPills">
          <span>{config?.devMode ? 'Dev auth' : 'Entra'}</span>
          <span>{manifest ? `generation ${manifest.generation}` : emptyForest ? 'first run' : 'loading'}</span>
        </div>
      </header>
      <section className="appLayout">
        <ChatWorkspace
          config={config}
          initialTurns={turns}
          onChatResult={(result) => { void handleChatResult(result); }}
          onIngestComplete={async () => {
            if (!config) return;
            try {
              await refresh(config);
            } catch (err) {
              if (!_isForestNotFound(err)) setError(err instanceof Error ? err.message : String(err));
            }
          }}
        />
        <aside className="memoryPanel" aria-label="Memory inspector">
          <section className="memoryIntro">
            <p className="kicker">Memory layer</p>
            <h2>{emptyForest ? 'Ready to extract first memories.' : 'Memory updates behind the chat.'}</h2>
            <p>{emptyForest ? 'Send a message. MemForest will extract useful details and commit the first forest in the background.' : 'Facts, trees, and trace events appear here when a reply uses committed memory.'}</p>
            {error ? <p className="error">{error}</p> : null}
          </section>
          <MemoryForestViz manifest={manifest} nodes={nodes} />
          <FactInspector result={visibleQuery} detail={factDetail} forestFacts={forestFacts} />
          <QueryTraceViz trace={trace} />
          <SessionBrowser sessions={sessions} turns={turns} />
          <OpsStatus manifest={manifest} authMode={config?.devMode ? 'Dev headers' : 'Entra'} error={error} />
        </aside>
      </section>
    </main>
  );
}

createRoot(document.getElementById('root')!).render(<App />);

function _isForestNotFound(err: unknown): boolean {
  return err instanceof ApiError ? err.code === 'forest_not_found' : err instanceof Error && err.message.includes('forest_not_found');
}

function _memoryStatusCopy(status: ChatResult['memory_status']): string {
  if (status === 'grounded') return 'Reply used recalled MemForest facts.';
  if (status === 'no_forest') return 'No committed forest existed before this turn.';
  if (status === 'no_facts') return 'The forest was queried, but no relevant facts were found.';
  return 'Memory recall was unavailable for this turn.';
}
