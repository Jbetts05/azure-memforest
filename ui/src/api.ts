export type ApiConfig = {
  baseUrl: string;
  userId: string;
  accessToken?: string;
  devMode?: boolean;
  devTenantId?: string;
  devActorId?: string;
};

export class ApiError extends Error {
  constructor(public status: number, public code: string | null, message: string) {
    super(message);
  }
}

async function request<T>(config: ApiConfig, path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set('Content-Type', 'application/json');
  if (config.accessToken) headers.set('Authorization', `Bearer ${config.accessToken}`);
  if (config.devMode) {
    headers.set('x-tenant-id', config.devTenantId ?? 'smoke-tenant');
    headers.set('x-actor-id', config.devActorId ?? config.userId);
  }
  const response = await fetch(`${config.baseUrl}${path}`, { ...init, headers });
  if (!response.ok) {
    const text = await response.text();
    let code: string | null = null;
    try {
      const body = JSON.parse(text) as { error?: unknown };
      code = typeof body.error === 'string' ? body.error : null;
    } catch {
      code = null;
    }
    throw new ApiError(response.status, code, `${response.status} ${text}`);
  }
  return response.json() as Promise<T>;
}

export type Session = { session_id: string; cell_ids: string[]; metadata: Record<string, unknown> };
export type Turn = { turn_id: string; session_id: string; turn_index: number; role: string; content: string; timestamp?: string | null };
export type Manifest = { manifest_id: string; forest_version: string; generation: number; status: string; metadata: Record<string, unknown> };
export type TreeNode = { tree_id: string; node_id: string; level: string; summary: string; child_ids: string[]; fact_ids: string[]; metadata: Record<string, unknown> };
export type Subtree = { forest_version: string; tree_id: string; root_node_id: string; truncated: boolean; nodes: TreeNode[] };
export type QueryResult = { query_id: string; answer?: string; reason?: string; facts: Array<{ fact_id: string; text: string; metadata: Record<string, unknown> }> };
export type QueryTrace = { query_id: string; forest_version: string; event_count: number; event_summaries: Array<Record<string, unknown>> };
export type FactDetail = { fact_id: string; text: string; metadata: Record<string, unknown>; locations: Array<Record<string, unknown>> };
export type JobStatus = { job_id: string; status: 'accepted' | 'running' | 'succeeded' | 'failed' | 'cancelled'; error?: string | null };
export type JobEvent = { event_type: string; timestamp_unix?: number | null; status: string; detail?: string | null };
export type TurnInput = { role: string; content: string; timestamp?: string; content_id?: string; metadata?: Record<string, unknown> };
export type ChatResult = {
  status: 'succeeded';
  reply: string;
  query_id?: string | null;
  reason?: string | null;
  memory_status: 'grounded' | 'no_forest' | 'no_facts' | 'unavailable';
  facts: QueryResult['facts'];
};

export const api = {
  sessions: (config: ApiConfig) => request<Session[]>(config, `/v1/users/${config.userId}/sessions`),
  turns: (config: ApiConfig, sessionId: string) => request<Turn[]>(config, `/v1/users/${config.userId}/sessions/${sessionId}/turns`),
  latestForest: (config: ApiConfig) => request<Manifest>(config, `/v1/users/${config.userId}/forest/latest`),
  treeRoots: (config: ApiConfig, forestVersion: string) => request<{ forest_version: string; roots: TreeNode[] }>(config, `/v1/users/${config.userId}/forest/${forestVersion}/trees`),
  subtree: (config: ApiConfig, forestVersion: string, treeId: string, rootNodeId: string) => request<Subtree>(config, `/v1/users/${config.userId}/forest/${forestVersion}/trees/${treeId}/subtree?root_node_id=${encodeURIComponent(rootNodeId)}&max_depth=4&max_nodes=80`),
  query: (config: ApiConfig, question: string) => request<QueryResult>(config, `/v1/users/${config.userId}/query`, { method: 'POST', body: JSON.stringify({ question, max_facts: 8, trace_requested: true }) }),
  chat: (config: ApiConfig, message: string, turns: TurnInput[]) => request<ChatResult>(config, `/v1/users/${config.userId}/chat`, {
    method: 'POST',
    body: JSON.stringify({ message, turns, max_facts: 8, trace_requested: true })
  }),
  trace: (config: ApiConfig, queryId: string) => request<QueryTrace>(config, `/v1/users/${config.userId}/queries/${queryId}/trace`),
  fact: (config: ApiConfig, forestVersion: string, factId: string) => request<FactDetail>(config, `/v1/users/${config.userId}/forest/${forestVersion}/facts/${factId}`),
  ingestSession: (config: ApiConfig, sessionId: string, turns: TurnInput[], metadata: Record<string, unknown> = {}) => request<{ job_id: string; status: string }>(
    config,
    `/v1/users/${config.userId}/sessions/${sessionId}:ingest`,
    {
      method: 'POST',
      headers: { 'Idempotency-Key': crypto.randomUUID() },
      body: JSON.stringify({ turns, metadata: { source: 'chat-ui', ...metadata } })
    }
  ),
  job: (config: ApiConfig, jobId: string) => request<JobStatus>(config, `/v1/users/${config.userId}/jobs/${jobId}`),
  jobEvents: (config: ApiConfig, jobId: string) => request<JobEvent[]>(config, `/v1/users/${config.userId}/jobs/${jobId}/events`)
};
