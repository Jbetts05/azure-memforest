export type ApiConfig = {
  baseUrl: string;
  userId: string;
  accessToken?: string;
  devMode?: boolean;
  devTenantId?: string;
  devActorId?: string;
};

async function request<T>(config: ApiConfig, path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set('Content-Type', 'application/json');
  if (config.accessToken) headers.set('Authorization', `Bearer ${config.accessToken}`);
  if (config.devMode) {
    headers.set('x-tenant-id', config.devTenantId ?? 'smoke-tenant');
    headers.set('x-actor-id', config.devActorId ?? config.userId);
  }
  const response = await fetch(`${config.baseUrl}${path}`, { ...init, headers });
  if (!response.ok) throw new Error(`${response.status} ${await response.text()}`);
  return response.json() as Promise<T>;
}

export type Session = { session_id: string; cell_ids: string[]; metadata: Record<string, unknown> };
export type Turn = { turn_id: string; session_id: string; turn_index: number; role: string; content: string };
export type Manifest = { manifest_id: string; forest_version: string; generation: number; status: string; metadata: Record<string, unknown> };
export type TreeNode = { tree_id: string; node_id: string; level: string; summary: string; child_ids: string[]; fact_ids: string[]; metadata: Record<string, unknown> };
export type QueryResult = { query_id: string; answer?: string; reason?: string; facts: Array<{ fact_id: string; text: string; metadata: Record<string, unknown> }> };
export type QueryTrace = { query_id: string; forest_version: string; event_count: number; event_summaries: Array<Record<string, unknown>> };
export type FactDetail = { fact_id: string; text: string; metadata: Record<string, unknown>; locations: Array<Record<string, unknown>> };
export type JobStatus = { job_id: string; status: 'accepted' | 'running' | 'succeeded' | 'failed' | 'cancelled'; error?: string | null };
export type JobEvent = { event_type: string; timestamp_unix?: number | null; status: string; detail?: string | null };

export const api = {
  sessions: (config: ApiConfig) => request<Session[]>(config, `/v1/users/${config.userId}/sessions`),
  turns: (config: ApiConfig, sessionId: string) => request<Turn[]>(config, `/v1/users/${config.userId}/sessions/${sessionId}/turns`),
  latestForest: (config: ApiConfig) => request<Manifest>(config, `/v1/users/${config.userId}/forest/latest`),
  treeRoots: (config: ApiConfig, forestVersion: string) => request<{ forest_version: string; roots: TreeNode[] }>(config, `/v1/users/${config.userId}/forest/${forestVersion}/trees`),
  query: (config: ApiConfig, question: string) => request<QueryResult>(config, `/v1/users/${config.userId}/query`, { method: 'POST', body: JSON.stringify({ question, max_facts: 8, trace_requested: true }) }),
  trace: (config: ApiConfig, queryId: string) => request<QueryTrace>(config, `/v1/users/${config.userId}/queries/${queryId}/trace`),
  fact: (config: ApiConfig, forestVersion: string, factId: string) => request<FactDetail>(config, `/v1/users/${config.userId}/forest/${forestVersion}/facts/${factId}`),
  ingestSession: (config: ApiConfig, sessionId: string, content: string) => request<{ job_id: string; status: string }>(
    config,
    `/v1/users/${config.userId}/sessions/${sessionId}:ingest`,
    {
      method: 'POST',
      headers: { 'Idempotency-Key': crypto.randomUUID() },
      body: JSON.stringify({ turns: [{ role: 'user', content, timestamp: new Date().toISOString() }], metadata: { source: 'ui' } })
    }
  ),
  job: (config: ApiConfig, jobId: string) => request<JobStatus>(config, `/v1/users/${config.userId}/jobs/${jobId}`),
  jobEvents: (config: ApiConfig, jobId: string) => request<JobEvent[]>(config, `/v1/users/${config.userId}/jobs/${jobId}/events`)
};
