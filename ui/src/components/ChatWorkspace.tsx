import { useState } from 'react';
import { api, type ApiConfig, type JobEvent, type JobStatus, type QueryResult } from '../api';

const terminal = new Set(['succeeded', 'failed', 'cancelled']);

export function ChatWorkspace({ config, onQuery, onIngestComplete }: { config: ApiConfig | null; onQuery: (result: QueryResult) => void; onIngestComplete: () => Promise<void> }) {
  const [memoryText, setMemoryText] = useState('Alice met Morgan in London.');
  const [question, setQuestion] = useState('Where did Alice move?');
  const [job, setJob] = useState<JobStatus | null>(null);
  const [events, setEvents] = useState<JobEvent[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [pollingTimedOut, setPollingTimedOut] = useState(false);

  async function ingest() {
    if (!config) return;
    setError(null);
    setPollingTimedOut(false);
    const sessionId = `ui-${crypto.randomUUID()}`;
    const accepted = await api.ingestSession(config, sessionId, memoryText);
    let current = await api.job(config, accepted.job_id);
    setJob(current);
    setEvents(_eventsOrEmpty(await api.jobEvents(config, accepted.job_id)));
    for (let i = 0; i < 30 && !terminal.has(current.status); i += 1) {
      await new Promise((resolve) => setTimeout(resolve, 2000));
      current = await api.job(config, accepted.job_id);
      setJob(current);
      setEvents(_eventsOrEmpty(await api.jobEvents(config, accepted.job_id)));
    }
    if (current.status === 'succeeded') await onIngestComplete();
    if (current.status === 'failed') setError(current.error ?? 'Ingest failed');
    if (!terminal.has(current.status)) setPollingTimedOut(true);
  }

  async function query() {
    if (!config) return;
    setError(null);
    onQuery(await api.query(config, question));
  }

  return (
    <section className="workspaceControls" aria-label="Chat workspace">
      <label>Memory turn<textarea value={memoryText} onChange={(event) => setMemoryText(event.target.value)} /></label>
      <button disabled={!config} onClick={() => void ingest()}>Ingest memory</button>
      <label>Question<input value={question} onChange={(event) => setQuestion(event.target.value)} /></label>
      <button disabled={!config} onClick={() => void query()}>Query memory</button>
      {job ? <p className="tiny">Job {job.job_id}: {job.status}</p> : null}
      {events.length ? <p className="tiny">{events.map((event) => event.event_type).join(' → ')}</p> : null}
      {pollingTimedOut ? <p className="tiny">Polling timed out; refresh job status to continue watching.</p> : null}
      {error ? <p className="error">{error}</p> : null}
    </section>
  );
}

function _eventsOrEmpty(value: unknown): JobEvent[] {
  return Array.isArray(value) ? value as JobEvent[] : [];
}
