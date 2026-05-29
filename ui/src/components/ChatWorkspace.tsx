import { useEffect, useMemo, useRef, useState } from 'react';
import { api, ApiError, type ApiConfig, type ChatResult, type JobEvent, type JobStatus, type Turn, type TurnInput } from '../api';

type ChatMessage = {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  memoryStatus?: ChatResult['memory_status'];
};

const terminal = new Set(['succeeded', 'failed', 'cancelled']);

export function ChatWorkspace({
  config,
  initialTurns,
  onChatResult,
  onIngestComplete
}: {
  config: ApiConfig | null;
  initialTurns: Turn[];
  onChatResult: (result: ChatResult) => void;
  onIngestComplete: () => Promise<void>;
}) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [draft, setDraft] = useState('');
  const [job, setJob] = useState<JobStatus | null>(null);
  const [events, setEvents] = useState<JobEvent[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [extracting, setExtracting] = useState(false);
  const [pollingTimedOut, setPollingTimedOut] = useState(false);
  const bottomRef = useRef<HTMLDivElement | null>(null);
  const ingestSeqRef = useRef(0);
  const activeSessionId = useMemo(() => _activeSessionId(config), [config]);

  useEffect(() => {
    if (!messages.length && initialTurns.length) {
      setMessages(initialTurns.map((turn) => ({
        id: turn.turn_id,
        role: turn.role === 'assistant' ? 'assistant' : 'user',
        content: turn.content,
        timestamp: turn.timestamp ?? new Date().toISOString()
      })));
    }
  }, [initialTurns, messages.length]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [messages, busy]);

  async function send() {
    if (!config || busy || !draft.trim()) return;
    const content = draft.trim();
    const now = new Date().toISOString();
    const userMessage: ChatMessage = { id: `local-user-${crypto.randomUUID()}`, role: 'user', content, timestamp: now };
    const history = [...messages, userMessage];
    setMessages(history);
    setDraft('');
    setBusy(true);
    setError(null);
    setPollingTimedOut(false);
    try {
      const result = await _chat(config, content, _turnInputs(history.slice(0, -1)));
      onChatResult(result);
      const assistantMessage: ChatMessage = {
        id: `local-assistant-${crypto.randomUUID()}`,
        role: 'assistant',
        content: result.reply,
        timestamp: new Date().toISOString(),
        memoryStatus: result.memory_status
      };
      setMessages((current) => [...current, assistantMessage]);
      void ingestWindow(config, activeSessionId, history);
    } catch (err) {
      setError(_friendlyError(err));
      const fallback: ChatMessage = {
        id: `local-assistant-${crypto.randomUUID()}`,
        role: 'assistant',
        content: 'I can keep the conversation here, but the live chat service is not available for this turn.',
        timestamp: new Date().toISOString()
      };
      setMessages((current) => [...current, fallback]);
    } finally {
      setBusy(false);
    }
  }

  async function ingestWindow(config: ApiConfig, rootSessionId: string, history: ChatMessage[]) {
    const seq = ingestSeqRef.current + 1;
    ingestSeqRef.current = seq;
    setExtracting(true);
    setPollingTimedOut(false);
    setEvents([]);
    try {
      const userMessages = history.filter((message) => message.role === 'user');
      const latestUserIndex = userMessages.length - 1;
      const windowIndex = Math.max(0, Math.floor(latestUserIndex / 6));
      const windowUserTurns = userMessages.slice(windowIndex * 6, windowIndex * 6 + 6);
      const turns = windowUserTurns.map((message): TurnInput => ({
        role: 'user',
        content: message.content,
        timestamp: message.timestamp,
        content_id: message.id,
        metadata: { source: 'chat-ui' }
      }));
      const accepted = await api.ingestSession(config, `${rootSessionId}-window-${windowIndex}`, turns, { active_session_id: rootSessionId, window_index: windowIndex });
      let current = await api.job(config, accepted.job_id);
      if (ingestSeqRef.current === seq) {
        setJob(current);
        setEvents(_eventsOrEmpty(await api.jobEvents(config, accepted.job_id)));
      }
      for (let i = 0; i < 75 && !terminal.has(current.status); i += 1) {
        await new Promise((resolve) => setTimeout(resolve, i < 10 ? 2000 : 4000));
        current = await api.job(config, accepted.job_id);
        if (ingestSeqRef.current !== seq) return;
        setJob(current);
        setEvents(_eventsOrEmpty(await api.jobEvents(config, accepted.job_id)));
      }
      if (ingestSeqRef.current !== seq) return;
      if (current.status === 'succeeded') await onIngestComplete();
      if (current.status === 'failed') setError(current.error ?? 'Memory extraction failed.');
      if (!terminal.has(current.status)) setPollingTimedOut(true);
    } catch (err) {
      setError(_friendlyError(err));
    } finally {
      if (ingestSeqRef.current === seq) setExtracting(false);
    }
  }

  return (
    <section className="chatWorkspace" aria-label="MemForest chat">
      <div className="chatScroller">
        {!messages.length ? (
          <div className="emptyChat">
            <p className="kicker">MemForest chat</p>
            <h1>Talk normally. Memory builds quietly.</h1>
            <p>Ask questions, share context, or continue a thread. MemForest extracts durable memories from the conversation and uses them in future replies.</p>
          </div>
        ) : null}
        {messages.map((message) => (
          <article key={message.id} className={`bubble ${message.role}`}>
            <span>{message.role === 'assistant' ? 'MemForest' : 'You'}</span>
            <p>{message.content}</p>
            {message.memoryStatus ? <small>{_memoryLabel(message.memoryStatus)}</small> : null}
          </article>
        ))}
        {busy ? <article className="bubble assistant pending"><span>MemForest</span><p>Thinking, then updating memory...</p></article> : null}
        <div ref={bottomRef} />
      </div>
      <form className="composer" onSubmit={(event) => { event.preventDefault(); void send(); }}>
        <textarea
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && !event.shiftKey) {
              event.preventDefault();
              void send();
            }
          }}
          placeholder="Message MemForest..."
          rows={1}
        />
        <button disabled={!config || busy || !draft.trim()} type="submit">Send</button>
      </form>
      <div className="chatStatus" aria-live="polite">
        <span>{extracting ? 'Updating memory in the background...' : job ? `Memory job ${job.status}` : 'Memory extraction runs after each message.'}</span>
        {events.length ? <span>{events.map((event) => event.event_type).join(' -> ')}</span> : null}
        {pollingTimedOut ? <span>Still updating memory in the background.</span> : null}
        {error ? <span className="error">{error}</span> : null}
      </div>
    </section>
  );
}

async function _chat(config: ApiConfig, content: string, turns: TurnInput[]): Promise<ChatResult> {
  return api.chat(config, content, turns);
}

function _activeSessionId(config: ApiConfig | null): string {
  if (!config) return 'chat-pending';
  const key = `memforest.activeSession.${config.userId}`;
  const existing = localStorage.getItem(key);
  if (existing) return existing;
  const created = `chat-${crypto.randomUUID()}`;
  localStorage.setItem(key, created);
  return created;
}

function _turnInputs(messages: ChatMessage[]): TurnInput[] {
  return messages.slice(-12).map((message) => ({
    role: message.role,
    content: message.content,
    timestamp: message.timestamp,
    content_id: message.id,
    metadata: { source: 'chat-ui-context' }
  }));
}

function _eventsOrEmpty(value: unknown): JobEvent[] {
  return Array.isArray(value) ? value as JobEvent[] : [];
}

function _memoryLabel(status: ChatResult['memory_status']): string {
  if (status === 'grounded') return 'Answered with recalled memory';
  if (status === 'no_forest') return 'First-run memory will be extracted from this chat';
  if (status === 'no_facts') return 'No relevant memory found yet';
  return 'Memory unavailable for this turn';
}

function _friendlyError(err: unknown): string {
  if (err instanceof ApiError && err.code === 'forest_not_found') return 'Start chatting; the first forest is still being built.';
  return err instanceof Error ? err.message : String(err);
}
