import type { Session, Turn } from '../api';

export function SessionBrowser({ sessions, turns }: { sessions: Session[]; turns: Turn[] }) {
  return (
    <section className="chat" aria-label="Session browser">
      <strong>Session browser</strong>
      {(sessions.length ? sessions : [{ session_id: 'No committed sessions loaded', cell_ids: [], metadata: {} }]).map((session) => (
        <p key={session.session_id} className="tiny">{session.session_id}</p>
      ))}
      {turns.slice(0, 4).map((turn) => (
        <p key={turn.turn_id} className={`msg ${turn.role === 'assistant' ? 'agent' : 'user'}`}>{turn.content}</p>
      ))}
    </section>
  );
}
