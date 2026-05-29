import type { QueryTrace } from '../api';

export function QueryTraceViz({ trace }: { trace?: QueryTrace | null }) {
  const events = trace?.event_summaries ?? [];
  return (
    <section className="tracePanel" aria-label="Query trace visualization">
      <strong>Trace path</strong>
      <p className="tiny">{trace ? `${trace.event_count} events · ${trace.forest_version}` : 'No trace loaded'}</p>
      <ol>
        {events.slice(0, 8).map((event, index) => (
          <li key={index}>
            <span>{String(event.event_type ?? 'event')}</span>
            <small>{String(event.tree_id ?? event.fact_id ?? event.node_id ?? '')}</small>
          </li>
        ))}
      </ol>
    </section>
  );
}
