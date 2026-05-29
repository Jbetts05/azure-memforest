import type { FactDetail, QueryResult } from '../api';

export function FactInspector({ result, detail }: { result: QueryResult; detail?: FactDetail | null }) {
  return (
    <section className="inspector" aria-label="Fact inspector">
      <strong>Fact inspector</strong>
      {result.facts.map((fact) => (
        <article key={fact.fact_id}>
          <span>{fact.fact_id}</span>
          <p>{fact.text}</p>
          <small>{String(fact.metadata.forest_version ?? 'latest')}</small>
        </article>
      ))}
      {detail ? (
        <article>
          <span>Locations</span>
          <p>{detail.locations.length} tree location(s)</p>
          <small>{detail.locations.map((loc) => String(loc.tree_id ?? '')).filter(Boolean).join(', ') || 'none'}</small>
        </article>
      ) : null}
    </section>
  );
}
