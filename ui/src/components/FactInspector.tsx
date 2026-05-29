import type { FactDetail, QueryResult } from '../api';

export function FactInspector({ result, detail, forestFacts }: { result: QueryResult; detail?: FactDetail | null; forestFacts: FactDetail[] }) {
  return (
    <section className="inspector" aria-label="Fact inspector">
      <strong>Fact inspector</strong>
      <p className="tiny">{result.facts.length ? 'Facts used by the latest reply.' : 'No reply-grounding facts for the latest turn.'}</p>
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
      {forestFacts.length ? (
        <>
          <strong className="subhead">Facts in latest forest</strong>
          {forestFacts.map((fact) => (
            <article key={fact.fact_id}>
              <span>{fact.fact_id}</span>
              <p>{fact.text}</p>
              <small>{fact.locations.length} tree location(s)</small>
            </article>
          ))}
        </>
      ) : null}
    </section>
  );
}
