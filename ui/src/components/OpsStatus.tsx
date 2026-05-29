import type { Manifest } from '../api';

export function OpsStatus({ manifest, authMode, error }: { manifest?: Manifest | null; authMode: string; error?: string | null }) {
  const checks = [
    ['API', error ? 'Error' : 'Reachable'],
    ['Forest', manifest ? `g${manifest.generation}` : 'Unknown'],
    ['Version', manifest?.forest_version ?? 'No manifest'],
    ['Auth', authMode],
    ['UI', 'Feature-gated']
  ];

  return (
    <section className="ops" aria-label="Operations status">
      <strong>Ops status</strong>
      {checks.map(([label, value]) => (
        <div key={label}>
          <span>{label}</span>
          <b>{value}</b>
        </div>
      ))}
    </section>
  );
}
