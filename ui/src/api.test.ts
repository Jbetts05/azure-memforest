import { describe, expect, it, vi, beforeEach } from 'vitest';
import { api, type ApiConfig } from './api';

const config: ApiConfig = {
  baseUrl: 'https://api.example',
  userId: 'user-1',
  devMode: true,
  devTenantId: 'tenant-1',
  devActorId: 'user-1'
};

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn(async (_url: string, init?: RequestInit) => ({
    ok: true,
    json: async () => ({ job_id: 'job-1', status: 'accepted', headers: Object.fromEntries(new Headers(init?.headers).entries()) })
  })));
  vi.stubGlobal('crypto', { randomUUID: () => 'uuid-1' });
});

describe('api client', () => {
  it('sends dev auth headers and idempotency key for ingest', async () => {
    await api.ingestSession(config, 'session-1', 'hello');
    const call = (fetch as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
    const headers = new Headers(call[1].headers);

    expect(call[0]).toBe('https://api.example/v1/users/user-1/sessions/session-1:ingest');
    expect(headers.get('x-tenant-id')).toBe('tenant-1');
    expect(headers.get('x-actor-id')).toBe('user-1');
    expect(headers.get('Idempotency-Key')).toBe('uuid-1');
  });

  it('builds trace URL from query id', async () => {
    await api.trace(config, 'query-1');
    const call = (fetch as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(call[0]).toBe('https://api.example/v1/users/user-1/queries/query-1/trace');
  });
});
