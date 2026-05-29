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
    await api.ingestSession(config, 'session-1', [{ role: 'user', content: 'hello', content_id: 'turn-1' }]);
    const call = (fetch as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
    const headers = new Headers(call[1].headers);
    const body = JSON.parse(String(call[1].body));

    expect(call[0]).toBe('https://api.example/v1/users/user-1/sessions/session-1:ingest');
    expect(headers.get('x-tenant-id')).toBe('tenant-1');
    expect(headers.get('x-actor-id')).toBe('user-1');
    expect(headers.get('Idempotency-Key')).toBe('uuid-1');
    expect(body.turns[0].content_id).toBe('turn-1');
    expect(body.metadata.source).toBe('chat-ui');
  });

  it('posts chat turns to chat endpoint', async () => {
    await api.chat(config, 'hello', [{ role: 'user', content: 'hello', content_id: 'turn-1' }]);
    const call = (fetch as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
    const body = JSON.parse(String(call[1].body));

    expect(call[0]).toBe('https://api.example/v1/users/user-1/chat');
    expect(body.message).toBe('hello');
    expect(body.turns[0].content_id).toBe('turn-1');
    expect(body.trace_requested).toBe(true);
  });

  it('passes root node id when loading a subtree', async () => {
    await api.subtree(config, 'forest-1', 'tree-1', 'root node');
    const call = (fetch as unknown as ReturnType<typeof vi.fn>).mock.calls[0];

    expect(call[0]).toBe('https://api.example/v1/users/user-1/forest/forest-1/trees/tree-1/subtree?root_node_id=root%20node&max_depth=4&max_nodes=80');
  });

  it('sends an idempotent clear memory request', async () => {
    await api.clearMemory(config);
    const call = (fetch as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
    const headers = new Headers(call[1].headers);

    expect(call[0]).toBe('https://api.example/v1/users/user-1/memory:clear');
    expect(call[1].method).toBe('POST');
    expect(headers.get('Idempotency-Key')).toBe('uuid-1');
  });

  it('builds trace URL from query id', async () => {
    await api.trace(config, 'query-1');
    const call = (fetch as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(call[0]).toBe('https://api.example/v1/users/user-1/queries/query-1/trace');
  });
});
