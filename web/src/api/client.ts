const API_BASE = '/api';

export interface SessionResponse {
  token: string;
  name: string;
  role: string;
}

export async function createSession(name: string, role: string): Promise<SessionResponse> {
  const resp = await fetch(`${API_BASE}/session`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, role }),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `Failed to create session (status ${resp.status})`);
  }
  return resp.json();
}

export async function apiGet(path: string, token: string): Promise<Response> {
  return fetch(`${API_BASE}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
}

export async function apiPost(path: string, token: string, body?: unknown): Promise<Response> {
  return fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
}
