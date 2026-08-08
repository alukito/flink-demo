import assert from 'node:assert/strict';
import test from 'node:test';
import { clearSession, hasRequiredRole, readSession, writeSession, type StorageLike } from './session.ts';

class MemoryStorage implements StorageLike {
  private readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }
}

test('reads and clears all three session fields', () => {
  const storage = new MemoryStorage();
  writeSession({ token: 'token-1', name: 'Ada', role: 'buyer' }, storage);

  assert.deepEqual(readSession(storage), { token: 'token-1', name: 'Ada', role: 'buyer' });
  assert.equal(storage.getItem('token'), 'token-1');
  assert.equal(storage.getItem('name'), 'Ada');
  assert.equal(storage.getItem('role'), 'buyer');

  clearSession(storage);

  assert.equal(readSession(storage), null);
  assert.equal(storage.getItem('token'), null);
  assert.equal(storage.getItem('name'), null);
  assert.equal(storage.getItem('role'), null);
});

test('authorizes only a complete session with the requested role', () => {
  const cases: Array<{ token: string | null; name: string | null; role: string | null; authorized: boolean }> = [
    { token: 'token-1', name: 'Ada', role: 'buyer', authorized: true },
    { token: null, name: 'Ada', role: 'buyer', authorized: false },
    { token: 'token-1', name: null, role: 'buyer', authorized: false },
    { token: 'token-1', name: 'Ada', role: null, authorized: false },
    { token: 'token-1', name: 'Ada', role: 'seller', authorized: false },
    { token: 'token-1', name: 'Ada', role: 'administrator', authorized: false },
  ];

  for (const entry of cases) {
    const storage = new MemoryStorage();
    if (entry.token !== null) storage.setItem('token', entry.token);
    if (entry.name !== null) storage.setItem('name', entry.name);
    if (entry.role !== null) storage.setItem('role', entry.role);

    assert.equal(hasRequiredRole('buyer', storage), entry.authorized);
  }
});
