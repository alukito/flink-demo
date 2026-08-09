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

test('round-trips and clears the UUID session identity', () => {
  const storage = new MemoryStorage();
  writeSession({ id: 'buyer-a', token: 'token-1', name: 'Ada', role: 'buyer' }, storage);

  assert.deepEqual(readSession(storage), { id: 'buyer-a', token: 'token-1', name: 'Ada', role: 'buyer' });
  assert.equal(storage.getItem('id'), 'buyer-a');
  assert.equal(storage.getItem('token'), 'token-1');
  assert.equal(storage.getItem('name'), 'Ada');
  assert.equal(storage.getItem('role'), 'buyer');

  clearSession(storage);

  assert.equal(readSession(storage), null);
  assert.equal(storage.getItem('id'), null);
  assert.equal(storage.getItem('token'), null);
  assert.equal(storage.getItem('name'), null);
  assert.equal(storage.getItem('role'), null);
});

test('authorizes only a complete UUID session with the requested role', () => {
  const cases: Array<{ id: string | null; token: string | null; name: string | null; role: string | null; authorized: boolean }> = [
    { id: 'buyer-a', token: 'token-1', name: 'Ada', role: 'buyer', authorized: true },
    { id: null, token: 'token-1', name: 'Ada', role: 'buyer', authorized: false },
    { id: 'buyer-a', token: null, name: 'Ada', role: 'buyer', authorized: false },
    { id: 'buyer-a', token: 'token-1', name: 'Ada', role: null, authorized: false },
    { id: 'buyer-a', token: 'token-1', name: 'Ada', role: 'seller', authorized: false },
    { id: 'buyer-a', token: 'token-1', name: 'Ada', role: 'administrator', authorized: false },
  ];

  for (const entry of cases) {
    const storage = new MemoryStorage();
    if (entry.id !== null) storage.setItem('id', entry.id);
    if (entry.token !== null) storage.setItem('token', entry.token);
    if (entry.name !== null) storage.setItem('name', entry.name);
    if (entry.role !== null) storage.setItem('role', entry.role);

    assert.equal(hasRequiredRole('buyer', storage), entry.authorized);
  }
});

test('keeps a UUID session authorized when its display name is missing', () => {
  const storage = new MemoryStorage();
  storage.setItem('id', 'buyer-a');
  storage.setItem('token', 'token-1');
  storage.setItem('role', 'buyer');

  assert.deepEqual(readSession(storage), { id: 'buyer-a', token: 'token-1', name: null, role: 'buyer' });
  assert.equal(hasRequiredRole('buyer', storage), true);
});
