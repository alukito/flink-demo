import assert from 'node:assert/strict';
import test from 'node:test';
import { requestFreshDashboardToken } from './dashboardToken.ts';
import { resolveWebSocketToken, shouldReconnect } from './webSocketLifecycle.ts';

test('uses the session token only when the override is omitted', () => {
  assert.equal(resolveWebSocketToken(undefined, 'buyer-token'), 'buyer-token');
});

test('keeps an explicit null override so Dashboard opens no user-session socket', () => {
  assert.equal(resolveWebSocketToken(null, 'buyer-token'), null);
});

test('uses an explicit dashboard token instead of the user session', () => {
  assert.equal(resolveWebSocketToken('dashboard-token', 'buyer-token'), 'dashboard-token');
});

test('removes a persisted dashboard token before requesting a fresh in-memory token', async () => {
  const values = new Map([['dash_token', 'expired-token']]);
  const storage = {
    removeItem(key: string) {
      values.delete(key);
    },
  };

  const token = await requestFreshDashboardToken(
    storage,
    async (name, role) => {
      assert.equal(values.has('dash_token'), false);
      assert.equal(name, 'dashboard-test');
      assert.equal(role, 'dashboard');
      return { token: 'fresh-token' };
    },
    'dashboard-test',
  );

  assert.equal(token, 'fresh-token');
  assert.equal(values.has('dash_token'), false);
});

test('does not reconnect after its effect has been disposed', () => {
  assert.equal(
    shouldReconnect({ disposed: true, isCurrentSocket: true, hasToken: true }),
    false,
  );
});

test('does not reconnect when a stale socket closes after replacement', () => {
  assert.equal(
    shouldReconnect({ disposed: false, isCurrentSocket: false, hasToken: true }),
    false,
  );
});

test('does not reconnect without a token', () => {
  assert.equal(
    shouldReconnect({ disposed: false, isCurrentSocket: true, hasToken: false }),
    false,
  );
});

test('reconnects only for the active socket in a live effect with a token', () => {
  assert.equal(
    shouldReconnect({ disposed: false, isCurrentSocket: true, hasToken: true }),
    true,
  );
});
