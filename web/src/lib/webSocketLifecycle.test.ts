import assert from 'node:assert/strict';
import test from 'node:test';
import { shouldReconnect } from './webSocketLifecycle.ts';

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
