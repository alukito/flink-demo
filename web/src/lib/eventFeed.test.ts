import assert from 'node:assert/strict';
import test from 'node:test';
import type { EventEnvelope } from '../context/EventContext.tsx';
import { appendUniqueEvent } from './eventFeed.ts';

function event(eventId: string, payload: Record<string, unknown> = {}): EventEnvelope {
  return {
    event_id: eventId,
    event_type: 'product.listed',
    actor_id: 'seller-1',
    actor_role: 'seller',
    timestamp: '2026-08-08T10:00:00.000Z',
    payload,
  };
}

test('prepends a new event envelope without changing the retained events', () => {
  const retained = [event('event-1', { product_id: 'p-1' })];

  const next = appendUniqueEvent(retained, event('event-2', { product_id: 'p-2' }), 100);

  assert.deepEqual(next.map((entry) => entry.event_id), ['event-2', 'event-1']);
  assert.deepEqual(retained.map((entry) => entry.event_id), ['event-1']);
});

test('ignores a repeated event ID even when its payload differs', () => {
  const retained = [event('event-1', { product_id: 'p-1', quantity: 1 })];

  const next = appendUniqueEvent(retained, event('event-1', { product_id: 'p-99', quantity: 99 }), 100);

  assert.strictEqual(next, retained);
  assert.deepEqual(next, [event('event-1', { product_id: 'p-1', quantity: 1 })]);
});

test('retains at most one hundred newest unique events', () => {
  const retained = Array.from({ length: 100 }, (_, index) => event(`event-${index}`));

  const next = appendUniqueEvent(retained, event('event-new'), 100);

  assert.equal(next.length, 100);
  assert.equal(next[0].event_id, 'event-new');
  assert.equal(next[99].event_id, 'event-98');
});
