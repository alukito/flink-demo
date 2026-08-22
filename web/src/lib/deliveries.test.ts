import assert from 'node:assert/strict';
import test from 'node:test';
import { copyDeliveries, deliveryReadiness, secondsUntilReady } from './deliveries.ts';

test('describes missing or invalid readiness without claiming zero seconds', () => {
  const now = new Date('2026-08-09T00:00:00.000Z');

  assert.deepEqual(deliveryReadiness(undefined, now), {
    kind: 'unavailable',
    label: 'Readiness unavailable',
  });
  assert.deepEqual(deliveryReadiness('invalid', now), {
    kind: 'unavailable',
    label: 'Readiness unavailable',
  });
});

test('describes future and elapsed readiness with explicit textual states', () => {
  const now = new Date('2026-08-09T00:00:00.000Z');

  assert.deepEqual(deliveryReadiness('2026-08-09T00:00:05.800Z', now), {
    kind: 'waiting',
    seconds: 6,
    label: 'Ready in 6s',
  });
  assert.deepEqual(deliveryReadiness('2026-08-09T00:00:00.000Z', now), {
    kind: 'ready',
    label: 'Ready to deliver',
  });
});

test('rounds a 5.8-second readiness remainder up to six seconds', () => {
  const now = new Date('2026-08-09T00:00:00.000Z');

  assert.equal(secondsUntilReady('2026-08-09T00:00:05.800Z', now), 6);
});

test('returns zero when a delivery is ready or has an invalid readiness timestamp', () => {
  const now = new Date('2026-08-09T00:00:10.000Z');

  assert.equal(secondsUntilReady('2026-08-09T00:00:10.000Z', now), 0);
  assert.equal(secondsUntilReady('not-a-timestamp', now), 0);
});

test('copies delivery response arrays before UI state can change them', () => {
  const response = {
    active: [{
      id: 'active-1', buyer_id: 'buyer-1', seller_id: 'seller-1',
      items: [], shipping_address: 'Jakarta', status: 'picked', created_at: '2026-08-09T00:00:00.000Z',
    }],
    history: [{
      id: 'history-1', buyer_id: 'buyer-2', seller_id: 'seller-2',
      items: [], shipping_address: 'Bandung', status: 'delivered', created_at: '2026-08-09T00:00:00.000Z',
    }],
  };

  const deliveries = copyDeliveries(response);
  deliveries.active.pop();
  deliveries.history.push({
    id: 'history-2', buyer_id: 'buyer-3', seller_id: 'seller-3',
    items: [], shipping_address: 'Surabaya', status: 'delivered', created_at: '2026-08-09T00:00:00.000Z',
  });

  assert.deepEqual(response, {
    active: [{
      id: 'active-1', buyer_id: 'buyer-1', seller_id: 'seller-1',
      items: [], shipping_address: 'Jakarta', status: 'picked', created_at: '2026-08-09T00:00:00.000Z',
    }],
    history: [{
      id: 'history-1', buyer_id: 'buyer-2', seller_id: 'seller-2',
      items: [], shipping_address: 'Bandung', status: 'delivered', created_at: '2026-08-09T00:00:00.000Z',
    }],
  });
  assert.notStrictEqual(deliveries.active, response.active);
  assert.notStrictEqual(deliveries.history, response.history);
});
