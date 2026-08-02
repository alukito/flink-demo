import assert from 'node:assert/strict';
import test from 'node:test';
import {
  bucketAlertCounts,
  deliveryDurations,
  isCepAlert,
  latestOrderSurge,
  retainRecentAlerts,
  trendingProductCounts,
  upsertCepAlert,
  type CepAlert,
} from './cepAlerts.ts';

const NOW = new Date('2026-08-02T12:00:00.000Z');

function alert(overrides: Partial<CepAlert> = {}): CepAlert {
  return {
    alert_id: 'abandoned_cart:cart-1',
    pattern: 'abandoned_cart',
    detected_at: '2026-08-02T11:00:00.000Z',
    detail: {},
    ...overrides,
  };
}

test('identifies CEP alerts and rejects malformed detection times', () => {
  assert.equal(isCepAlert(alert()), true);
  assert.equal(isCepAlert(alert({ detected_at: 'not-a-time' })), false);
  assert.equal(isCepAlert({ alert_id: 'x', pattern: 'order_surge', detected_at: NOW.toISOString(), detail: [] }), false);
});

test('retains an eight-hour inclusive boundary and rejects stale or future alerts', () => {
  const retained = retainRecentAlerts([
    alert({ alert_id: 'boundary', detected_at: '2026-08-02T04:00:00.000Z' }),
    alert({ alert_id: 'stale', detected_at: '2026-08-02T03:59:59.999Z' }),
    alert({ alert_id: 'future', detected_at: '2026-08-02T12:00:00.001Z' }),
    alert({ alert_id: 'invalid', detected_at: 'invalid' }),
  ], NOW);

  assert.deepEqual(retained.map((entry) => entry.alert_id), ['boundary']);
});

test('replaces duplicate alert IDs immutably and sorts replay deterministically', () => {
  const original = [alert({ alert_id: 'same', detected_at: '2026-08-02T10:00:00.000Z', detail: { count: 1 } })];
  const next = upsertCepAlert(original, alert({ alert_id: 'same', detected_at: '2026-08-02T10:01:00.000Z', detail: { count: 2 } }), NOW);
  const sorted = retainRecentAlerts([
    alert({ alert_id: 'z', detected_at: '2026-08-02T10:30:00.000Z' }),
    alert({ alert_id: 'a', detected_at: '2026-08-02T10:30:00.000Z' }),
    ...next,
  ], NOW);

  assert.equal(original[0].detail.count, 1);
  assert.deepEqual(next, [alert({ alert_id: 'same', detected_at: '2026-08-02T10:01:00.000Z', detail: { count: 2 } })]);
  assert.deepEqual(sorted.map((entry) => entry.alert_id), ['same', 'a', 'z']);
});

test('creates 48 ten-minute abandoned-cart buckets across the last eight hours', () => {
  const buckets = bucketAlertCounts([
    alert({ detected_at: '2026-08-02T04:00:00.000Z' }),
    alert({ alert_id: 'inside', detected_at: '2026-08-02T11:59:59.000Z' }),
    alert({ alert_id: 'stale', detected_at: '2026-08-02T03:59:59.000Z' }),
  ], 'abandoned_cart', NOW);

  assert.equal(buckets.length, 48);
  assert.deepEqual(buckets[0], { start: '2026-08-02T04:00:00.000Z', count: 1 });
  assert.deepEqual(buckets[buckets.length - 1], { start: '2026-08-02T11:50:00.000Z', count: 1 });
});

test('aggregates trending products by product and count without buyer data', () => {
  const products = trendingProductCounts([
    alert({ alert_id: 'p-1', pattern: 'trending_product', detail: { product_id: 'p1', product_name: 'Alpha' } }),
    alert({ alert_id: 'p-2', pattern: 'trending_product', detail: { product_id: 'p2', product_name: 'Beta' } }),
    alert({ alert_id: 'p-3', pattern: 'trending_product', detail: { product_id: 'p1', product_name: 'Alpha' } }),
  ]);

  assert.deepEqual(products, [
    { productId: 'p1', productName: 'Alpha', count: 2 },
    { productId: 'p2', productName: 'Beta', count: 1 },
  ]);
});

test('reports the latest order-surge status and alert count', () => {
  const surge = latestOrderSurge([
    alert({ alert_id: 'surge-1', pattern: 'order_surge', detected_at: '2026-08-02T10:00:00.000Z' }),
    alert({ alert_id: 'surge-2', pattern: 'order_surge', detected_at: '2026-08-02T10:30:00.000Z' }),
  ]);

  assert.deepEqual(surge, { detected: true, count: 2, detectedAt: '2026-08-02T10:30:00.000Z' });
  assert.deepEqual(latestOrderSurge([]), { detected: false, count: 0, detectedAt: null });
});

test('returns deterministic non-negative checkout-to-delivery elapsed seconds', () => {
  const durations = deliveryDurations([
    alert({ alert_id: 'delivery-b', pattern: 'delivery_completed', detected_at: '2026-08-02T10:00:00.000Z', detail: { order_id: 'b', elapsed_seconds: 47 } }),
    alert({ alert_id: 'delivery-a', pattern: 'delivery_completed', detected_at: '2026-08-02T09:00:00.000Z', detail: { order_id: 'a', elapsed_seconds: 11 } }),
    alert({ alert_id: 'invalid-duration', pattern: 'delivery_completed', detail: { elapsed_seconds: -1 } }),
  ]);

  assert.deepEqual(durations, [
    { alertId: 'delivery-a', orderId: 'a', detectedAt: '2026-08-02T09:00:00.000Z', elapsedSeconds: 11 },
    { alertId: 'delivery-b', orderId: 'b', detectedAt: '2026-08-02T10:00:00.000Z', elapsedSeconds: 47 },
  ]);
});
