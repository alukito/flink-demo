import assert from 'node:assert/strict';
import test from 'node:test';
import type { WindowStat } from '../context/EventContext.tsx';
import type { CepAlert } from './cepAlerts.ts';
import { dashboardReducer, initialDashboardData } from '../dashboard/dashboardState.ts';

const OPENED_AT = new Date('2026-08-15T03:02:00.000Z');

function stat(overrides: Partial<WindowStat> = {}): WindowStat {
  return {
    metric: 'listings_count',
    scope: 'window',
    window_end: '2026-08-15T03:05:00.000Z',
    value: 2,
    detail: {},
    ...overrides,
  };
}

function alert(overrides: Partial<CepAlert> = {}): CepAlert {
  return {
    alert_id: 'abandoned_cart:cart-1',
    pattern: 'abandoned_cart',
    detected_at: '2026-08-15T03:01:00.000Z',
    detail: {},
    ...overrides,
  };
}

test('replaces one metric snapshot by metric scope and window end', () => {
  const first = stat({ value: 2 });
  const latest = stat({ value: 9 });
  const state = dashboardReducer(dashboardReducer(initialDashboardData(OPENED_AT), { type: 'message', message: first }), { type: 'message', message: latest });

  assert.deepEqual(state.stats, [latest]);
});

test('retains the latest 24 windows for each metric independently', () => {
  let state = initialDashboardData(OPENED_AT);
  for (let index = 0; index < 25; index += 1) {
    const windowEnd = new Date(Date.UTC(2026, 7, 15, 3, 5 + index * 5)).toISOString();
    state = dashboardReducer(state, { type: 'message', message: stat({ metric: 'listings_count', window_end: windowEnd, value: index }) });
    state = dashboardReducer(state, { type: 'message', message: stat({ metric: 'cart_adds_count', window_end: windowEnd, value: index }) });
  }

  assert.equal(state.stats.length, 48);
  assert.deepEqual(
    state.stats.filter((entry) => entry.metric === 'listings_count').map((entry) => entry.value),
    Array.from({ length: 24 }, (_, index) => index + 1),
  );
  assert.deepEqual(
    state.stats.filter((entry) => entry.metric === 'cart_adds_count').map((entry) => entry.value),
    Array.from({ length: 24 }, (_, index) => index + 1),
  );
});

test('upserts CEP alerts immutably and prunes alerts older than eight hours', () => {
  const state = initialDashboardData(OPENED_AT);
  const first = dashboardReducer(state, { type: 'message', message: alert({ detail: { count: 1 } }) });
  const replaced = dashboardReducer(first, { type: 'message', message: alert({ detail: { count: 2 } }) });
  const pruned = dashboardReducer(replaced, { type: 'tick', now: new Date('2026-08-15T11:02:00.001Z') });

  assert.notEqual(replaced.alerts, first.alerts);
  assert.deepEqual(first.alerts[0]?.detail, { count: 1 });
  assert.deepEqual(replaced.alerts, [alert({ detail: { count: 2 } })]);
  assert.deepEqual(pruned.alerts, []);
});

test('clear returns empty metrics and alerts while preserving session timing', () => {
  const state = initialDashboardData(OPENED_AT);
  const cleared = dashboardReducer({ ...state, stats: [stat()], alerts: [alert()] }, { type: 'clear' });

  assert.equal(cleared.sessionStart, state.sessionStart);
  assert.deepEqual(cleared.stats, []);
  assert.deepEqual(cleared.alerts, []);
});
