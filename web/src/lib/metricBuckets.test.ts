import assert from 'node:assert/strict';
import test from 'node:test';
import type { WindowStat } from '../context/EventContext.tsx';
import {
  activeWindowEnd,
  dashboardSessionStart,
  formatJakartaBucketRange,
  formatJakartaBucketStart,
  metricBuckets,
} from './metricBuckets.ts';

const OPENED_AT = new Date('2026-08-08T07:02:00.000Z');
const SESSION_START = '2026-08-08T07:05:00.000Z';

function stat(overrides: Partial<WindowStat> = {}): WindowStat {
  return {
    metric: 'listings_count',
    scope: 'window',
    window_end: '2026-08-08T07:05:00.000Z',
    value: 2,
    detail: {},
    ...overrides,
  };
}

test('starts with only the active aligned window and hides snapshots from before the dashboard session', () => {
  const buckets = metricBuckets([
    stat({ window_end: '2026-08-08T07:00:00.000Z', value: 99 }),
    stat({ window_end: '2026-08-08T07:05:00.000Z', value: 2 }),
  ], dashboardSessionStart(OPENED_AT), OPENED_AT);

  assert.equal(dashboardSessionStart(OPENED_AT), SESSION_START);
  assert.deepEqual(buckets, [
    { start: '2026-08-08T07:00:00.000Z', windowEnd: SESSION_START, value: 2, detail: {} },
  ]);
});

test('grows through elapsed windows without gaps after the dashboard opens', () => {
  const buckets = metricBuckets([
    stat({ window_end: '2026-08-08T07:05:00.000Z', value: 2 }),
    stat({ window_end: '2026-08-08T07:10:00.000Z', value: 4 }),
  ], SESSION_START, new Date('2026-08-08T07:11:00.000Z'));

  assert.deepEqual(buckets, [
    { start: '2026-08-08T07:00:00.000Z', windowEnd: '2026-08-08T07:05:00.000Z', value: 2, detail: {} },
    { start: '2026-08-08T07:05:00.000Z', windowEnd: '2026-08-08T07:10:00.000Z', value: 4, detail: {} },
    { start: '2026-08-08T07:10:00.000Z', windowEnd: '2026-08-08T07:15:00.000Z', value: 0, detail: {} },
  ]);
});

test('uses the latest same-window snapshot without adding a bucket', () => {
  const buckets = metricBuckets([
    stat({ value: 2, detail: { phase: 'first' } }),
    stat({ value: 9, detail: { phase: 'latest' } }),
  ], SESSION_START, OPENED_AT);

  assert.deepEqual(buckets, [{
    start: '2026-08-08T07:00:00.000Z',
    windowEnd: '2026-08-08T07:05:00.000Z',
    value: 9,
    detail: { phase: 'latest' },
  }]);
});

test('makes the exact aligned boundary the end of the previous bucket and active end of the next', () => {
  const boundary = new Date('2026-08-08T07:10:00.000Z');
  assert.equal(activeWindowEnd(boundary), '2026-08-08T07:15:00.000Z');

  const buckets = metricBuckets([], SESSION_START, boundary);

  assert.deepEqual(buckets[buckets.length - 1], {
    start: '2026-08-08T07:10:00.000Z',
    windowEnd: '2026-08-08T07:15:00.000Z',
    value: 0,
    detail: {},
  });
});

test('retains the newest 24 elapsed windows after session history overflows', () => {
  const buckets = metricBuckets([
    stat({ window_end: 'not-a-timestamp', value: 99 }),
    stat({ window_end: '2026-08-08T07:15:00.000Z', value: 88 }),
    stat({ window_end: '2026-08-08T07:20:00.000Z', value: 6 }),
    stat({ window_end: '2026-08-08T09:15:00.000Z', value: 7 }),
    stat({ window_end: '2026-08-08T09:20:00.000Z', value: 77 }),
  ], SESSION_START, new Date('2026-08-08T09:11:00.000Z'));

  assert.equal(buckets.length, 24);
  assert.deepEqual(buckets[0], {
    start: '2026-08-08T07:15:00.000Z',
    windowEnd: '2026-08-08T07:20:00.000Z',
    value: 6,
    detail: {},
  });
  assert.deepEqual(buckets[buckets.length - 1], {
    start: '2026-08-08T09:10:00.000Z',
    windowEnd: '2026-08-08T09:15:00.000Z',
    value: 7,
    detail: {},
  });
  assert.equal(buckets.every((bucket, index) => index === 0 || bucket.start > buckets[index - 1].start), true);
});

test('formats bucket labels in Jakarta across a UTC-to-WIB date boundary', () => {
  assert.equal(formatJakartaBucketStart('2026-08-08T07:05:00.000Z'), '14:00');
  assert.equal(formatJakartaBucketRange('2026-08-08T16:55:00.000Z'), '23:50–23:55 WIB');
  assert.equal(formatJakartaBucketRange('2026-08-08T17:00:00.000Z'), '23:55–00:00 WIB');
  assert.equal(formatJakartaBucketStart('not-a-timestamp'), null);
  assert.equal(formatJakartaBucketRange('not-a-timestamp'), null);
});
