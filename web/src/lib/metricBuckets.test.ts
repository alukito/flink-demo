import assert from 'node:assert/strict';
import test from 'node:test';
import type { WindowStat } from '../context/EventContext.tsx';
import {
  activeWindowEnd,
  formatJakartaBucketRange,
  formatJakartaBucketStart,
  metricBuckets,
} from './metricBuckets.ts';

const NOW = new Date('2026-08-08T07:10:00.000Z');

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

test('normalizes sparse snapshots into the approved aligned five-minute sequence', () => {
  const buckets = metricBuckets([
    stat({ window_end: '2026-08-08T07:05:00.000Z', value: 2 }),
    stat({ window_end: '2026-08-08T07:15:00.000Z', value: 1, detail: { product: 'coffee' } }),
  ], NOW);

  assert.deepEqual(buckets.slice(-3), [
    { start: '2026-08-08T07:00:00.000Z', windowEnd: '2026-08-08T07:05:00.000Z', value: 2, detail: {} },
    { start: '2026-08-08T07:05:00.000Z', windowEnd: '2026-08-08T07:10:00.000Z', value: 0, detail: {} },
    { start: '2026-08-08T07:10:00.000Z', windowEnd: '2026-08-08T07:15:00.000Z', value: 1, detail: { product: 'coffee' } },
  ]);
});

test('uses the latest snapshot for a matching window end', () => {
  const buckets = metricBuckets([
    stat({ value: 2, detail: { phase: 'first' } }),
    stat({ value: 9, detail: { phase: 'latest' } }),
  ], NOW);

  assert.deepEqual(buckets[buckets.length - 3], {
    start: '2026-08-08T07:00:00.000Z',
    windowEnd: '2026-08-08T07:05:00.000Z',
    value: 9,
    detail: { phase: 'latest' },
  });
});

test('makes the exact aligned boundary the end of the previous bucket and active end of the next', () => {
  assert.equal(activeWindowEnd(NOW), '2026-08-08T07:15:00.000Z');

  const buckets = metricBuckets([], NOW);

  assert.deepEqual(buckets[buckets.length - 1], {
    start: '2026-08-08T07:10:00.000Z',
    windowEnd: '2026-08-08T07:15:00.000Z',
    value: 0,
    detail: {},
  });
});

test('ignores invalid and out-of-range snapshot timestamps while returning 24 ordered buckets', () => {
  const buckets = metricBuckets([
    stat({ window_end: 'not-a-timestamp', value: 99 }),
    stat({ window_end: '2026-08-08T05:15:00.000Z', value: 88 }),
    stat({ window_end: '2026-08-08T07:20:00.000Z', value: 77 }),
  ], NOW);

  assert.equal(buckets.length, 24);
  assert.deepEqual(buckets[0], {
    start: '2026-08-08T05:15:00.000Z',
    windowEnd: '2026-08-08T05:20:00.000Z',
    value: 0,
    detail: {},
  });
  assert.deepEqual(buckets[buckets.length - 1], {
    start: '2026-08-08T07:10:00.000Z',
    windowEnd: '2026-08-08T07:15:00.000Z',
    value: 0,
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
