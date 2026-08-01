import assert from 'node:assert/strict';
import test from 'node:test';
import {
  jakartaDateKey,
  jakartaDayForWindowEnd,
  jakartaRefreshSnapshot,
  millisecondsUntilNextJakartaMidnight,
} from './jakartaDay.ts';

test('Jakarta date changes at 17:00 UTC', () => {
  assert.equal(jakartaDateKey(new Date('2026-07-25T16:59:59Z')), '2026-07-25');
  assert.equal(jakartaDateKey(new Date('2026-07-25T17:00:00Z')), '2026-07-26');
});

test('daily window belongs to the Jakarta day immediately before its end', () => {
  assert.equal(jakartaDayForWindowEnd('2026-07-25T17:00:00Z'), '2026-07-25');
  assert.equal(jakartaDayForWindowEnd('not-a-date'), null);
});

test('next Jakarta midnight delay reaches the 17:00 UTC boundary', () => {
  assert.equal(
    millisecondsUntilNextJakartaMidnight(new Date('2026-07-25T16:59:59Z')),
    1000,
  );
});

test('refresh snapshot corrects a render that crossed Jakarta midnight before effect setup', () => {
  const renderedDay = jakartaDateKey(new Date('2026-07-25T16:59:59Z'));
  const snapshot = jakartaRefreshSnapshot(new Date('2026-07-25T17:00:00Z'));

  assert.notEqual(snapshot.day, renderedDay);
  assert.deepEqual(snapshot, { day: '2026-07-26', delay: 24 * 60 * 60 * 1000 });
});
