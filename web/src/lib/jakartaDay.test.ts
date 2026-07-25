import assert from 'node:assert/strict';
import test from 'node:test';
import {
  jakartaDateKey,
  jakartaDayForWindowEnd,
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
