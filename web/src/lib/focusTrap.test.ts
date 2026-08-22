import assert from 'node:assert/strict';
import test from 'node:test';
import { nextFocusIndex } from './focusTrap.ts';

test('wraps focus from last to first and first to last', () => {
  assert.equal(nextFocusIndex(2, 3, false), 0);
  assert.equal(nextFocusIndex(0, 3, true), 2);
});
