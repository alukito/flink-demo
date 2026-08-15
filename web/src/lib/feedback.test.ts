import assert from 'node:assert/strict';
import test from 'node:test';
import { createFeedback, expireFeedback } from './feedback.ts';

test('new feedback replaces prior feedback and expires only its own generation', () => {
  const first = createFeedback('success', 'Product added');
  const second = createFeedback('success', 'Order confirmed');

  assert.notEqual(first.id, second.id);
  assert.equal(expireFeedback(second, first.id), second);
  assert.equal(expireFeedback(second, second.id), null);
});
