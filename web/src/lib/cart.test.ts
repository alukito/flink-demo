import assert from 'node:assert/strict';
import test from 'node:test';
import { cartItemCount } from './cart.ts';

test('sums quantities across product lines instead of counting lines', () => {
  const items = [
    { product: { id: 'p-1', name: 'Coffee' }, quantity: 3 },
    { product: { id: 'p-2', name: 'Tea' }, quantity: 1 },
  ];

  assert.equal(cartItemCount(items), 4);
});
