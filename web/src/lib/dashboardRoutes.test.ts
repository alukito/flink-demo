import assert from 'node:assert/strict';
import test from 'node:test';
import { DASHBOARD_STEPS, dashboardAdjacentPath } from '../dashboard/dashboardRoutes.ts';

test('orders the three presentation routes from live input to pattern signals', () => {
  assert.deepEqual(DASHBOARD_STEPS.map((step) => step.path), [
    '/dashboard/live',
    '/dashboard/windows',
    '/dashboard/patterns',
  ]);
});

test('finds the adjacent presentation route without leaving the sequence', () => {
  assert.equal(dashboardAdjacentPath('/dashboard/windows', 1), '/dashboard/patterns');
  assert.equal(dashboardAdjacentPath('/dashboard/live', -1), null);
  assert.equal(dashboardAdjacentPath('/dashboard/unknown', 1), null);
});
