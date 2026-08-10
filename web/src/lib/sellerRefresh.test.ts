import assert from 'node:assert/strict';
import test from 'node:test';
import { loadLatestSellerOrders } from './sellerRefresh.ts';

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve: (value: T) => void = () => {};
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

test('does not publish an older seller order response after a newer refresh', async () => {
  let latestGeneration = 1;
  const oldResponse = deferred<{ ok: boolean; json: () => Promise<string[]> }>();
  const published: string[][] = [];

  const oldRefresh = loadLatestSellerOrders({
    generation: 1,
    getLatestGeneration: () => latestGeneration,
    listOrders: () => oldResponse.promise,
    commit: (orders) => published.push(orders),
  });

  latestGeneration = 2;
  await loadLatestSellerOrders({
    generation: 2,
    getLatestGeneration: () => latestGeneration,
    listOrders: async () => ({ ok: true, json: async () => ['delivered'] }),
    commit: (orders) => published.push(orders),
  });

  oldResponse.resolve({ ok: true, json: async () => ['picked'] });
  await oldRefresh;

  assert.deepEqual(published, [['delivered']]);
});
