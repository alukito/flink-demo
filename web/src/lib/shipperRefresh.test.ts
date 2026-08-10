import assert from 'node:assert/strict';
import test from 'node:test';
import { loadLatestShipperSnapshot } from './shipperRefresh.ts';

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

test('does not publish an older snapshot after its JSON body resolves behind a newer refresh', async () => {
  let latestGeneration = 1;
  const oldJobsBody = deferred<string[]>();
  const oldDeliveriesBody = deferred<string[]>();
  const oldJobsStarted = deferred<void>();
  const oldDeliveriesStarted = deferred<void>();
  const published: Array<{ jobs: string[]; deliveries: string[] }> = [];

  const oldRefresh = loadLatestShipperSnapshot({
    generation: 1,
    getLatestGeneration: () => latestGeneration,
    listJobs: async () => ({
      ok: true,
      json: () => {
        oldJobsStarted.resolve();
        return oldJobsBody.promise;
      },
    }),
    listDeliveries: async () => ({
      ok: true,
      json: () => {
        oldDeliveriesStarted.resolve();
        return oldDeliveriesBody.promise;
      },
    }),
    commit: (snapshot) => published.push(snapshot),
  });
  await Promise.all([oldJobsStarted.promise, oldDeliveriesStarted.promise]);

  latestGeneration = 2;
  await loadLatestShipperSnapshot({
    generation: 2,
    getLatestGeneration: () => latestGeneration,
    listJobs: async () => ({ ok: true, json: async () => ['new-job'] }),
    listDeliveries: async () => ({ ok: true, json: async () => ['new-delivery'] }),
    commit: (snapshot) => published.push(snapshot),
  });

  oldJobsBody.resolve(['old-job']);
  oldDeliveriesBody.resolve(['old-delivery']);
  await oldRefresh;

  assert.deepEqual(published, [{ jobs: ['new-job'], deliveries: ['new-delivery'] }]);
});

test('does not publish a partial snapshot when either response fails', async () => {
  const published: Array<{ jobs: string[]; deliveries: string[] }> = [];

  await loadLatestShipperSnapshot({
    generation: 1,
    getLatestGeneration: () => 1,
    listJobs: async () => ({ ok: true, json: async () => ['job'] }),
    listDeliveries: async () => ({ ok: false, json: async () => ['delivery'] }),
    commit: (snapshot) => published.push(snapshot),
  });

  assert.deepEqual(published, []);
});
