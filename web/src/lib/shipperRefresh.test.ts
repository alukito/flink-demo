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
  const errors: string[] = [];

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
    reportError: (message) => errors.push(message),
  });
  await Promise.all([oldJobsStarted.promise, oldDeliveriesStarted.promise]);

  latestGeneration = 2;
  await loadLatestShipperSnapshot({
    generation: 2,
    getLatestGeneration: () => latestGeneration,
    listJobs: async () => ({ ok: true, json: async () => ['new-job'] }),
    listDeliveries: async () => ({ ok: true, json: async () => ['new-delivery'] }),
    commit: (snapshot) => published.push(snapshot),
    reportError: (message) => errors.push(message),
  });

  oldJobsBody.resolve(['old-job']);
  oldDeliveriesBody.resolve(['old-delivery']);
  await oldRefresh;

  assert.deepEqual(published, [{ jobs: ['new-job'], deliveries: ['new-delivery'] }]);
  assert.deepEqual(errors, []);
});

test('does not publish a partial snapshot when either response fails', async () => {
  const published: Array<{ jobs: string[]; deliveries: string[] }> = [];
  const errors: string[] = [];

  await loadLatestShipperSnapshot({
    generation: 1,
    getLatestGeneration: () => 1,
    listJobs: async () => ({ ok: true, json: async () => ['job'] }),
    listDeliveries: async () => ({ ok: false, json: async () => ['delivery'] }),
    commit: (snapshot) => published.push(snapshot),
    reportError: (message) => errors.push(message),
  });

  assert.deepEqual(published, []);
  assert.deepEqual(errors, ['Unable to refresh delivery data']);
});

test('reports a current transport rejection without replacing the rendered snapshot', async () => {
  const published: Array<{ jobs: string[]; deliveries: string[] }> = [];
  const errors: string[] = [];

  await loadLatestShipperSnapshot<string[], string[]>({
    generation: 3,
    getLatestGeneration: () => 3,
    listJobs: async () => { throw new Error('network unavailable'); },
    listDeliveries: async () => ({ ok: true, json: async () => ['delivery'] }),
    commit: (snapshot) => published.push(snapshot),
    reportError: (message) => errors.push(message),
  });

  assert.deepEqual(published, []);
  assert.deepEqual(errors, ['Unable to refresh delivery data']);
});

test('reports invalid JSON from a current refresh without replacing the rendered snapshot', async () => {
  const published: Array<{ jobs: string[]; deliveries: string[] }> = [];
  const errors: string[] = [];

  await loadLatestShipperSnapshot({
    generation: 4,
    getLatestGeneration: () => 4,
    listJobs: async () => ({ ok: true, json: async () => ['job'] }),
    listDeliveries: async () => ({
      ok: true,
      json: async () => { throw new SyntaxError('invalid JSON'); },
    }),
    commit: (snapshot) => published.push(snapshot),
    reportError: (message) => errors.push(message),
  });

  assert.deepEqual(published, []);
  assert.deepEqual(errors, ['Unable to refresh delivery data']);
});

test('does not surface a stale refresh failure after a newer snapshot succeeds', async () => {
  let latestGeneration = 1;
  const oldJobs = deferred<{ ok: boolean; json: () => Promise<string[]> }>();
  const published: Array<{ jobs: string[]; deliveries: string[] }> = [];
  const errors: string[] = [];

  const oldRefresh = loadLatestShipperSnapshot({
    generation: 1,
    getLatestGeneration: () => latestGeneration,
    listJobs: () => oldJobs.promise,
    listDeliveries: async () => ({ ok: true, json: async () => ['old-delivery'] }),
    commit: (snapshot) => published.push(snapshot),
    reportError: (message) => errors.push(message),
  });

  latestGeneration = 2;
  await loadLatestShipperSnapshot({
    generation: 2,
    getLatestGeneration: () => latestGeneration,
    listJobs: async () => ({ ok: true, json: async () => ['new-job'] }),
    listDeliveries: async () => ({ ok: true, json: async () => ['new-delivery'] }),
    commit: (snapshot) => published.push(snapshot),
    reportError: (message) => errors.push(message),
  });

  oldJobs.resolve({ ok: false, json: async () => ['old-job'] });
  await oldRefresh;

  assert.deepEqual(published, [{ jobs: ['new-job'], deliveries: ['new-delivery'] }]);
  assert.deepEqual(errors, []);
});
