export interface ShipperSnapshotCommit {
  generation: number;
  latestGeneration: number;
  jobsSucceeded: boolean;
  deliveriesSucceeded: boolean;
}

export function shouldCommitShipperSnapshot({
  generation,
  latestGeneration,
  jobsSucceeded,
  deliveriesSucceeded,
}: ShipperSnapshotCommit): boolean {
  return generation === latestGeneration && jobsSucceeded && deliveriesSucceeded;
}

export interface SnapshotResponse<T> {
  ok: boolean;
  json: () => Promise<T>;
}

interface LatestShipperSnapshotOptions<TJobs, TDeliveries> {
  generation: number;
  getLatestGeneration: () => number;
  listJobs: () => Promise<SnapshotResponse<TJobs>>;
  listDeliveries: () => Promise<SnapshotResponse<TDeliveries>>;
  commit: (snapshot: { jobs: TJobs; deliveries: TDeliveries }) => void;
  reportError: (message: string) => void;
}

const refreshErrorMessage = 'Unable to refresh delivery data';

export async function loadLatestShipperSnapshot<TJobs, TDeliveries>({
  generation,
  getLatestGeneration,
  listJobs,
  listDeliveries,
  commit,
  reportError,
}: LatestShipperSnapshotOptions<TJobs, TDeliveries>): Promise<void> {
  let jobsResponse: SnapshotResponse<TJobs>;
  let deliveriesResponse: SnapshotResponse<TDeliveries>;
  try {
    [jobsResponse, deliveriesResponse] = await Promise.all([listJobs(), listDeliveries()]);
  } catch {
    if (generation === getLatestGeneration()) reportError(refreshErrorMessage);
    return;
  }

  if (!shouldCommitShipperSnapshot({
    generation,
    latestGeneration: getLatestGeneration(),
    jobsSucceeded: jobsResponse.ok,
    deliveriesSucceeded: deliveriesResponse.ok,
  })) {
    if (generation === getLatestGeneration()) reportError(refreshErrorMessage);
    return;
  }

  let jobs: TJobs;
  let deliveries: TDeliveries;
  try {
    [jobs, deliveries] = await Promise.all([jobsResponse.json(), deliveriesResponse.json()]);
  } catch {
    if (generation === getLatestGeneration()) reportError(refreshErrorMessage);
    return;
  }

  if (!shouldCommitShipperSnapshot({
    generation,
    latestGeneration: getLatestGeneration(),
    jobsSucceeded: true,
    deliveriesSucceeded: true,
  })) return;

  commit({ jobs, deliveries });
}
