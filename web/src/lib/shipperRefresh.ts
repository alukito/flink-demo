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
}

export async function loadLatestShipperSnapshot<TJobs, TDeliveries>({
  generation,
  getLatestGeneration,
  listJobs,
  listDeliveries,
  commit,
}: LatestShipperSnapshotOptions<TJobs, TDeliveries>): Promise<void> {
  const [jobsResponse, deliveriesResponse] = await Promise.all([listJobs(), listDeliveries()]);
  if (!shouldCommitShipperSnapshot({
    generation,
    latestGeneration: getLatestGeneration(),
    jobsSucceeded: jobsResponse.ok,
    deliveriesSucceeded: deliveriesResponse.ok,
  })) return;

  const [jobs, deliveries] = await Promise.all([jobsResponse.json(), deliveriesResponse.json()]);
  if (!shouldCommitShipperSnapshot({
    generation,
    latestGeneration: getLatestGeneration(),
    jobsSucceeded: true,
    deliveriesSucceeded: true,
  })) return;

  commit({ jobs, deliveries });
}
