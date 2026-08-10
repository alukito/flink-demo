export interface SellerOrdersResponse<T> {
  ok: boolean;
  json: () => Promise<T>;
}

interface LatestSellerOrdersOptions<T> {
  generation: number;
  getLatestGeneration: () => number;
  listOrders: () => Promise<SellerOrdersResponse<T>>;
  commit: (orders: T) => void;
}

export async function loadLatestSellerOrders<T>({
  generation,
  getLatestGeneration,
  listOrders,
  commit,
}: LatestSellerOrdersOptions<T>): Promise<void> {
  const response = await listOrders();
  if (!response.ok || generation !== getLatestGeneration()) return;

  const orders = await response.json();
  if (generation !== getLatestGeneration()) return;

  commit(orders);
}
