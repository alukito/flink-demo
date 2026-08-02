export interface CepAlert {
  alert_id: string;
  pattern: string;
  detected_at: string;
  detail: Record<string, unknown>;
}

export interface AlertBucket {
  start: string;
  count: number;
}

export interface TrendingProductCount {
  productId: string;
  productName: string;
  count: number;
}

export interface OrderSurgeStatus {
  detected: boolean;
  count: number;
  detectedAt: string | null;
}

export interface DeliveryDuration {
  alertId: string;
  orderId: string;
  detectedAt: string;
  elapsedSeconds: number;
}

const EIGHT_HOURS_MS = 8 * 60 * 60 * 1000;
const TEN_MINUTES_MS = 10 * 60 * 1000;

function timestamp(value: string): number | null {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isCepAlert(value: unknown): value is CepAlert {
  if (!isRecord(value)) return false;
  return typeof value.alert_id === 'string'
    && typeof value.pattern === 'string'
    && typeof value.detected_at === 'string'
    && timestamp(value.detected_at) !== null
    && isRecord(value.detail);
}

function compareAlerts(left: CepAlert, right: CepAlert): number {
  const byTime = timestamp(left.detected_at)! - timestamp(right.detected_at)!;
  return byTime === 0 ? left.alert_id.localeCompare(right.alert_id) : byTime;
}

export function retainRecentAlerts(alerts: readonly CepAlert[], now = new Date()): CepAlert[] {
  const nowMs = now.getTime();
  const cutoff = nowMs - EIGHT_HOURS_MS;
  const retained = new Map<string, CepAlert>();

  for (const candidate of alerts) {
    if (!isCepAlert(candidate)) continue;
    const detectedAt = timestamp(candidate.detected_at);
    if (detectedAt === null || detectedAt < cutoff || detectedAt > nowMs) continue;
    retained.set(candidate.alert_id, candidate);
  }

  return [...retained.values()].sort(compareAlerts);
}

export function upsertCepAlert(alerts: readonly CepAlert[], alert: CepAlert, now = new Date()): CepAlert[] {
  return retainRecentAlerts([...alerts.filter((entry) => entry.alert_id !== alert.alert_id), alert], now);
}

export function bucketAlertCounts(alerts: readonly CepAlert[], pattern: string, now = new Date()): AlertBucket[] {
  const end = now.getTime();
  const start = end - EIGHT_HOURS_MS;
  const buckets = Array.from({ length: 48 }, (_, index) => ({
    start: new Date(start + index * TEN_MINUTES_MS).toISOString(),
    count: 0,
  }));

  for (const alert of alerts) {
    if (alert.pattern !== pattern) continue;
    const detectedAt = timestamp(alert.detected_at);
    if (detectedAt === null || detectedAt < start || detectedAt > end) continue;
    const bucketIndex = Math.min(47, Math.floor((detectedAt - start) / TEN_MINUTES_MS));
    buckets[bucketIndex].count += 1;
  }
  return buckets;
}

export function trendingProductCounts(alerts: readonly CepAlert[]): TrendingProductCount[] {
  const products = new Map<string, TrendingProductCount>();
  for (const alert of alerts) {
    if (alert.pattern !== 'trending_product') continue;
    const productId = typeof alert.detail.product_id === 'string' ? alert.detail.product_id : null;
    if (!productId) continue;
    const productName = typeof alert.detail.product_name === 'string' ? alert.detail.product_name : productId;
    const current = products.get(productId);
    products.set(productId, current
      ? { ...current, count: current.count + 1 }
      : { productId, productName, count: 1 });
  }
  return [...products.values()].sort((left, right) =>
    right.count - left.count || left.productName.localeCompare(right.productName) || left.productId.localeCompare(right.productId));
}

export function latestOrderSurge(alerts: readonly CepAlert[]): OrderSurgeStatus {
  const surges = alerts.filter((alert) => alert.pattern === 'order_surge').sort(compareAlerts);
  const latest = surges[surges.length - 1];
  return { detected: Boolean(latest), count: surges.length, detectedAt: latest?.detected_at ?? null };
}

export function deliveryDurations(alerts: readonly CepAlert[]): DeliveryDuration[] {
  return alerts.flatMap((alert) => {
    if (alert.pattern !== 'delivery_completed') return [];
    const elapsedSeconds = alert.detail.elapsed_seconds;
    if (typeof elapsedSeconds !== 'number' || !Number.isFinite(elapsedSeconds) || elapsedSeconds < 0) return [];
    return [{
      alertId: alert.alert_id,
      orderId: typeof alert.detail.order_id === 'string' ? alert.detail.order_id : alert.alert_id,
      detectedAt: alert.detected_at,
      elapsedSeconds,
    }];
  }).sort((left, right) => left.detectedAt.localeCompare(right.detectedAt) || left.alertId.localeCompare(right.alertId));
}
