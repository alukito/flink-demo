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
  shipperId: string;
  shipperName: string;
}

const EIGHT_HOURS_MS = 8 * 60 * 60 * 1000;
const TEN_MINUTES_MS = 10 * 60 * 1000;

function timestamp(value: string): number | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match) return null;
  const [, year, month, day, hour, minute, second, fraction = '', offset] = match;
  const calendar = new Date(Date.UTC(
    Number(year),
    Number(month) - 1,
    Number(day),
    Number(hour),
    Number(minute),
    Number(second),
    Number(`${fraction.padEnd(3, '0') || '0'}`),
  ));
  if (calendar.getUTCFullYear() !== Number(year)
    || calendar.getUTCMonth() !== Number(month) - 1
    || calendar.getUTCDate() !== Number(day)
    || calendar.getUTCHours() !== Number(hour)
    || calendar.getUTCMinutes() !== Number(minute)
    || calendar.getUTCSeconds() !== Number(second)
    || (offset !== 'Z' && (Number(offset.slice(1, 3)) > 23 || Number(offset.slice(4, 6)) > 59))) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isCepAlertEnvelope(value: unknown): value is Record<string, unknown> {
  return isRecord(value)
    && typeof value.alert_id === 'string'
    && typeof value.pattern === 'string'
    && isRecord(value.detail);
}

export function isCepAlert(value: unknown): value is CepAlert {
  if (!isCepAlertEnvelope(value)) return false;
  return typeof value.detected_at === 'string'
    && timestamp(value.detected_at) !== null
    && isRecord(value.detail);
}

/**
 * Separates a non-CEP dashboard message (`undefined`) from a CEP envelope.
 * A malformed CEP envelope is represented by `null` so callers can discard it
 * without accidentally treating it as a Level 1 event.
 */
export function readCepAlertMessage(value: unknown): CepAlert | null | undefined {
  if (!isCepAlertEnvelope(value)) return undefined;
  return isCepAlert(value) ? value : null;
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
      shipperId: typeof alert.detail.shipper_id === 'string' ? alert.detail.shipper_id : '—',
      shipperName: typeof alert.detail.shipper_name === 'string'
        ? alert.detail.shipper_name
        : typeof alert.detail.shipper_id === 'string' ? alert.detail.shipper_id : '—',
    }];
  }).sort((left, right) => left.detectedAt.localeCompare(right.detectedAt) || left.alertId.localeCompare(right.alertId));
}
