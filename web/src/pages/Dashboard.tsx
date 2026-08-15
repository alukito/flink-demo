import { useMemo } from 'react';
import type { EventEnvelope, MetricName } from '../context/EventContext';
import { MetricBarChart } from '../components/MetricBarChart';
import { jakartaDayForWindowEnd } from '../lib/jakartaDay';
import {
  bucketAlertCounts,
  deliveryDurations,
  latestOrderSurge,
  trendingProductCounts,
  type AlertBucket,
  type DeliveryDuration,
} from '../lib/cepAlerts';
import { metricBuckets } from '../lib/metricBuckets';
import { DashboardProvider, useDashboard } from '../dashboard/DashboardContext';

const METRICS: Array<{ name: MetricName; label: string; window: boolean; daily: boolean; rupiah?: boolean }> = [
  { name: 'listings_count', label: 'Listings', window: true, daily: false },
  { name: 'cart_adds_count', label: 'Cart adds', window: true, daily: false },
  { name: 'tx_count', label: 'Checkouts', window: true, daily: true },
  { name: 'confirmed_orders', label: 'Confirmed', window: true, daily: false },
  { name: 'delivered_orders', label: 'Delivered', window: true, daily: true },
  { name: 'top_product', label: 'Top product', window: true, daily: false },
  { name: 'revenue', label: 'Revenue', window: false, daily: true, rupiah: true },
];

function formatValue(value: number | undefined, rupiah = false): string {
  if (value === undefined) return '—';
  return rupiah ? new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', maximumFractionDigits: 0 }).format(value) : value.toLocaleString('id-ID');
}

function EventRow({ event }: { event: EventEnvelope }) {
  return <div className="event-row"><span>{new Date(event.timestamp).toLocaleTimeString()}</span><strong>{event.event_type}</strong><span>{event.actor_name ?? event.actor_id}</span><span>{JSON.stringify(event.payload)}</span></div>;
}

function AlertCountChart({ points, label }: { points: AlertBucket[]; label: string }) {
  const max = Math.max(1, ...points.map((point) => point.count));
  return <div className="cep-chart" aria-label={`${label} history`}>
    {points.map((point) => <div key={point.start} className="cep-bar" title={`${new Date(point.start).toLocaleString()} — ${point.count.toLocaleString('id-ID')} alerts`} style={{ height: `${Math.max(3, point.count / max * 1e2)}%` }} />)}
  </div>;
}

function DurationChart({ points }: { points: DeliveryDuration[] }) {
  const max = Math.max(1, ...points.map((point) => point.elapsedSeconds));
  return <div className="cep-chart" aria-label="Checkout to delivery elapsed time">
    {points.length === 0 ? <span className="empty-chart">Waiting for a completed delivery…</span> : points.map((point) => <div key={point.alertId} className="cep-duration-bar" title={`${new Date(point.detectedAt).toLocaleString()} — ${point.elapsedSeconds.toLocaleString('id-ID')} seconds`} style={{ height: `${Math.max(8, point.elapsedSeconds / max * 1e2)}%` }} />)}
  </div>;
}

function DashboardContent() {
  const { clearAll, connectionState, events, groupedStats: grouped, jakartaDay, now, recentAlerts, sessionStart } = useDashboard();
  const abandonedCartBuckets = useMemo(() => bucketAlertCounts(recentAlerts, 'abandoned_cart'), [recentAlerts]);
  const deliveryDelayBuckets = useMemo(() => bucketAlertCounts(recentAlerts, 'slow_delivery'), [recentAlerts]);
  const trendingProducts = useMemo(() => trendingProductCounts(recentAlerts), [recentAlerts]);
  const surge = useMemo(() => latestOrderSurge(recentAlerts), [recentAlerts]);
  const durations = useMemo(() => deliveryDurations(recentAlerts), [recentAlerts]);

  return <main className="dashboard">
    <header className="dashboard-header"><h1>Stream Processing Dashboard</h1><div><span className={connectionState === 'live' ? 'connection connected' : 'connection'}>{connectionState === 'live' ? 'Connected' : connectionState === 'reconnecting' ? 'Reconnecting…' : 'Connecting…'}</span><button onClick={clearAll}>Clear</button></div></header>
    <section><h2>Level 1 — Live Event Feed</h2><p>Raw Kafka events, forwarded without stateful processing.</p><div className="event-feed">{events.length === 0 ? <div className="empty">Waiting for events…</div> : events.map((event) => <EventRow key={event.event_id} event={event} />)}</div></section>
    <section><h2>Level 2 — Stateful Aggregations</h2><p>Five-minute aligned windows update every five seconds; daily totals reset at Jakarta midnight (WIB).</p><div className="metric-grid">{METRICS.map((metric) => {
      const values = grouped[metric.name];
      const windows = values.filter((item) => item.scope === 'window');
      const buckets = metric.window ? metricBuckets(windows, sessionStart, now) : [];
      const activeBucket = buckets[buckets.length - 1];
      const daily = values.find(
        (item) =>
          item.scope === 'daily' &&
          jakartaDayForWindowEnd(item.window_end) === jakartaDay,
      );
      const topName = metric.name === 'top_product' ? activeBucket?.detail.name : undefined;
      return <article className="metric-card" key={metric.name}><h3>{metric.label}</h3><div className="metric-values"><div><span>Current 5 min</span><strong>{metric.window ? formatValue(activeBucket?.value, metric.rupiah) : '—'}</strong></div><div><span>Today</span><strong>{formatValue(daily?.value, metric.rupiah)}</strong></div></div>{topName && <p className="metric-detail">{topName}</p>}{metric.window ? <MetricBarChart buckets={buckets} title={`${metric.label} five-minute aligned-window history`} formatValue={(value) => formatValue(value, metric.rupiah)} /> : <div className="daily-placeholder"><span className="empty-chart">Daily cumulative</span></div>}</article>;
    })}</div></section>
    <section><h2>Level 3 — CEP Alert History</h2><p>Immutable alert facts retained in this dashboard for the last eight hours.</p><div className="cep-grid">
      <article className="metric-card"><h3>Abandoned carts</h3><p className="cep-card-note">Count by ten-minute bucket</p><AlertCountChart points={abandonedCartBuckets} label="Abandoned carts" /></article>
      <article className="metric-card"><h3>Delivery delays</h3><p className="cep-card-note">Count by ten-minute bucket</p><AlertCountChart points={deliveryDelayBuckets} label="Delivery delays" /></article>
      <article className="metric-card"><h3>Trending products</h3><p className="cep-card-note">Qualified product alerts</p><div className="trending-products">{trendingProducts.length === 0 ? <span className="empty-chart">Waiting for product trends…</span> : <table><thead><tr><th>Product</th><th>Count</th></tr></thead><tbody>{trendingProducts.map((product) => <tr key={product.productId}><td title={product.productId}>{product.productName}</td><td>{product.count.toLocaleString('id-ID')}</td></tr>)}</tbody></table>}</div></article>
      <article className="metric-card"><h3>Order surge</h3><p className="cep-card-note">Checkout activity in the latest alert window</p><div className={surge.detected ? 'surge-status detected' : 'surge-status'}><strong>{surge.detected ? 'Detected' : 'Not detected'}</strong><span>{surge.count.toLocaleString('id-ID')} alerts</span>{surge.detectedAt && <small>Latest: {new Date(surge.detectedAt).toLocaleString()}</small>}</div></article>
      <article className="metric-card cep-duration-card"><h3>Checkout to delivery</h3><p className="cep-card-note">Elapsed seconds per completed order</p><DurationChart points={durations} /></article>
    </div></section>
  </main>;
}

export default function Dashboard() {
  return <DashboardProvider><DashboardContent /></DashboardProvider>;
}
