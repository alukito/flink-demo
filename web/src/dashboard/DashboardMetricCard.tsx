import type { MetricName, WindowStat } from '../context/EventContext';
import { jakartaDayForWindowEnd } from '../lib/jakartaDay';
import { formatJakartaBucketRange, metricBuckets } from '../lib/metricBuckets';
import { MetricBarChart } from '../components/MetricBarChart';

export interface DashboardMetricDefinition {
  name: MetricName;
  label: string;
  window: boolean;
  daily: boolean;
  rupiah?: boolean;
}

interface DashboardMetricCardProps {
  metric: DashboardMetricDefinition;
  stats: readonly WindowStat[];
  sessionStart: string;
  jakartaDay: string;
  now: Date;
}

function formatValue(value: number | undefined, rupiah = false): string {
  if (value === undefined) return '—';
  return rupiah
    ? new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', maximumFractionDigits: 0 }).format(value)
    : value.toLocaleString('id-ID');
}

export function DashboardMetricCard({ metric, stats, sessionStart, jakartaDay, now }: DashboardMetricCardProps) {
  const windows = stats.filter((item) => item.scope === 'window');
  const buckets = metric.window ? metricBuckets(windows, sessionStart, now) : [];
  const daily = metric.daily
    ? stats.find((item) => item.scope === 'daily' && jakartaDayForWindowEnd(item.window_end) === jakartaDay)
    : undefined;
  const topProductRows = metric.name === 'top_product'
    ? buckets
      .filter((bucket) => typeof bucket.detail.name === 'string' && bucket.detail.name.length > 0)
      .slice(-5)
      .reverse()
    : [];
  const headingId = `metric-${metric.name}`;

  return (
    <article
      className={`dashboard-metric-card${metric.name === 'revenue' ? ' dashboard-metric-card--revenue' : ''}`}
      aria-labelledby={headingId}
    >
      <header className="dashboard-metric-card__header">
        <h3 id={headingId}>{metric.label}</h3>
        <span>{metric.window ? '5 min aligned' : 'WIB day'}</span>
      </header>

      <div className="dashboard-metric-card__values">
        {metric.daily && (
          <div>
            <span>Today</span>
            <strong>{formatValue(daily?.value, metric.rupiah)}</strong>
          </div>
        )}
      </div>

      {metric.name === 'top_product' ? (
        <div className="top-product-windows">
          <table aria-label="Top product by five-minute window">
            <thead><tr><th>Window</th><th>Top product</th><th>Adds</th></tr></thead>
            <tbody>
              {topProductRows.length === 0 ? (
                <tr><td colSpan={3}>Waiting for product activity…</td></tr>
              ) : topProductRows.map((bucket) => (
                <tr key={bucket.windowEnd}>
                  <td>{formatJakartaBucketRange(bucket.windowEnd) ?? bucket.windowEnd}</td>
                  <td>{bucket.detail.name}</td>
                  <td>{formatValue(bucket.value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : metric.window ? (
        <MetricBarChart
          buckets={buckets}
          title={`${metric.label} five-minute aligned-window history`}
          formatValue={(value) => formatValue(value, metric.rupiah)}
        />
      ) : null}
    </article>
  );
}
