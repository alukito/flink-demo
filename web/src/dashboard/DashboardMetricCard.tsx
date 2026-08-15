import type { MetricName, WindowStat } from '../context/EventContext';
import { jakartaDayForWindowEnd } from '../lib/jakartaDay';
import { metricBuckets } from '../lib/metricBuckets';
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
  const activeBucket = buckets[buckets.length - 1];
  const daily = metric.daily
    ? stats.find((item) => item.scope === 'daily' && jakartaDayForWindowEnd(item.window_end) === jakartaDay)
    : undefined;
  const topProduct = metric.name === 'top_product' && typeof activeBucket?.detail.name === 'string'
    ? activeBucket.detail.name
    : null;
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
        {metric.window && (
          <div>
            <span>Current 5 min</span>
            <strong>{formatValue(activeBucket?.value, metric.rupiah)}</strong>
          </div>
        )}
        {metric.daily && (
          <div>
            <span>Today</span>
            <strong>{formatValue(daily?.value, metric.rupiah)}</strong>
          </div>
        )}
      </div>

      {topProduct && <p className="dashboard-metric-card__detail">{topProduct}</p>}
      {metric.window ? (
        <MetricBarChart
          buckets={buckets}
          title={`${metric.label} five-minute aligned-window history`}
          formatValue={(value) => formatValue(value, metric.rupiah)}
        />
      ) : (
        <div className="dashboard-metric-card__daily" aria-hidden="true">
          <span>Daily cumulative</span>
        </div>
      )}
    </article>
  );
}
