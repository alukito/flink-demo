import { DashboardMetricCard, type DashboardMetricDefinition } from '../../dashboard/DashboardMetricCard';
import { useDashboard } from '../../dashboard/DashboardContext';

export const METRICS = [
  { name: 'listings_count', label: 'Listings', window: true, daily: false },
  { name: 'cart_adds_count', label: 'Cart adds', window: true, daily: false },
  { name: 'tx_count', label: 'Checkouts', window: true, daily: true },
  { name: 'confirmed_orders', label: 'Confirmed', window: true, daily: false },
  { name: 'delivered_orders', label: 'Delivered', window: true, daily: true },
  { name: 'top_product', label: 'Top product', window: true, daily: false },
  { name: 'revenue', label: 'Today’s revenue', window: false, daily: true, rupiah: true },
] as const satisfies readonly DashboardMetricDefinition[];

export function DashboardWindowsPage() {
  const { groupedStats, jakartaDay, now, sessionStart } = useDashboard();

  return (
    <section className="dashboard-page dashboard-page--windows" aria-labelledby="window-heading">
      <header className="dashboard-page__heading">
        <div>
          <h2 id="window-heading">Five-minute windows</h2>
          <p>Aligned windows update live; daily totals reset at Jakarta midnight.</p>
        </div>
        <span className="dashboard-page__cadence">24 slots · WIB</span>
      </header>
      <div className="dashboard-metric-grid">
        {METRICS.map((metric) => (
          <DashboardMetricCard
            key={metric.name}
            metric={metric}
            stats={groupedStats[metric.name]}
            sessionStart={sessionStart}
            jakartaDay={jakartaDay}
            now={now}
          />
        ))}
      </div>
    </section>
  );
}
