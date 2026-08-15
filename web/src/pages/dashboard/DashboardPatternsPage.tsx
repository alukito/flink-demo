import { useMemo } from 'react';
import { AlertCountChart } from '../../dashboard/AlertCountChart';
import { DeliveryDurationChart } from '../../dashboard/DeliveryDurationChart';
import { useDashboard } from '../../dashboard/DashboardContext';
import {
  bucketAlertCounts,
  deliveryDurations,
  latestOrderSurge,
  trendingProductCounts,
} from '../../lib/cepAlerts';

export function DashboardPatternsPage() {
  const { now, recentAlerts } = useDashboard();
  const abandonedCarts = useMemo(() => bucketAlertCounts(recentAlerts, 'abandoned_cart', now), [now, recentAlerts]);
  const slowDelivery = useMemo(() => bucketAlertCounts(recentAlerts, 'slow_delivery', now), [now, recentAlerts]);
  const trendingProducts = useMemo(() => trendingProductCounts(recentAlerts), [recentAlerts]);
  const visibleTrendingProducts = trendingProducts.slice(0, 5);
  const surge = useMemo(() => latestOrderSurge(recentAlerts), [recentAlerts]);
  const durations = useMemo(() => deliveryDurations(recentAlerts), [recentAlerts]);

  return (
    <section className="dashboard-page dashboard-page--patterns" aria-labelledby="patterns-heading">
      <header className="dashboard-page__heading">
        <div>
          <h2 id="patterns-heading">CEP pattern signals</h2>
          <p>Immutable alert facts retained for the last eight hours.</p>
        </div>
        <span className="dashboard-page__cadence">10 min buckets</span>
      </header>
      <div className="dashboard-pattern-grid">
        <article className="dashboard-pattern-card" aria-labelledby="abandoned-carts-heading">
          <header><h3 id="abandoned-carts-heading">Abandoned carts</h3><span>48 buckets</span></header>
          <p>Ten-minute count history</p>
          <AlertCountChart points={abandonedCarts} label="Abandoned carts" />
        </article>

        <article className="dashboard-pattern-card" aria-labelledby="slow-delivery-heading">
          <header><h3 id="slow-delivery-heading">Slow delivery</h3><span>48 buckets</span></header>
          <p>Ten-minute count history</p>
          <AlertCountChart points={slowDelivery} label="Slow delivery" />
        </article>

        <article className="dashboard-pattern-card dashboard-pattern-card--trending" aria-labelledby="trending-products-heading">
          <header><h3 id="trending-products-heading">Trending products</h3><span>Top 5 by count</span></header>
          <div className="trending-products">
            <table aria-label="Trending products">
              <thead><tr><th scope="col">Product</th><th scope="col">Count</th></tr></thead>
              <tbody>
                {visibleTrendingProducts.length === 0 ? (
                  <tr><td colSpan={2}>Waiting for product trends…</td></tr>
                ) : visibleTrendingProducts.map((product) => (
                  <tr key={product.productId}>
                    <td title={product.productId}>{product.productName}</td>
                    <td>{product.count.toLocaleString('id-ID')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>

        <article className="dashboard-pattern-card dashboard-pattern-card--surge" aria-labelledby="order-surge-heading">
          <header><h3 id="order-surge-heading">Order surge</h3><span>Binary signal</span></header>
          <div className="order-surge" data-detected={surge.detected}>
            <span aria-hidden="true" />
            <strong>{surge.detected ? 'Detected' : 'Not detected'}</strong>
            <small>{surge.count.toLocaleString('id-ID')} alerts retained</small>
          </div>
        </article>

        <article className="dashboard-pattern-card dashboard-pattern-card--duration" aria-labelledby="checkout-delivery-heading">
          <header><h3 id="checkout-delivery-heading">Checkout to delivery</h3><span>Completed orders</span></header>
          <p>Elapsed seconds per completed order</p>
          <DeliveryDurationChart points={durations} />
        </article>
      </div>
    </section>
  );
}
