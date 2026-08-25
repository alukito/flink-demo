import { useMemo } from 'react';
import { useDashboard } from '../../dashboard/DashboardContext';
import {
  deliveryDurations,
  latestOrderSurge,
  trendingProductCounts,
} from '../../lib/cepAlerts';
import type { CepAlert } from '../../lib/cepAlerts';

const WIB_DATE_TIME = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Jakarta',
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23',
});

function detailText(alert: CepAlert, name: string, fallback = '—'): string {
  const value = alert.detail[name];
  return typeof value === 'string' && value.length > 0 ? value : fallback;
}

function participant(alert: CepAlert, role: string): string {
  return detailText(alert, `${role}_name`, detailText(alert, `${role}_id`));
}

function eventTime(value: string): string {
  return `${WIB_DATE_TIME.format(new Date(value))} WIB`;
}

function elapsedTime(seconds: number): string {
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return minutes > 0 ? `${minutes}m ${String(remainder).padStart(2, '0')}s` : `${remainder}s`;
}

export function DashboardPatternsPage() {
  const { recentAlerts } = useDashboard();
  const abandonedCarts = useMemo(() => recentAlerts.filter((alert) => alert.pattern === 'abandoned_cart'), [recentAlerts]);
  const slowDelivery = useMemo(() => recentAlerts.filter((alert) => alert.pattern === 'slow_delivery'), [recentAlerts]);
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
          <header><h3 id="abandoned-carts-heading">Abandoned carts</h3><span>Special events</span></header>
          <div className="pattern-event-table">
            <table aria-label="Abandoned carts">
              <thead><tr><th scope="col">Detected</th><th scope="col">Buyer</th><th scope="col">Seller</th><th scope="col">Cart</th></tr></thead>
              <tbody>{abandonedCarts.length === 0 ? (
                <tr><td colSpan={4}>Waiting for an abandoned cart…</td></tr>
              ) : abandonedCarts.map((alert) => (
                <tr key={alert.alert_id}><td>{eventTime(alert.detected_at)}</td><td>{participant(alert, 'buyer')}</td><td>{participant(alert, 'seller')}</td><td title={detailText(alert, 'cart_id')}>{detailText(alert, 'cart_id')}</td></tr>
              ))}</tbody>
            </table>
          </div>
        </article>

        <article className="dashboard-pattern-card" aria-labelledby="slow-delivery-heading">
          <header><h3 id="slow-delivery-heading">Slow delivery</h3><span>Special events</span></header>
          <div className="pattern-event-table">
            <table aria-label="Slow delivery">
              <thead><tr><th scope="col">Detected</th><th scope="col">Buyer</th><th scope="col">Seller</th><th scope="col">Shipper</th><th scope="col">Order</th></tr></thead>
              <tbody>{slowDelivery.length === 0 ? (
                <tr><td colSpan={5}>Waiting for a slow delivery…</td></tr>
              ) : slowDelivery.map((alert) => (
                <tr key={alert.alert_id}><td>{eventTime(alert.detected_at)}</td><td>{participant(alert, 'buyer')}</td><td>{participant(alert, 'seller')}</td><td>{participant(alert, 'shipper')}</td><td title={detailText(alert, 'order_id')}>{detailText(alert, 'order_id')}</td></tr>
              ))}</tbody>
            </table>
          </div>
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
          <div className="pattern-event-table">
            <table aria-label="Checkout to delivery">
              <thead><tr><th scope="col">Completed</th><th scope="col">Shipper</th><th scope="col">Elapsed</th><th scope="col">Order</th></tr></thead>
              <tbody>{durations.length === 0 ? (
                <tr><td colSpan={4}>Waiting for a completed delivery…</td></tr>
              ) : durations.map((duration) => (
                <tr key={duration.alertId}><td>{eventTime(duration.detectedAt)}</td><td title={duration.shipperId}>{duration.shipperName}</td><td>{elapsedTime(duration.elapsedSeconds)}</td><td title={duration.orderId}>{duration.orderId}</td></tr>
              ))}</tbody>
            </table>
          </div>
        </article>
      </div>
    </section>
  );
}
