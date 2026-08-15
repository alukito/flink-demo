import type { DeliveryDuration } from '../lib/cepAlerts';

interface DeliveryDurationChartProps {
  points: readonly DeliveryDuration[];
}

export function DeliveryDurationChart({ points }: DeliveryDurationChartProps) {
  const maximum = Math.max(1, ...points.map((point) => point.elapsedSeconds));

  return (
    <div className="delivery-duration-chart" role="list" aria-label="Checkout to delivery elapsed seconds per completed order">
      {points.length === 0 ? (
        <span className="dashboard-empty-chart" role="listitem">Waiting for a completed delivery…</span>
      ) : points.map((point) => (
        <div className="delivery-duration-chart__item" key={point.alertId} role="listitem">
          <span
            className="delivery-duration-chart__bar"
            style={{ height: `${Math.max(10, point.elapsedSeconds / maximum * 100)}%` }}
            title={`${point.orderId} — ${point.elapsedSeconds.toLocaleString('id-ID')} seconds`}
          />
          <strong>{point.elapsedSeconds.toLocaleString('id-ID')}s</strong>
          <small title={point.orderId}>{point.orderId}</small>
        </div>
      ))}
    </div>
  );
}
