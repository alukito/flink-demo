import type { AlertBucket } from '../lib/cepAlerts';

const WIB_TIME = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Jakarta',
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23',
});

interface AlertCountChartProps {
  points: readonly AlertBucket[];
  label: string;
}

export function AlertCountChart({ points, label }: AlertCountChartProps) {
  const maximum = Math.max(1, ...points.map((point) => point.count));

  return (
    <div className="alert-count-chart" role="list" aria-label={`${label} ten-minute count history`}>
      {points.map((point) => {
        const description = `${WIB_TIME.format(new Date(point.start))} WIB — ${point.count.toLocaleString('id-ID')} alerts`;
        return (
          <span
            aria-label={description}
            className="alert-count-chart__bar"
            key={point.start}
            role="listitem"
            style={{ height: point.count === 0 ? '0%' : `${Math.max(4, point.count / maximum * 100)}%` }}
            title={description}
          />
        );
      })}
    </div>
  );
}
