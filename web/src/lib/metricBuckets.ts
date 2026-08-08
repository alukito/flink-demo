export const FIVE_MINUTES_MS = 5 * 60 * 1000;
export const METRIC_BUCKET_COUNT = 24;

export interface MetricSnapshot {
  window_end: string;
  value: number;
  detail: Record<string, string>;
}

export interface MetricBucket {
  start: string;
  windowEnd: string;
  value: number;
  detail: Record<string, string>;
}

const JAKARTA_TIME_FORMATTER = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Jakarta',
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23',
});

function timestamp(value: string): number | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?Z$/.exec(value);
  if (!match) return null;

  const [, year, month, day, hour, minute, second, fraction = ''] = match;
  const calendar = new Date(Date.UTC(
    Number(year),
    Number(month) - 1,
    Number(day),
    Number(hour),
    Number(minute),
    Number(second),
    Number(fraction.padEnd(3, '0') || '0'),
  ));
  if (calendar.getUTCFullYear() !== Number(year)
    || calendar.getUTCMonth() !== Number(month) - 1
    || calendar.getUTCDate() !== Number(day)
    || calendar.getUTCHours() !== Number(hour)
    || calendar.getUTCMinutes() !== Number(minute)
    || calendar.getUTCSeconds() !== Number(second)) return null;
  return calendar.getTime();
}

function windowEndIso(timestampMs: number): string {
  return new Date(timestampMs).toISOString();
}

export function activeWindowEnd(now = new Date()): string {
  return windowEndIso((Math.floor(now.getTime() / FIVE_MINUTES_MS) + 1) * FIVE_MINUTES_MS);
}

export function metricBuckets(stats: readonly MetricSnapshot[], now = new Date()): MetricBucket[] {
  const snapshotsByWindowEnd = new Map<number, MetricSnapshot>();
  for (const snapshot of stats) {
    const windowEnd = timestamp(snapshot.window_end);
    if (windowEnd !== null) snapshotsByWindowEnd.set(windowEnd, snapshot);
  }

  const activeEnd = timestamp(activeWindowEnd(now))!;
  return Array.from({ length: METRIC_BUCKET_COUNT }, (_, index) => {
    const windowEnd = activeEnd - (METRIC_BUCKET_COUNT - 1 - index) * FIVE_MINUTES_MS;
    const snapshot = snapshotsByWindowEnd.get(windowEnd);
    return {
      start: windowEndIso(windowEnd - FIVE_MINUTES_MS),
      windowEnd: windowEndIso(windowEnd),
      value: snapshot?.value ?? 0,
      detail: snapshot?.detail ?? {},
    };
  });
}

export function formatJakartaBucketStart(windowEnd: string): string | null {
  const end = timestamp(windowEnd);
  return end === null ? null : JAKARTA_TIME_FORMATTER.format(new Date(end - FIVE_MINUTES_MS));
}

export function formatJakartaBucketRange(windowEnd: string): string | null {
  const start = formatJakartaBucketStart(windowEnd);
  const end = timestamp(windowEnd);
  return start === null || end === null ? null : `${start}–${JAKARTA_TIME_FORMATTER.format(new Date(end))} WIB`;
}
