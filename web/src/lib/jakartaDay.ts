const JAKARTA_OFFSET_MS = 7 * 60 * 60 * 1000;

const jakartaFormatter = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Jakarta',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

function jakartaParts(date: Date): { year: number; month: number; day: number } {
  const values = Object.fromEntries(
    jakartaFormatter
      .formatToParts(date)
      .filter(({ type }) => type === 'year' || type === 'month' || type === 'day')
      .map(({ type, value }) => [type, Number(value)]),
  );
  return { year: values.year, month: values.month, day: values.day };
}

export function jakartaDateKey(date: Date): string {
  const { year, month, day } = jakartaParts(date);
  return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

export function jakartaDayForWindowEnd(windowEnd: string): string | null {
  const end = Date.parse(windowEnd);
  if (!Number.isFinite(end)) return null;
  return jakartaDateKey(new Date(end - 1));
}

export function millisecondsUntilNextJakartaMidnight(now: Date): number {
  const { year, month, day } = jakartaParts(now);
  const nextMidnight = Date.UTC(year, month - 1, day + 1) - JAKARTA_OFFSET_MS;
  return Math.max(1, nextMidnight - now.getTime());
}

export function jakartaRefreshSnapshot(now: Date): { day: string; delay: number } {
  return {
    day: jakartaDateKey(now),
    delay: millisecondsUntilNextJakartaMidnight(now),
  };
}
