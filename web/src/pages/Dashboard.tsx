import { useCallback, useEffect, useMemo, useState } from 'react';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents, isWindowStat, type DashboardMessage, type EventEnvelope, type MetricName, type WindowStat } from '../context/EventContext';
import { createSession } from '../api/client';

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
  return <div className="event-row"><span>{new Date(event.timestamp).toLocaleTimeString()}</span><strong>{event.event_type}</strong><span>{event.actor_id}</span><span>{JSON.stringify(event.payload)}</span></div>;
}

function MetricChart({ points, rupiah }: { points: WindowStat[]; rupiah?: boolean }) {
  const max = Math.max(1, ...points.map((point) => point.value));
  return <div className="metric-chart" aria-label="Sliding window history">
    {points.length === 0 ? <span className="empty-chart">Waiting for a window…</span> : points.map((point) => <div key={point.window_end} className="metric-bar" title={`${new Date(point.window_end).toLocaleTimeString()} — ${formatValue(point.value, rupiah)}`} style={{ height: `${Math.max(8, point.value / max * 100)}%` }} />)}
  </div>;
}

export default function Dashboard() {
  const { events, addEvent, clearEvents } = useEvents();
  const [dashToken, setDashToken] = useState<string | null>(() => localStorage.getItem('dash_token'));
  const [stats, setStats] = useState<WindowStat[]>([]);
  const onMessage = useCallback((message: DashboardMessage) => {
    if (!isWindowStat(message)) { addEvent(message); return; }
    setStats((previous) => {
      const unique = previous.filter((item) => !(item.metric === message.metric && item.scope === message.scope && item.window_end === message.window_end));
      const next = [...unique, message];
      const windows = next.filter((item) => item.scope === 'window').sort((a, b) => a.window_end.localeCompare(b.window_end));
      const retainedWindows = METRICS.flatMap(({ name }) => windows.filter((item) => item.metric === name).slice(-24));
      const retainedDaily = METRICS.flatMap(({ name }) => next.filter((item) => item.metric === name && item.scope === 'daily').sort((a, b) => b.window_end.localeCompare(a.window_end)).slice(0, 1));
      return [...retainedWindows, ...retainedDaily];
    });
  }, [addEvent]);
  const { connected } = useWebSocket<DashboardMessage>(onMessage, dashToken);

  useEffect(() => {
    if (dashToken) return;
    createSession(`dashboard-${Math.random().toString(36).slice(2, 8)}`, 'dashboard').then((response) => { localStorage.setItem('dash_token', response.token); setDashToken(response.token); }).catch((error) => console.error('[dashboard] failed to create session', error));
  }, [dashToken]);

  const grouped = useMemo(() => Object.fromEntries(METRICS.map(({ name }) => [name, stats.filter((item) => item.metric === name)])) as Record<MetricName, WindowStat[]>, [stats]);

  return <main className="dashboard">
    <header className="dashboard-header"><h1>Stream Processing Dashboard</h1><div><span className={connected ? 'connection connected' : 'connection'}>{connected ? 'Connected' : dashToken ? 'Reconnecting…' : 'Connecting…'}</span><button onClick={() => { clearEvents(); setStats([]); }}>Clear</button></div></header>
    <section><h2>Level 1 — Live Event Feed</h2><p>Raw Kafka events, forwarded without stateful processing.</p><div className="event-feed">{events.length === 0 ? <div className="empty">Waiting for events…</div> : events.map((event) => <EventRow key={event.event_id} event={event} />)}</div></section>
    <section><h2>Level 2 — Stateful Aggregations</h2><p>Five-minute windows slide every five seconds; daily totals reset at UTC midnight.</p><div className="metric-grid">{METRICS.map((metric) => {
      const values = grouped[metric.name];
      const windows = values.filter((item) => item.scope === 'window');
      const latestWindow = windows[windows.length - 1];
      const daily = values.find((item) => item.scope === 'daily');
      const topName = metric.name === 'top_product' ? latestWindow?.detail.name : undefined;
      return <article className="metric-card" key={metric.name}><h3>{metric.label}</h3><div className="metric-values"><div><span>5 min</span><strong>{formatValue(latestWindow?.value, metric.rupiah)}</strong></div><div><span>Today</span><strong>{formatValue(daily?.value, metric.rupiah)}</strong></div></div>{topName && <p className="metric-detail">{topName}</p>}{metric.window ? <MetricChart points={windows} rupiah={metric.rupiah} /> : <div className="metric-chart"><span className="empty-chart">Daily cumulative</span></div>}</article>;
    })}</div></section>
  </main>;
}
