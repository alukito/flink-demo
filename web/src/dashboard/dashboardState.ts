import type { DashboardMessage, MetricName, WindowStat } from '../context/EventContext.tsx';
import { readCepAlertMessage, retainRecentAlerts, upsertCepAlert, type CepAlert } from '../lib/cepAlerts.ts';
import { jakartaDateKey } from '../lib/jakartaDay.ts';
import { dashboardSessionStart } from '../lib/metricBuckets.ts';

export const DASHBOARD_METRICS: readonly MetricName[] = [
  'listings_count',
  'cart_adds_count',
  'tx_count',
  'confirmed_orders',
  'delivered_orders',
  'top_product',
  'revenue',
];

export interface DashboardData {
  stats: WindowStat[];
  alerts: CepAlert[];
  sessionStart: string;
  jakartaDay: string;
  now: Date;
}

export type DashboardAction =
  | { type: 'message'; message: DashboardMessage }
  | { type: 'tick'; now: Date }
  | { type: 'jakarta-day'; day: string }
  | { type: 'clear' };

export function initialDashboardData(now = new Date()): DashboardData {
  return {
    stats: [],
    alerts: [],
    sessionStart: dashboardSessionStart(now),
    jakartaDay: jakartaDateKey(now),
    now,
  };
}

function isWindowStat(message: DashboardMessage): message is WindowStat {
  return 'metric' in message && 'scope' in message && 'window_end' in message;
}

function retainStats(stats: readonly WindowStat[]): WindowStat[] {
  const windows = stats
    .filter((item) => item.scope === 'window')
    .sort((left, right) => left.window_end.localeCompare(right.window_end));
  const retainedWindows = DASHBOARD_METRICS.flatMap((metric) =>
    windows.filter((item) => item.metric === metric).slice(-24));
  const retainedDaily = DASHBOARD_METRICS.flatMap((metric) =>
    stats
      .filter((item) => item.metric === metric && item.scope === 'daily')
      .sort((left, right) => right.window_end.localeCompare(left.window_end))
      .slice(0, 1));

  return [...retainedWindows, ...retainedDaily];
}

function reduceMessage(state: DashboardData, message: DashboardMessage): DashboardData {
  const cepAlert = readCepAlertMessage(message);
  if (cepAlert !== undefined) {
    return cepAlert === null
      ? state
      : { ...state, alerts: upsertCepAlert(state.alerts, cepAlert, state.now) };
  }
  if (!isWindowStat(message)) return state;

  const unique = state.stats.filter((item) =>
    !(item.metric === message.metric && item.scope === message.scope && item.window_end === message.window_end));
  return { ...state, stats: retainStats([...unique, message]) };
}

export function dashboardReducer(state: DashboardData, action: DashboardAction): DashboardData {
  switch (action.type) {
    case 'message':
      return reduceMessage(state, action.message);
    case 'tick':
      return { ...state, now: action.now, alerts: retainRecentAlerts(state.alerts, action.now) };
    case 'jakarta-day':
      return { ...state, jakartaDay: action.day };
    case 'clear':
      return { ...state, stats: [], alerts: [] };
  }
}
