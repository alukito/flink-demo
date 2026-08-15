import { createContext, useCallback, useContext, useEffect, useMemo, useReducer, useRef, useState, type ReactNode } from 'react';
import { createSession } from '../api/client';
import { isEventEnvelope, useEvents, type DashboardMessage, type EventEnvelope, type MetricName, type WindowStat } from '../context/EventContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { retainRecentAlerts, type CepAlert } from '../lib/cepAlerts';
import { requestFreshDashboardToken } from '../lib/dashboardToken';
import { jakartaRefreshSnapshot } from '../lib/jakartaDay';
import { DASHBOARD_METRICS, dashboardReducer, initialDashboardData, type DashboardData } from './dashboardState';

export type DashboardConnectionState = 'connecting' | 'reconnecting' | 'live';

export interface DashboardContextValue extends DashboardData {
  events: EventEnvelope[];
  connectionState: DashboardConnectionState;
  clearAll: () => void;
  groupedStats: Record<MetricName, WindowStat[]>;
  recentAlerts: CepAlert[];
}

const DashboardContext = createContext<DashboardContextValue | undefined>(undefined);

export function DashboardProvider({ children }: { children: ReactNode }) {
  const { events, addEvent, clearEvents } = useEvents();
  const [dashToken, setDashToken] = useState<string | null>(null);
  const tokenRequest = useRef<Promise<string> | null>(null);
  const [data, dispatch] = useReducer(dashboardReducer, undefined, initialDashboardData);
  const onMessage = useCallback((message: DashboardMessage) => {
    if (isEventEnvelope(message)) addEvent(message);
    dispatch({ type: 'message', message });
  }, [addEvent]);
  const { connected } = useWebSocket<DashboardMessage>(onMessage, dashToken);

  useEffect(() => {
    let disposed = false;
    let request = tokenRequest.current;
    if (request === null) {
      const name = `dashboard-${Math.random().toString(36).slice(2, 8)}`;
      request = requestFreshDashboardToken(localStorage, createSession, name);
      tokenRequest.current = request;
    }
    request
      .then((token) => {
        if (!disposed) setDashToken(token);
      })
      .catch((error: unknown) => {
        if (!disposed) console.error('[dashboard] failed to create session', error);
      });
    return () => {
      disposed = true;
    };
  }, []);

  useEffect(() => {
    const timer = window.setInterval(() => dispatch({ type: 'tick', now: new Date() }), 5_000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;
    const refreshAndSchedule = () => {
      const snapshot = jakartaRefreshSnapshot(new Date());
      dispatch({ type: 'jakarta-day', day: snapshot.day });
      timer = setTimeout(refreshAndSchedule, snapshot.delay + 50);
    };
    refreshAndSchedule();
    return () => clearTimeout(timer);
  }, []);

  const clearAll = useCallback(() => {
    clearEvents();
    dispatch({ type: 'clear' });
  }, [clearEvents]);
  const groupedStats = useMemo(() => Object.fromEntries(
    DASHBOARD_METRICS.map((metric) => [metric, data.stats.filter((item) => item.metric === metric)]),
  ) as Record<MetricName, WindowStat[]>, [data.stats]);
  const recentAlerts = useMemo(() => retainRecentAlerts(data.alerts, data.now), [data.alerts, data.now]);
  const connectionState: DashboardConnectionState = connected ? 'live' : dashToken ? 'reconnecting' : 'connecting';
  const value = useMemo<DashboardContextValue>(() => ({
    ...data,
    events,
    connectionState,
    clearAll,
    groupedStats,
    recentAlerts,
  }), [clearAll, connectionState, data, events, groupedStats, recentAlerts]);

  return <DashboardContext.Provider value={value}>{children}</DashboardContext.Provider>;
}

// oxlint-disable-next-line react/only-export-components -- the hook is the provider's public consumer API.
export function useDashboard(): DashboardContextValue {
  const context = useContext(DashboardContext);
  if (!context) throw new Error('useDashboard must be used within DashboardProvider');
  return context;
}
