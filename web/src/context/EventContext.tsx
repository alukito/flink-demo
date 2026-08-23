import { createContext, useContext, useState, useCallback, type ReactNode } from 'react';
import type { CepAlert } from '../lib/cepAlerts';
import { appendUniqueEvent } from '../lib/eventFeed';
import type { OrderEvent } from '../lib/orderEvents';

export type { CepAlert } from '../lib/cepAlerts';

export interface EventEnvelope extends OrderEvent {}

export type MetricName = 'listings_count' | 'cart_adds_count' | 'tx_count' | 'confirmed_orders' | 'delivered_orders' | 'top_product' | 'revenue';
export interface WindowStat {
  metric: MetricName;
  scope: 'window' | 'daily';
  window_end: string;
  value: number;
  detail: Record<string, string>;
}
export type DashboardMessage = (EventEnvelope | WindowStat | CepAlert) & {
  replay?: boolean;
};
export function isWindowStat(value: DashboardMessage): value is WindowStat {
  return 'metric' in value && 'scope' in value && 'window_end' in value;
}
export function isEventEnvelope(value: DashboardMessage): value is EventEnvelope {
  return 'event_id' in value
    && 'event_type' in value
    && 'actor_id' in value
    && 'actor_role' in value
    && 'timestamp' in value
    && 'payload' in value;
}

interface EventState {
  events: EventEnvelope[];
  addEvent: (event: EventEnvelope) => void;
  clearEvents: () => void;
}

const EventContext = createContext<EventState | undefined>(undefined);

export function EventProvider({ children }: { children: ReactNode }) {
  const [events, setEvents] = useState<EventEnvelope[]>([]);

  const addEvent = useCallback((event: EventEnvelope) => {
    setEvents((prev) => appendUniqueEvent(prev, event));
  }, []);

  const clearEvents = useCallback(() => {
    setEvents([]);
  }, []);

  return (
    <EventContext.Provider value={{ events, addEvent, clearEvents }}>
      {children}
    </EventContext.Provider>
  );
}

export function useEvents(): EventState {
  const ctx = useContext(EventContext);
  if (!ctx) {
    throw new Error('useEvents must be used within EventProvider');
  }
  return ctx;
}
