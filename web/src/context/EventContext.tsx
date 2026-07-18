import { createContext, useContext, useState, useCallback, type ReactNode } from 'react';

export interface EventEnvelope {
  event_id: string;
  event_type: string;
  actor_id: string;
  actor_role: string;
  timestamp: string;
  payload: Record<string, any>;
}

interface EventState {
  events: EventEnvelope[];
  addEvent: (event: EventEnvelope) => void;
  clearEvents: () => void;
}

const EventContext = createContext<EventState | undefined>(undefined);

const MAX_EVENTS = 100;

export function EventProvider({ children }: { children: ReactNode }) {
  const [events, setEvents] = useState<EventEnvelope[]>([]);

  const addEvent = useCallback((event: EventEnvelope) => {
    setEvents((prev) => [event, ...prev].slice(0, MAX_EVENTS));
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
