export const MAX_LIVE_EVENTS = 100;

interface IdentifiedEvent {
  event_id: string;
}

export function appendUniqueEvent<T extends IdentifiedEvent>(
  events: T[],
  event: T,
  maxEvents = MAX_LIVE_EVENTS,
): T[] {
  const retained = events.length <= maxEvents ? events : events.slice(0, maxEvents);
  if (retained.some((entry) => entry.event_id === event.event_id)) return retained;
  return [event, ...retained].slice(0, maxEvents);
}
