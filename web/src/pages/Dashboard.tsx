import { useEffect, useState } from 'react';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents, type EventEnvelope } from '../context/EventContext';
import { createSession } from '../api/client';

function eventColor(eventType: string): string {
  if (eventType.startsWith('product')) return '#2563eb';
  if (eventType.startsWith('cart')) return '#7c3aed';
  if (eventType.startsWith('order')) return '#059669';
  if (eventType.startsWith('shipment.picked')) return '#d97706';
  if (eventType.startsWith('shipment.delivered')) return '#dc2626';
  return '#6b7280';
}

function EventRow({ event }: { event: EventEnvelope }) {
  const time = new Date(event.timestamp).toLocaleTimeString();
  return (
    <div style={{
      display: 'flex', gap: '12px', padding: '8px 12px',
      borderBottom: '1px solid #e5e7eb', fontFamily: 'monospace', fontSize: '13px',
    }}>
      <span style={{ color: '#9ca3af', minWidth: '80px' }}>{time}</span>
      <span style={{ color: eventColor(event.event_type), minWidth: '160px', fontWeight: 'bold' }}>
        {event.event_type}
      </span>
      <span style={{ color: '#6b7280', minWidth: '100px' }}>{event.actor_id}</span>
      <span style={{ color: '#374151' }}>
        {JSON.stringify(event.payload)}
      </span>
    </div>
  );
}

export default function Dashboard() {
  const { events, addEvent, clearEvents } = useEvents();
  const [dashToken, setDashToken] = useState<string | null>(() => localStorage.getItem('dash_token'));
  const { connected } = useWebSocket(addEvent, dashToken);

  // Auto-create a dashboard session for the WebSocket connection.
  // Uses a SEPARATE localStorage key (dash_token) so it does NOT
  // pollute the shared SessionContext (token/name/role) that the
  // role pages rely on.
  useEffect(() => {
    if (dashToken) return;

    const name = `dashboard-${Math.random().toString(36).slice(2, 8)}`;
    createSession(name, 'dashboard')
      .then((resp) => {
        localStorage.setItem('dash_token', resp.token);
        setDashToken(resp.token);
      })
      .catch((err) => console.error('[dashboard] failed to create session', err));
  }, [dashToken]);

  return (
    <div style={{ padding: '20px', maxWidth: '1200px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1>Dashboard</h1>
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
          <span style={{
            padding: '4px 12px', borderRadius: '12px', fontSize: '12px',
            background: connected ? '#d1fae5' : '#fee2e2',
            color: connected ? '#059669' : '#dc2626',
          }}>
            {connected ? 'Connected' : dashToken ? 'Reconnecting...' : 'Connecting...'}
          </span>
          <button onClick={clearEvents} style={{ padding: '6px 16px', fontSize: '12px' }}>
            Clear
          </button>
        </div>
      </div>

      <div style={{ marginBottom: '16px' }}>
        <h2>Level 1 — Live Event Feed</h2>
        <p style={{ color: '#6b7280', fontSize: '14px' }}>
          Raw events from Kafka, delivered via WebSocket (stateless consumer, no processing)
        </p>
      </div>

      <div style={{
        background: 'white', borderRadius: '8px', border: '1px solid #e5e7eb',
        maxHeight: '600px', overflowY: 'auto',
      }}>
        {events.length === 0 ? (
          <div style={{ padding: '40px', textAlign: 'center', color: '#9ca3af' }}>
            Waiting for events...
          </div>
        ) : (
          events.map((event) => (
            <EventRow key={event.event_id} event={event} />
          ))
        )}
      </div>
    </div>
  );
}
