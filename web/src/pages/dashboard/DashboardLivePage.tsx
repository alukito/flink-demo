import type { EventEnvelope } from '../../context/EventContext';
import { useDashboard } from '../../dashboard/DashboardContext';

const WIB_TIMESTAMP = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Jakarta',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23',
});

function EventPayload({ payload }: { payload: EventEnvelope['payload'] }) {
  const serialized = JSON.stringify(payload);
  return (
    <details className="event-payload">
      <summary title={serialized}>{serialized}</summary>
      <pre>{JSON.stringify(payload, null, 2)}</pre>
    </details>
  );
}

export function DashboardLivePage() {
  const { events } = useDashboard();

  return (
    <section className="dashboard-page dashboard-page--live" aria-labelledby="live-feed-heading">
      <header className="dashboard-page__heading">
        <div>
          <h2 id="live-feed-heading">Live event feed</h2>
          <p>Raw Kafka events, newest first. Times use Western Indonesian Time.</p>
        </div>
        <strong>{events.length.toLocaleString('id-ID')} / 100</strong>
      </header>
      <div className="event-feed__scroller">
        <table className="event-feed" aria-label="Live event feed">
          <thead>
            <tr>
              <th scope="col">WIB time</th>
              <th scope="col">Event</th>
              <th scope="col">Actor</th>
              <th scope="col">Payload</th>
            </tr>
          </thead>
          <tbody>
            {events.length === 0 ? (
              <tr className="event-feed__empty">
                <td colSpan={4}>Waiting for events…</td>
              </tr>
            ) : events.map((event) => (
              <tr key={event.event_id}>
                <td data-label="WIB time"><time dateTime={event.timestamp}>{WIB_TIMESTAMP.format(new Date(event.timestamp))}</time></td>
                <td data-label="Event"><strong>{event.event_type}</strong></td>
                <td data-label="Actor">
                  <span>{event.actor_name ?? event.actor_id}</span>
                  <small>{event.actor_role}</small>
                </td>
                <td data-label="Payload"><EventPayload payload={event.payload} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
