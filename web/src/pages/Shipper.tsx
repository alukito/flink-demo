import { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  deliverJob,
  listShipperDeliveries,
  listShipperJobs,
  pickJob,
} from '../api/client';
import { ActionCard } from '../components/ActionCard';
import { RoleLayout } from '../components/RoleLayout';
import { Button } from '../components/ui/Button';
import { EmptyState } from '../components/ui/EmptyState';
import { FeedbackBanner } from '../components/ui/FeedbackBanner';
import { StatusBadge } from '../components/ui/StatusBadge';
import { useEvents } from '../context/EventContext';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import {
  copyDeliveries,
  deliveryReadiness,
  type Delivery,
  type ShipperDeliveries,
} from '../lib/deliveries';
import { createFeedback, type ActionFeedback } from '../lib/feedback';
import { isShipperQueueEvent } from '../lib/orderEvents';
import { loadLatestShipperSnapshot } from '../lib/shipperRefresh';

interface ShipperState {
  jobs: Delivery[];
  deliveries: ShipperDeliveries;
}

const emptyShipperState: ShipperState = {
  jobs: [],
  deliveries: { active: [], history: [] },
};

function participantName(delivery: Delivery): string {
  return delivery.buyer_name ?? delivery.buyer_id;
}

function formatTimestamp(value: string | undefined): string {
  const milliseconds = Date.parse(value ?? '');
  return Number.isFinite(milliseconds) ? new Date(milliseconds).toLocaleString() : 'Unavailable';
}

function formatElapsed(pickedAt: string | undefined, deliveredAt: string | undefined): string {
  const pickedMilliseconds = Date.parse(pickedAt ?? '');
  const deliveredMilliseconds = Date.parse(deliveredAt ?? '');
  if (!Number.isFinite(pickedMilliseconds) || !Number.isFinite(deliveredMilliseconds)) {
    return 'Unavailable';
  }

  const elapsedSeconds = Math.max(0, Math.ceil((deliveredMilliseconds - pickedMilliseconds) / 1000));
  const hours = Math.floor(elapsedSeconds / 3600);
  const minutes = Math.floor((elapsedSeconds % 3600) / 60);
  const seconds = elapsedSeconds % 60;
  return [
    hours > 0 ? `${hours}h` : '',
    minutes > 0 ? `${minutes}m` : '',
    `${seconds}s`,
  ].filter(Boolean).join(' ');
}

function DeliveryItems({ delivery }: { delivery: Delivery }) {
  return (
    <ul className="shipper-items" aria-label="Products">
      {delivery.items.map((item, index) => (
        <li key={`${item.product_id}-${index}`}>{item.quantity} × {item.product_name}</li>
      ))}
    </ul>
  );
}

export default function Shipper() {
  const { id, name, token, clearSession } = useSession();
  const navigate = useNavigate();
  const { events, addEvent } = useEvents();
  useWebSocket(addEvent);

  const [shipperState, setShipperState] = useState<ShipperState>(emptyShipperState);
  const [now, setNow] = useState(() => new Date());
  const [feedback, setFeedback] = useState<ActionFeedback | null>(null);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [pickingId, setPickingId] = useState<string | null>(null);
  const [deliveringId, setDeliveringId] = useState<string | null>(null);
  const latestRefreshGeneration = useRef(0);
  const { jobs, deliveries } = shipperState;

  const loadShipperState = useCallback(async () => {
    if (!token) return;

    const generation = ++latestRefreshGeneration.current;
    await loadLatestShipperSnapshot<Delivery[], ShipperDeliveries>({
      generation,
      getLatestGeneration: () => latestRefreshGeneration.current,
      listJobs: () => listShipperJobs(token),
      listDeliveries: () => listShipperDeliveries(token),
      commit: ({ jobs: nextJobs, deliveries: nextDeliveries }) => {
        setShipperState({
          jobs: nextJobs,
          deliveries: copyDeliveries(nextDeliveries),
        });
        setRefreshError(null);
      },
      reportError: setRefreshError,
    });
  }, [token]);

  useEffect(() => {
    void loadShipperState();
  }, [loadShipperState]);

  useEffect(() => {
    const pageTimer = window.setInterval(() => setNow(new Date()), 1000);
    return () => window.clearInterval(pageTimer);
  }, []);

  useEffect(() => {
    const newestEvent = events[0];
    if (!newestEvent) return;

    if (isShipperQueueEvent(newestEvent)) {
      void loadShipperState();
      return;
    }

    if (newestEvent.event_type === 'shipment.delivered' && newestEvent.payload.shipper_id === id) {
      void loadShipperState();
    }
  }, [events, id, loadShipperState]);

  const handlePickJob = async (orderId: string) => {
    if (!token || pickingId) return;
    setFeedback(null);
    setPickingId(orderId);
    try {
      const response = await pickJob(token, orderId);
      if (!response.ok) {
        const message = (await response.text()).trim();
        setFeedback(createFeedback(
          'error',
          response.status === 409
            ? 'Job already picked by another shipper'
            : message || 'Job could not be picked up. Try again.',
        ));
        return;
      }
      setFeedback(createFeedback('success', 'Job picked up'));
    } catch {
      setFeedback(createFeedback('error', 'Job could not be picked up. Try again.'));
    } finally {
      setPickingId(null);
      await loadShipperState();
    }
  };

  const handleDeliver = async (delivery: Delivery) => {
    if (!token || deliveringId || deliveryReadiness(delivery.ready_at, new Date()).kind !== 'ready') return;
    setFeedback(null);
    setDeliveringId(delivery.id);
    try {
      const response = await deliverJob(token, delivery.id);
      if (!response.ok) {
        const message = (await response.text()).trim();
        setFeedback(createFeedback('error', message || 'Delivery could not be completed. Try again.'));
        return;
      }
      setFeedback(createFeedback('success', 'Delivery completed'));
    } catch {
      setFeedback(createFeedback('error', 'Delivery could not be completed. Try again.'));
    } finally {
      setDeliveringId(null);
      await loadShipperState();
    }
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  const history = [...deliveries.history].sort((first, second) => (
    (Date.parse(second.delivered_at ?? '') || 0) - (Date.parse(first.delivered_at ?? '') || 0)
  ));

  return (
    <RoleLayout
      roleLabel="Deliver"
      participantName={name ?? 'Participant'}
      pulseKey={events[0]?.event_id ?? 'initial'}
      onLogout={handleLogout}
    >
      <div className="shipper-view">
        {feedback ? <FeedbackBanner tone={feedback.tone}>{feedback.message}</FeedbackBanner> : null}
        {refreshError ? <FeedbackBanner tone="error">{refreshError}</FeedbackBanner> : null}

        <div className="shipper-workbench">
          <ActionCard
            className="shipper-panel shipper-active"
            title="Active delivery"
            description={`${deliveries.active.length} ${deliveries.active.length === 1 ? 'run' : 'runs'} in progress.`}
          >
            {deliveries.active.length === 0 ? (
              <EmptyState
                title="No active delivery"
                description="Pick up an available job to begin a run."
              />
            ) : (
              <div className="shipper-card-list">
                {deliveries.active.map((delivery) => {
                  const readiness = deliveryReadiness(delivery.ready_at, now);
                  return (
                    <article className="shipper-card shipper-active-card" key={delivery.id}>
                      <header className="shipper-card__header">
                        <div>
                          <span className="shipper-card__label">Deliver to</span>
                          <h3>{participantName(delivery)}</h3>
                        </div>
                        <StatusBadge tone="info">In transit</StatusBadge>
                      </header>
                      <DeliveryItems delivery={delivery} />
                      <dl className="shipper-meta">
                        <div><dt>Destination</dt><dd>{delivery.shipping_address}</dd></div>
                        <div><dt>Seller</dt><dd>{delivery.seller_name ?? delivery.seller_id}</dd></div>
                        <div><dt>Picked</dt><dd>{formatTimestamp(delivery.picked_at)}</dd></div>
                      </dl>
                      <p className="shipper-readiness" data-kind={readiness.kind}>{readiness.label}</p>
                      <Button
                        type="button"
                        loading={deliveringId === delivery.id}
                        loadingLabel="Delivering…"
                        disabled={readiness.kind !== 'ready' || deliveringId !== null}
                        onClick={() => void handleDeliver(delivery)}
                      >
                        Mark delivered
                      </Button>
                    </article>
                  );
                })}
              </div>
            )}
          </ActionCard>

          <ActionCard
            className="shipper-panel shipper-jobs"
            title="Available jobs"
            description="Confirmed orders ready for a shipper."
          >
            {jobs.length === 0 ? (
              <EmptyState title="No jobs available" description="New confirmed orders will appear here." />
            ) : (
              <div className="shipper-card-list">
                {jobs.map((job) => (
                  <article className="shipper-card shipper-job-card" key={job.id}>
                    <header className="shipper-card__header">
                      <div>
                        <span className="shipper-card__label">Deliver to</span>
                        <h3>{participantName(job)}</h3>
                      </div>
                      <StatusBadge tone="neutral">Available</StatusBadge>
                    </header>
                    <DeliveryItems delivery={job} />
                    <dl className="shipper-meta">
                      <div><dt>Destination</dt><dd>{job.shipping_address}</dd></div>
                      <div><dt>Seller</dt><dd>{job.seller_name ?? job.seller_id}</dd></div>
                      <div><dt>Created</dt><dd>{formatTimestamp(job.created_at)}</dd></div>
                    </dl>
                    <Button
                      type="button"
                      variant="secondary"
                      loading={pickingId === job.id}
                      loadingLabel="Picking up…"
                      disabled={pickingId !== null}
                      onClick={() => void handlePickJob(job.id)}
                    >
                      Pick up job
                    </Button>
                  </article>
                ))}
              </div>
            )}
          </ActionCard>

          <ActionCard
            className="shipper-panel shipper-history"
            title="Completed deliveries"
            description="Delivered runs, newest first."
          >
            {history.length === 0 ? (
              <EmptyState title="No completed deliveries" description="Finished runs will be recorded here." />
            ) : (
              <div className="shipper-history-list">
                {history.map((delivery) => (
                  <article className="shipper-card shipper-history-card" key={delivery.id}>
                    <header className="shipper-card__header">
                      <div>
                        <span className="shipper-card__label">Delivered to</span>
                        <h3>{participantName(delivery)}</h3>
                      </div>
                      <StatusBadge tone="success">Delivered</StatusBadge>
                    </header>
                    <DeliveryItems delivery={delivery} />
                    <dl className="shipper-meta shipper-history-meta">
                      <div><dt>Destination</dt><dd>{delivery.shipping_address}</dd></div>
                      <div><dt>Picked</dt><dd>{formatTimestamp(delivery.picked_at)}</dd></div>
                      <div><dt>Delivered</dt><dd>{formatTimestamp(delivery.delivered_at)}</dd></div>
                      <div><dt>Elapsed</dt><dd>{formatElapsed(delivery.picked_at, delivery.delivered_at)}</dd></div>
                    </dl>
                  </article>
                ))}
              </div>
            )}
          </ActionCard>
        </div>
      </div>
    </RoleLayout>
  );
}
