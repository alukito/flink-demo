import { useState, useEffect, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents } from '../context/EventContext';
import {
  listShipperJobs, listShipperDeliveries, pickJob, deliverJob,
} from '../api/client';
import {
  copyDeliveries,
  secondsUntilReady,
  type Delivery,
  type ShipperDeliveries,
} from '../lib/deliveries';
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

export default function Shipper() {
  const { id, name, token, clearSession } = useSession();
  const navigate = useNavigate();
  const { events, addEvent } = useEvents();
  useWebSocket(addEvent);

  const [shipperState, setShipperState] = useState<ShipperState>(emptyShipperState);
  const [now, setNow] = useState(() => new Date());
  const [error, setError] = useState('');
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
      commit: ({ jobs, deliveries }) => setShipperState({
        jobs,
        deliveries: copyDeliveries(deliveries),
      }),
    });
  }, [token]);

  useEffect(() => {
    loadShipperState();
  }, [loadShipperState]);

  useEffect(() => {
    const pageTimer = window.setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(pageTimer);
  }, []);

  useEffect(() => {
    const newestEvent = events[0];
    if (!newestEvent) return;

    if (isShipperQueueEvent(newestEvent)) {
      loadShipperState();
      return;
    }

    if (newestEvent.event_type === 'shipment.delivered' && newestEvent.payload.shipper_id === id) {
      loadShipperState();
    }
  }, [events, id, loadShipperState]);

  const handlePickJob = async (orderId: string) => {
    if (!token || pickingId) return;
    setError('');
    setPickingId(orderId);
    try {
      const resp = await pickJob(token, orderId);
      if (!resp.ok) {
        setError(resp.status === 409 ? 'Job already picked by another shipper' : await resp.text());
      }
    } finally {
      setPickingId(null);
      await loadShipperState();
    }
  };

  const handleDeliver = async (orderId: string) => {
    if (!token || deliveringId) return;
    setError('');
    setDeliveringId(orderId);
    try {
      const resp = await deliverJob(token, orderId);
      if (!resp.ok) setError(await resp.text());
    } finally {
      setDeliveringId(null);
      await loadShipperState();
    }
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  const history = [...deliveries.history].sort((first, second) =>
    (Date.parse(second.delivered_at ?? '') || 0) - (Date.parse(first.delivered_at ?? '') || 0));

  return (
    <div style={{ padding: '20px', maxWidth: '900px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1>Shipper: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>

      {error && <p style={{ color: 'red', marginBottom: '16px' }}>{error}</p>}

      <section style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Available Jobs</h2>
        <div style={{ marginTop: '12px' }}>
          {jobs.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No jobs available. Waiting for sellers to confirm orders...</p>
          ) : jobs.map((job) => (
            <article key={job.id} style={{
              padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
              marginBottom: '12px', background: '#f9fafb',
            }}>
              <div style={{ fontWeight: 'bold' }}>Delivery to {job.buyer_name ?? job.buyer_id}</div>
              <div style={{ marginTop: '8px', fontSize: '14px', color: '#6b7280' }}>
                {job.items.map((item) => <div key={item.product_id}>{item.quantity}x {item.product_name}</div>)}
              </div>
              <div style={{ marginTop: '8px', color: '#6b7280', fontSize: '12px' }}>
                Seller: {job.seller_name ?? job.seller_id} · Destination: {job.shipping_address}
              </div>
              <div style={{ marginTop: '4px', color: '#6b7280', fontSize: '12px' }}>
                Created: {new Date(job.created_at).toLocaleString()}
              </div>
              <button
                onClick={() => handlePickJob(job.id)}
                disabled={pickingId === job.id}
                style={{ marginTop: '12px', padding: '6px 20px' }}
              >
                {pickingId === job.id ? 'Picking...' : 'Pick Up Job'}
              </button>
            </article>
          ))}
        </div>
      </section>

      <section style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #fbbf24' }}>
        <h2>My Active Deliveries</h2>
        <div style={{ marginTop: '12px' }}>
          {deliveries.active.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No active deliveries.</p>
          ) : deliveries.active.map((delivery) => {
            const remainingSeconds = secondsUntilReady(delivery.ready_at, now);
            const readyAt = Date.parse(delivery.ready_at ?? '');
            const isReady = Number.isFinite(readyAt) && remainingSeconds === 0;

            return (
              <article key={delivery.id} style={{
                padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
                marginBottom: '12px', background: isReady ? '#ecfdf5' : '#fffbeb',
              }}>
                <div style={{ fontWeight: 'bold' }}>Delivery to {delivery.buyer_name ?? delivery.buyer_id}</div>
                <div style={{ marginTop: '8px', fontSize: '14px', color: '#6b7280' }}>
                  {delivery.items.map((item) => <div key={item.product_id}>{item.quantity}x {item.product_name}</div>)}
                </div>
                <div style={{ marginTop: '8px', color: '#6b7280', fontSize: '12px' }}>
                  Seller: {delivery.seller_name ?? delivery.seller_id} · Destination: {delivery.shipping_address}
                </div>
                <div style={{ marginTop: '4px', color: '#6b7280', fontSize: '12px' }}>
                  Picked: {delivery.picked_at ? new Date(delivery.picked_at).toLocaleString() : 'unknown'} · Ready: {delivery.ready_at ? new Date(delivery.ready_at).toLocaleString() : 'unknown'}
                </div>
                <div style={{ color: isReady ? '#059669' : '#d97706', marginTop: '8px' }}>
                  {isReady ? 'Ready to deliver' : `Ready in ${remainingSeconds}s`}
                </div>
                <button
                  onClick={() => handleDeliver(delivery.id)}
                  disabled={!isReady || deliveringId === delivery.id}
                  style={{ marginTop: '12px', padding: '6px 20px', background: isReady ? '#059669' : undefined }}
                >
                  {deliveringId === delivery.id ? 'Delivering...' : isReady ? 'Mark Delivered' : `Mark Delivered (wait ${remainingSeconds}s)`}
                </button>
              </article>
            );
          })}
        </div>
      </section>

      <section style={{ background: 'white', borderRadius: '8px', padding: '20px', border: '1px solid #059669' }}>
        <h2>My Delivery History</h2>
        <div style={{ marginTop: '12px' }}>
          {history.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No delivered jobs yet.</p>
          ) : history.map((delivery) => {
            const pickedAt = Date.parse(delivery.picked_at ?? '');
            const deliveredAt = Date.parse(delivery.delivered_at ?? '');
            const elapsedSeconds = Number.isFinite(pickedAt) && Number.isFinite(deliveredAt)
              ? Math.max(0, Math.ceil((deliveredAt - pickedAt) / 1000))
              : null;

            return (
              <article key={delivery.id} style={{
                padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
                marginBottom: '12px', background: '#f0fdf4',
              }}>
                <div style={{ fontWeight: 'bold' }}>Delivered to {delivery.buyer_name ?? delivery.buyer_id}</div>
                <div style={{ marginTop: '8px', fontSize: '14px', color: '#6b7280' }}>
                  {delivery.items.map((item) => <div key={item.product_id}>{item.quantity}x {item.product_name}</div>)}
                </div>
                <div style={{ marginTop: '8px', color: '#6b7280', fontSize: '12px' }}>
                  Seller: {delivery.seller_name ?? delivery.seller_id} · Destination: {delivery.shipping_address}
                </div>
                <div style={{ marginTop: '4px', color: '#6b7280', fontSize: '12px' }}>
                  Picked: {delivery.picked_at ? new Date(delivery.picked_at).toLocaleString() : 'unknown'} · Delivered: {delivery.delivered_at ? new Date(delivery.delivered_at).toLocaleString() : 'unknown'}
                </div>
                <div style={{ color: '#059669', marginTop: '8px' }}>
                  Elapsed: {elapsedSeconds === null ? 'unknown' : `${elapsedSeconds}s`}
                </div>
              </article>
            );
          })}
        </div>
      </section>
    </div>
  );
}
