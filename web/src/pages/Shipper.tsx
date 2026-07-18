import { useState, useEffect, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents } from '../context/EventContext';
import {
  listShipperJobs, pickJob, deliverJob,
} from '../api/client';

interface OrderItem {
  product_id: string; product_name: string; quantity: number; unit_price: number;
}

interface Order {
  id: string; buyer_id: string; seller_id: string;
  items: OrderItem[]; total_amount: number; shipping_address: string;
  status: string; created_at: string;
}

export default function Shipper() {
  const { name, token, clearSession } = useSession();
  const navigate = useNavigate();
  const { events, addEvent } = useEvents();
  useWebSocket(addEvent);

  const [jobs, setJobs] = useState<Order[]>([]);
  const [pickedOrders, setPickedOrders] = useState<Record<string, number>>({}); // orderID → countdown seconds remaining
  const [error, setError] = useState('');
  const countdownRef = useRef<number | null>(null);

  useEffect(() => {
    if (!name || !token) { navigate('/'); }
  }, [name, token, navigate]);

  const loadJobs = useCallback(async () => {
    if (!token) return;
    const resp = await listShipperJobs(token);
    if (resp.ok) setJobs(await resp.json());
  }, [token]);

  useEffect(() => { loadJobs(); }, [loadJobs]);

  // Listen for order.confirmed events to refresh job board
  useEffect(() => {
    if (events.some(e => e.event_type === 'order.confirmed')) loadJobs();
  }, [events, loadJobs]);

  // Countdown timer effect
  useEffect(() => {
    const hasActiveCountdowns = Object.values(pickedOrders).some((s) => s > 0);
    if (!hasActiveCountdowns) {
      if (countdownRef.current) {
        clearInterval(countdownRef.current);
        countdownRef.current = null;
      }
      return;
    }
    if (!countdownRef.current) {
      countdownRef.current = window.setInterval(() => {
        setPickedOrders((prev) => {
          const next: Record<string, number> = {};
          for (const [id, seconds] of Object.entries(prev)) {
            next[id] = Math.max(0, seconds - 1);
          }
          return next;
        });
      }, 1000);
    }
    return () => {
      if (countdownRef.current) {
        clearInterval(countdownRef.current);
        countdownRef.current = null;
      }
    };
  }, [pickedOrders]);

  const handlePickJob = async (orderId: string) => {
    if (!token) return;
    setError('');
    const resp = await pickJob(token, orderId);
    if (!resp.ok) {
      if (resp.status === 409) {
        setError('Job already picked by another shipper');
      } else {
        setError(await resp.text());
      }
      loadJobs();
      return;
    }
    // Start countdown: random 5-15 seconds
    const countdown = Math.floor(Math.random() * 11) + 5;
    setPickedOrders((prev) => ({ ...prev, [orderId]: countdown }));
    // Remove from job board
    setJobs((prev) => prev.filter((j) => j.id !== orderId));
  };

  const handleDeliver = async (orderId: string) => {
    if (!token) return;
    const resp = await deliverJob(token, orderId);
    if (resp.ok) {
      setPickedOrders((prev) => {
        const next = { ...prev };
        delete next[orderId];
        return next;
      });
    }
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  if (!name) return null;

  const activeJobs = Object.entries(pickedOrders).filter(([, s]) => s > 0);
  const deliveredJobs = Object.entries(pickedOrders).filter(([, s]) => s === 0);

  return (
    <div style={{ padding: '20px', maxWidth: '900px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1>Shipper: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>

      {error && <p style={{ color: 'red', marginBottom: '16px' }}>{error}</p>}

      {/* Available Jobs */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Available Jobs</h2>
        <div style={{ marginTop: '12px' }}>
          {jobs.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No jobs available. Waiting for sellers to confirm orders...</p>
          ) : (
            jobs.map((job) => (
              <div key={job.id} style={{
                padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
                marginBottom: '12px', background: '#f9fafb',
              }}>
                <div style={{ fontWeight: 'bold' }}>Order from {job.buyer_id}</div>
                <div style={{ marginTop: '8px', fontSize: '14px', color: '#6b7280' }}>
                  {job.items.map((item, i) => (
                    <div key={i}>{item.quantity}x {item.product_name}</div>
                  ))}
                </div>
                <div style={{ marginTop: '8px', color: '#6b7280', fontSize: '12px' }}>
                  Ship to: {job.shipping_address}
                </div>
                <button
                  onClick={() => handlePickJob(job.id)}
                  style={{ marginTop: '12px', padding: '6px 20px' }}
                >
                  Pick Up Job
                </button>
              </div>
            ))
          )}
        </div>
      </div>

      {/* In Transit (countdown active) */}
      {activeJobs.length > 0 && (
        <div style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #fbbf24' }}>
          <h2>In Transit</h2>
          {activeJobs.map(([orderId, seconds]) => (
            <div key={orderId} style={{
              padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
              marginBottom: '12px', background: '#fffbeb',
            }}>
              <div style={{ fontWeight: 'bold' }}>Order {orderId.slice(0, 8)}...</div>
              <div style={{ fontSize: '24px', color: '#d97706', marginTop: '8px' }}>
                Delivering in {seconds}s...
              </div>
              <button disabled style={{ marginTop: '12px', padding: '6px 20px', opacity: 0.5 }}>
                Mark Delivered (wait {seconds}s)
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Ready to Deliver (countdown finished) */}
      {deliveredJobs.length > 0 && (
        <div style={{ background: 'white', borderRadius: '8px', padding: '20px', border: '1px solid #059669' }}>
          <h2>Ready to Deliver</h2>
          {deliveredJobs.map(([orderId]) => (
            <div key={orderId} style={{
              padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
              marginBottom: '12px', background: '#ecfdf5',
            }}>
              <div style={{ fontWeight: 'bold' }}>Order {orderId.slice(0, 8)}...</div>
              <div style={{ color: '#059669', marginTop: '4px' }}>Transit complete!</div>
              <button
                onClick={() => handleDeliver(orderId)}
                style={{ marginTop: '12px', padding: '6px 20px', background: '#059669' }}
              >
                Mark Delivered
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
