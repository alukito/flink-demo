import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents } from '../context/EventContext';
import {
  addProduct, listSellerProducts, listSellerOrders, confirmOrder,
} from '../api/client';
import { isSellerOrderEvent } from '../lib/orderEvents';

interface Product {
  id: string; name: string; price: number; quantity: number; seller_id: string;
}

interface OrderItem {
  product_id: string; product_name: string; quantity: number; unit_price: number;
}

interface Order {
  id: string; buyer_id: string; buyer_name: string; seller_id: string; seller_name: string;
  items: OrderItem[]; total_amount: number; shipping_address: string;
  status: string; created_at: string;
}

function formatPrice(price: number): string {
  return 'Rp ' + price.toLocaleString('id-ID');
}

export default function Seller() {
  const { id, name, token, clearSession } = useSession();
  const navigate = useNavigate();
  const { events, addEvent } = useEvents();
  useWebSocket(addEvent);

  const [products, setProducts] = useState<Product[]>([]);
  const [orders, setOrders] = useState<Order[]>([]);
  const [newProduct, setNewProduct] = useState({ name: '', price: '', quantity: '' });
  const [error, setError] = useState('');
  const [adding, setAdding] = useState(false);
  const [confirmingId, setConfirmingId] = useState<string | null>(null);

  const loadProducts = useCallback(async () => {
    if (!token) return;
    const resp = await listSellerProducts(token);
    if (resp.ok) setProducts(await resp.json());
  }, [token]);

  const loadOrders = useCallback(async () => {
    if (!token) return;
    const resp = await listSellerOrders(token);
    if (resp.ok) setOrders(await resp.json());
  }, [token]);

  useEffect(() => { loadProducts(); loadOrders(); }, [loadProducts, loadOrders]);

  useEffect(() => {
    if (events[0] && isSellerOrderEvent(events[0], id)) loadOrders();
  }, [events, id, loadOrders]);

  const handleAddProduct = async () => {
    if (!token || adding) return;
    setError('');
    const price = parseInt(newProduct.price);
    const quantity = parseInt(newProduct.quantity);
    if (!newProduct.name || !price || price <= 0) {
      setError('Name and positive price are required');
      return;
    }
    setAdding(true);
    try {
      const resp = await addProduct(token, newProduct.name, price, quantity || 0);
      if (!resp.ok) {
        setError(await resp.text());
        return;
      }
      setNewProduct({ name: '', price: '', quantity: '' });
      loadProducts();
    } finally {
      setAdding(false);
    }
  };

  const handleConfirmOrder = async (orderId: string) => {
    if (!token || confirmingId) return;
    setConfirmingId(orderId);
    try {
      const resp = await confirmOrder(token, orderId);
      if (resp.ok) loadOrders();
    } finally {
      setConfirmingId(null);
    }
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  return (
    <div style={{ padding: '20px', maxWidth: '900px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1>Seller: {name}</h1>
        <button onClick={handleLogout}>Logout</button>
      </div>

      {/* Product Panel */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Add Product</h2>
        <div style={{ display: 'flex', gap: '12px', marginTop: '12px' }}>
          <input
            placeholder="Product name"
            value={newProduct.name}
            onChange={(e) => setNewProduct({ ...newProduct, name: e.target.value })}
            style={{ flex: 1, padding: '8px' }}
          />
          <input
            placeholder="Price (Rp)"
            type="number"
            value={newProduct.price}
            onChange={(e) => setNewProduct({ ...newProduct, price: e.target.value })}
            style={{ width: '140px', padding: '8px' }}
          />
          <input
            placeholder="Quantity"
            type="number"
            value={newProduct.quantity}
            onChange={(e) => setNewProduct({ ...newProduct, quantity: e.target.value })}
            style={{ width: '100px', padding: '8px' }}
          />
          <button onClick={handleAddProduct} disabled={adding} style={{ padding: '8px 24px' }}>
            {adding ? 'Adding...' : 'Add'}
          </button>
        </div>
        {error && <p style={{ color: 'red', marginTop: '8px' }}>{error}</p>}

        <h3 style={{ marginTop: '20px' }}>Your Products</h3>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px', marginTop: '12px' }}>
          {products.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No products listed yet.</p>
          ) : (
            products.map((p) => (
              <div key={p.id} style={{
                padding: '12px', borderRadius: '6px', border: '1px solid #d1d5db',
                minWidth: '200px', background: '#f9fafb',
              }}>
                <div style={{ fontWeight: 'bold' }}>{p.name}</div>
                <div style={{ color: '#6b7280' }}>{formatPrice(p.price)}</div>
                <div style={{ color: '#6b7280', fontSize: '12px' }}>Qty: {p.quantity}</div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Order Inbox */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Order Inbox</h2>
        <div style={{ marginTop: '12px' }}>
          {orders.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No orders yet.</p>
          ) : (
            orders.map((o) => (
              <div key={o.id} style={{
                padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
                marginBottom: '12px', background: '#f9fafb',
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ fontWeight: 'bold' }}>Order from {o.buyer_name ?? o.buyer_id}</span>
                  <span style={{
                    padding: '2px 8px', borderRadius: '4px', fontSize: '12px',
                    background: o.status === 'checkout' ? '#fef3c7' : '#d1fae5',
                    color: o.status === 'checkout' ? '#d97706' : '#059669',
                  }}>
                    {o.status}
                  </span>
                </div>
                <div style={{ marginTop: '8px', fontSize: '14px', color: '#6b7280' }}>
                  {o.items.map((item, i) => (
                    <div key={i}>{item.quantity}x {item.product_name} ({formatPrice(item.unit_price)})</div>
                  ))}
                </div>
                <div style={{ marginTop: '8px', fontWeight: 'bold' }}>
                  Total: {formatPrice(o.total_amount)}
                </div>
                <div style={{ marginTop: '4px', color: '#6b7280', fontSize: '12px' }}>
                  Ship to: {o.shipping_address}
                </div>
                {o.status === 'checkout' && (
                  <button
                    onClick={() => handleConfirmOrder(o.id)}
                    disabled={confirmingId === o.id}
                    style={{ marginTop: '12px', padding: '6px 20px' }}
                  >
                    {confirmingId === o.id ? 'Confirming...' : 'Confirm Order'}
                  </button>
                )}
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
