import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents } from '../context/EventContext';
import {
  listBuyerProducts, addToCart, checkout, listBuyerOrders,
} from '../api/client';
import { cartItemCount } from '../lib/cart';
import { isBuyerOrderEvent } from '../lib/orderEvents';

interface Product {
  id: string; name: string; price: number; quantity: number; seller_id: string; seller_name: string;
}

interface CartItem {
  product: Product; quantity: number;
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

export default function Buyer() {
  const { id, name, token, clearSession } = useSession();
  const navigate = useNavigate();
  const { events, addEvent } = useEvents();
  useWebSocket(addEvent);

  const [products, setProducts] = useState<Product[]>([]);
  const [cart, setCart] = useState<CartItem[]>([]);
  const [cartId, setCartId] = useState(() => crypto.randomUUID());
  const [orders, setOrders] = useState<Order[]>([]);
  const [shippingAddress, setShippingAddress] = useState('');
  const [error, setError] = useState('');
  const [showCheckout, setShowCheckout] = useState(false);
  const [checkingOut, setCheckingOut] = useState(false);
  const [cartAddingId, setCartAddingId] = useState<string | null>(null);

  const loadProducts = useCallback(async () => {
    if (!token) return;
    const resp = await listBuyerProducts(token);
    if (resp.ok) setProducts(await resp.json());
  }, [token]);

  const loadOrders = useCallback(async () => {
    if (!token) return;
    const resp = await listBuyerOrders(token);
    if (resp.ok) setOrders(await resp.json());
  }, [token]);

  useEffect(() => { loadProducts(); loadOrders(); }, [loadProducts, loadOrders]);

  useEffect(() => {
    if (events[0]?.event_type === 'product.listed') loadProducts();
  }, [events, loadProducts]);

  useEffect(() => {
    if (events[0] && isBuyerOrderEvent(events[0], id)) loadOrders();
  }, [events, id, loadOrders]);

  const cartTotal = cart.reduce((sum, item) => sum + item.product.price * item.quantity, 0);

  const handleAddToCart = async (product: Product) => {
    if (!token || cartAddingId) return;
    setCartAddingId(product.id);
    try {
      const existing = cart.find((item) => item.product.id === product.id);
      if (existing) {
        setCart(cart.map((item) =>
          item.product.id === product.id ? { ...item, quantity: item.quantity + 1 } : item
        ));
      } else {
        setCart([...cart, { product, quantity: 1 }]);
      }
      await addToCart(token, cartId, product.id, 1);
    } finally {
      setCartAddingId(null);
    }
  };

  const handleCheckout = async () => {
    if (!token || cart.length === 0 || checkingOut) return;
    setError('');
    setCheckingOut(true);
    try {
      const items = cart.map((item) => ({
        product_id: item.product.id,
        quantity: item.quantity,
      }));
      const resp = await checkout(token, cartId, items, shippingAddress);
      if (!resp.ok) {
        setError(await resp.text());
        return;
      }
      setCart([]);
      setCartId(crypto.randomUUID());
      setShippingAddress('');
      setShowCheckout(false);
      loadOrders();
    } finally {
      setCheckingOut(false);
    }
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  return (
    <div style={{ padding: '20px', maxWidth: '900px', margin: '0 auto' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1>Buyer: {name}</h1>
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
          <span style={{ padding: '4px 12px', borderRadius: '4px', background: '#e0e7ff', fontSize: '14px' }}>
            Cart: {cartItemCount(cart)} items — {formatPrice(cartTotal)}
          </span>
          {cart.length > 0 && (
            <button onClick={() => setShowCheckout(!showCheckout)} style={{ padding: '6px 16px' }}>
              {showCheckout ? 'Cancel' : 'Checkout'}
            </button>
          )}
          <button onClick={handleLogout}>Logout</button>
        </div>
      </div>

      {/* Checkout form */}
      {showCheckout && (
        <div style={{
          background: 'white', borderRadius: '8px', padding: '20px',
          marginBottom: '20px', border: '1px solid #e5e7eb',
        }}>
          <h2>Checkout</h2>
          <div style={{ marginTop: '12px' }}>
            <input
              placeholder="Shipping address"
              value={shippingAddress}
              onChange={(e) => setShippingAddress(e.target.value)}
              style={{ width: '100%', padding: '8px', marginBottom: '12px' }}
            />
            <div style={{ marginBottom: '12px' }}>
              {cart.map((item) => (
                <div key={item.product.id} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0' }}>
                  <span>{item.quantity}x {item.product.name}</span>
                  <span>{formatPrice(item.product.price * item.quantity)}</span>
                </div>
              ))}
              <div style={{ fontWeight: 'bold', borderTop: '1px solid #e5e7eb', paddingTop: '8px' }}>
                Total: {formatPrice(cartTotal)}
              </div>
            </div>
            {error && <p style={{ color: 'red', marginBottom: '8px' }}>{error}</p>}
            <button
              onClick={handleCheckout}
              disabled={!shippingAddress || checkingOut}
              style={{ padding: '8px 24px' }}
            >
              {checkingOut ? 'Placing Order...' : 'Place Order'}
            </button>
          </div>
        </div>
      )}

      {/* Product Catalog */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', marginBottom: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Product Catalog</h2>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px', marginTop: '12px' }}>
          {products.length === 0 ? (
            <p style={{ color: '#9ca3af' }}>No products available yet. Wait for sellers to list products.</p>
          ) : (
            products.map((p) => (
              <div key={p.id} style={{
                padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
                minWidth: '200px', background: '#f9fafb',
              }}>
                <div style={{ fontWeight: 'bold' }}>{p.name}</div>
                <div style={{ color: '#6b7280' }}>{formatPrice(p.price)}</div>
                <div style={{ color: '#9ca3af', fontSize: '12px' }}>by {p.seller_name ?? p.seller_id}</div>
                <button
                  onClick={() => handleAddToCart(p)}
                  disabled={cartAddingId === p.id}
                  style={{ marginTop: '8px', padding: '4px 16px', fontSize: '12px' }}
                >
                  {cartAddingId === p.id ? 'Adding...' : 'Add to Cart'}
                </button>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Order Status */}
      <div style={{ background: 'white', borderRadius: '8px', padding: '20px', border: '1px solid #e5e7eb' }}>
        <h2>Your Orders</h2>
        {orders.length === 0 ? (
          <p style={{ color: '#9ca3af' }}>No orders yet.</p>
        ) : (
          orders.map((o) => (
            <div key={o.id} style={{
              padding: '16px', borderRadius: '6px', border: '1px solid #d1d5db',
              marginBottom: '12px', background: '#f9fafb',
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ fontWeight: 'bold' }}>Purchase from {o.seller_name ?? o.seller_id}</span>
                <span style={{
                  padding: '2px 8px', borderRadius: '4px', fontSize: '12px',
                  background: o.status === 'delivered' ? '#d1fae5' : o.status === 'picked' ? '#fed7aa' : o.status === 'confirmed' ? '#bfdbfe' : '#fef3c7',
                  color: o.status === 'delivered' ? '#059669' : o.status === 'picked' ? '#c2410c' : o.status === 'confirmed' ? '#2563eb' : '#d97706',
                }}>
                  {o.status}
                </span>
              </div>
              <div style={{ marginTop: '8px', fontSize: '14px', color: '#6b7280' }}>
                {o.items.map((item, i) => (
                  <div key={i}>{item.quantity}x {item.product_name}</div>
                ))}
              </div>
              <div style={{ marginTop: '8px', fontWeight: 'bold' }}>
                Total: {formatPrice(o.total_amount)}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
