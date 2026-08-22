import { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  addToCart,
  checkout,
  listBuyerOrders,
  listBuyerProducts,
} from '../api/client';
import { CartSheet } from '../components/CartSheet';
import { RoleLayout } from '../components/RoleLayout';
import { Button } from '../components/ui/Button';
import { EmptyState } from '../components/ui/EmptyState';
import { FeedbackBanner } from '../components/ui/FeedbackBanner';
import { StatusBadge, type StatusTone } from '../components/ui/StatusBadge';
import { useEvents } from '../context/EventContext';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { cartItemCount } from '../lib/cart';
import {
  createFeedback,
  expireFeedback,
  type ActionFeedback,
} from '../lib/feedback';
import { isBuyerOrderEvent } from '../lib/orderEvents';

interface Product {
  id: string;
  name: string;
  price: number;
  quantity: number;
  seller_id: string;
  seller_name: string;
}

interface CartItem {
  product: Product;
  quantity: number;
}

interface OrderItem {
  product_id: string;
  product_name: string;
  quantity: number;
  unit_price: number;
}

interface Order {
  id: string;
  buyer_id: string;
  buyer_name: string;
  seller_id: string;
  seller_name: string;
  items: OrderItem[];
  total_amount: number;
  shipping_address: string;
  status: string;
  created_at: string;
}

function formatPrice(price: number): string {
  return `Rp ${price.toLocaleString('id-ID')}`;
}

function statusTone(status: string): StatusTone {
  if (status === 'delivered') return 'success';
  if (status === 'picked') return 'warning';
  if (status === 'confirmed') return 'info';
  if (status === 'cancelled') return 'error';
  return 'neutral';
}

function statusLabel(status: string): string {
  return status.charAt(0).toUpperCase() + status.slice(1);
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
  const [showCheckout, setShowCheckout] = useState(false);
  const [checkingOut, setCheckingOut] = useState(false);
  const [cartAddingId, setCartAddingId] = useState<string | null>(null);
  const [feedback, setFeedback] = useState<ActionFeedback | null>(null);
  const cartTriggerRef = useRef<HTMLButtonElement>(null);

  const loadProducts = useCallback(async () => {
    if (!token) return;
    const response = await listBuyerProducts(token);
    if (response.ok) setProducts(await response.json());
  }, [token]);

  const loadOrders = useCallback(async () => {
    if (!token) return;
    const response = await listBuyerOrders(token);
    if (response.ok) setOrders(await response.json());
  }, [token]);

  useEffect(() => {
    loadProducts();
    loadOrders();
  }, [loadProducts, loadOrders]);

  useEffect(() => {
    if (events[0]?.event_type === 'product.listed') loadProducts();
  }, [events, loadProducts]);

  useEffect(() => {
    if (events[0] && isBuyerOrderEvent(events[0], id)) loadOrders();
  }, [events, id, loadOrders]);

  useEffect(() => {
    if (!feedback) return undefined;
    const feedbackId = feedback.id;
    const timeout = window.setTimeout(() => {
      setFeedback((current) => expireFeedback(current, feedbackId));
    }, 4_000);
    return () => window.clearTimeout(timeout);
  }, [feedback]);

  const cartTotal = cart.reduce(
    (sum, item) => sum + item.product.price * item.quantity,
    0,
  );
  const itemCount = cartItemCount(cart);

  const handleAddToCart = async (product: Product) => {
    if (!token || cartAddingId) return;
    setCartAddingId(product.id);
    try {
      setCart((current) => {
        const existing = current.find((item) => item.product.id === product.id);
        if (!existing) return [...current, { product, quantity: 1 }];
        return current.map((item) => (
          item.product.id === product.id
            ? { ...item, quantity: item.quantity + 1 }
            : item
        ));
      });
      await addToCart(token, cartId, product.id, 1);
      setFeedback(createFeedback('success', `${product.name} added to cart.`));
    } catch {
      setFeedback(createFeedback('error', `Could not add ${product.name} to the cart.`));
    } finally {
      setCartAddingId(null);
    }
  };

  const handleCheckout = async () => {
    if (!token || cart.length === 0 || checkingOut || !shippingAddress.trim()) return;
    setCheckingOut(true);
    try {
      const items = cart.map((item) => ({
        product_id: item.product.id,
        quantity: item.quantity,
      }));
      const response = await checkout(token, cartId, items, shippingAddress);
      if (!response.ok) {
        const message = await response.text();
        setFeedback(createFeedback('error', message || 'Could not place order.'));
        return;
      }

      setCart([]);
      setCartId(crypto.randomUUID());
      setShippingAddress('');
      setShowCheckout(false);
      setFeedback(createFeedback('success', 'Order placed'));
      await loadOrders();
    } finally {
      setCheckingOut(false);
    }
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  return (
    <RoleLayout
      roleLabel="Shop"
      participantName={name ?? 'Participant'}
      pulseKey={events[0]?.event_id ?? 'initial'}
      onLogout={handleLogout}
    >
      <div className="buyer-view">
        <button
          ref={cartTriggerRef}
          type="button"
          className="buyer-cart-summary"
          disabled={cart.length === 0}
          aria-haspopup="dialog"
          onClick={() => setShowCheckout(true)}
        >
          <span>Review cart</span>
          <strong>{itemCount} {itemCount === 1 ? 'item' : 'items'} · {formatPrice(cartTotal)}</strong>
        </button>

        <div className="buyer-workspace" data-cart-open={showCheckout || undefined}>
          <div className="buyer-content">
            {feedback ? (
              <FeedbackBanner tone={feedback.tone}>{feedback.message}</FeedbackBanner>
            ) : null}

            <section className="buyer-section buyer-catalog" aria-labelledby="buyer-catalog-heading">
              <header className="buyer-section__header">
                <div>
                  <span className="buyer-section__eyebrow">Available now</span>
                  <h2 id="buyer-catalog-heading">Product catalog</h2>
                </div>
                <span className="buyer-section__count">{products.length} listings</span>
              </header>

              {products.length === 0 ? (
                <EmptyState
                  title="No products yet"
                  description="New seller listings will appear here automatically."
                />
              ) : (
                <div className="buyer-product-grid">
                  {products.map((product) => (
                    <article key={product.id} className="buyer-product-card">
                      <div className="buyer-product-card__seller">
                        Sold by {product.seller_name ?? product.seller_id}
                      </div>
                      <h3>{product.name}</h3>
                      <strong className="buyer-product-card__price">{formatPrice(product.price)}</strong>
                      <span className="buyer-product-card__stock">{product.quantity} in stock</span>
                      <Button
                        type="button"
                        loading={cartAddingId === product.id}
                        loadingLabel="Adding…"
                        aria-label={cartAddingId === product.id
                          ? `Adding ${product.name} to cart`
                          : `Add ${product.name} to cart`}
                        onClick={() => handleAddToCart(product)}
                      >
                        Add to cart
                      </Button>
                    </article>
                  ))}
                </div>
              )}
            </section>

            <section className="buyer-section buyer-orders" aria-labelledby="buyer-orders-heading">
              <header className="buyer-section__header">
                <div>
                  <span className="buyer-section__eyebrow">Lifecycle</span>
                  <h2 id="buyer-orders-heading">Recent orders</h2>
                </div>
                <span className="buyer-section__count">{orders.length} orders</span>
              </header>

              {orders.length === 0 ? (
                <EmptyState
                  title="No orders yet"
                  description="Placed orders and delivery updates will collect here."
                />
              ) : (
                <div className="buyer-order-list">
                  {orders.map((order) => (
                    <article key={order.id} className="buyer-order-card">
                      <header>
                        <div>
                          <span className="buyer-order-card__label">Purchase from</span>
                          <h3>{order.seller_name ?? order.seller_id}</h3>
                        </div>
                        <StatusBadge tone={statusTone(order.status)}>
                          {statusLabel(order.status)}
                        </StatusBadge>
                      </header>
                      <ul>
                        {order.items.map((item) => (
                          <li key={`${order.id}-${item.product_id}`}>
                            {item.quantity} × {item.product_name}
                          </li>
                        ))}
                      </ul>
                      <div className="buyer-order-card__total">
                        <span>Total</span>
                        <strong>{formatPrice(order.total_amount)}</strong>
                      </div>
                    </article>
                  ))}
                </div>
              )}
            </section>
          </div>

          <CartSheet
            open={showCheckout}
            items={cart}
            total={cartTotal}
            address={shippingAddress}
            submitting={checkingOut}
            returnFocusRef={cartTriggerRef}
            onAddressChange={setShippingAddress}
            onClose={() => setShowCheckout(false)}
            onPlaceOrder={handleCheckout}
          />
        </div>
      </div>
    </RoleLayout>
  );
}
