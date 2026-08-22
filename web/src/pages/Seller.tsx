import { useState, useEffect, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSession } from '../context/SessionContext';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents } from '../context/EventContext';
import {
  addProduct, listSellerProducts, listSellerOrders, confirmOrder,
} from '../api/client';
import { isSellerOrderEvent } from '../lib/orderEvents';
import { createFeedback, type ActionFeedback } from '../lib/feedback';
import { loadLatestSellerOrders } from '../lib/sellerRefresh';
import { ActionCard } from '../components/ActionCard';
import { RoleLayout } from '../components/RoleLayout';
import { Button } from '../components/ui/Button';
import { EmptyState } from '../components/ui/EmptyState';
import { FeedbackBanner } from '../components/ui/FeedbackBanner';
import { StatusBadge, type StatusTone } from '../components/ui/StatusBadge';

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

const statusPresentation: Record<string, { label: string; tone: StatusTone }> = {
  checkout: { label: 'Checkout', tone: 'warning' },
  confirmed: { label: 'Confirmed', tone: 'info' },
  picked: { label: 'Picked', tone: 'warning' },
  delivered: { label: 'Delivered', tone: 'success' },
};

function formatPrice(price: number): string {
  return 'Rp ' + price.toLocaleString('id-ID');
}

function orderStatus(status: string) {
  return statusPresentation[status] ?? {
    label: status.charAt(0).toUpperCase() + status.slice(1),
    tone: 'neutral' as const,
  };
}

export default function Seller() {
  const { id, name, token, clearSession } = useSession();
  const navigate = useNavigate();
  const { events, addEvent } = useEvents();
  useWebSocket(addEvent);

  const [products, setProducts] = useState<Product[]>([]);
  const [orders, setOrders] = useState<Order[]>([]);
  const [newProduct, setNewProduct] = useState({ name: '', price: '', quantity: '' });
  const [feedback, setFeedback] = useState<ActionFeedback | null>(null);
  const [adding, setAdding] = useState(false);
  const [confirmingId, setConfirmingId] = useState<string | null>(null);
  const nameInputRef = useRef<HTMLInputElement>(null);
  const priceInputRef = useRef<HTMLInputElement>(null);
  const latestOrdersGeneration = useRef(0);

  const loadProducts = useCallback(async () => {
    if (!token) return;
    try {
      const resp = await listSellerProducts(token);
      if (resp.ok) setProducts(await resp.json());
    } catch {
      setFeedback(createFeedback('error', 'Products could not be refreshed. Existing products are still shown.'));
    }
  }, [token]);

  const loadOrders = useCallback(async () => {
    const generation = ++latestOrdersGeneration.current;
    if (!token) return;
    try {
      await loadLatestSellerOrders({
        generation,
        getLatestGeneration: () => latestOrdersGeneration.current,
        listOrders: () => listSellerOrders(token),
        commit: setOrders,
      });
    } catch {
      setFeedback(createFeedback('error', 'Orders could not be refreshed. Existing orders are still shown.'));
    }
  }, [token]);

  useEffect(() => { void loadProducts(); void loadOrders(); }, [loadProducts, loadOrders]);

  useEffect(() => {
    if (events[0] && isSellerOrderEvent(events[0], id)) void loadOrders();
  }, [events, id, loadOrders]);

  const handleAddProduct = async () => {
    if (!token || adding) return;
    setFeedback(null);
    const price = parseInt(newProduct.price, 10);
    const quantity = parseInt(newProduct.quantity, 10);
    if (!newProduct.name) {
      setFeedback(createFeedback('error', 'Enter a product name.'));
      nameInputRef.current?.focus();
      return;
    }
    if (!price || price <= 0) {
      setFeedback(createFeedback('error', 'Enter a price greater than zero.'));
      priceInputRef.current?.focus();
      return;
    }
    setAdding(true);
    try {
      const resp = await addProduct(token, newProduct.name, price, quantity || 0);
      if (!resp.ok) {
        const message = (await resp.text()).trim();
        setFeedback(createFeedback('error', message || 'Product could not be added. Try again.'));
        return;
      }
      setNewProduct({ name: '', price: '', quantity: '' });
      setFeedback(createFeedback('success', 'Product added'));
      void loadProducts();
    } catch {
      setFeedback(createFeedback('error', 'Product could not be added. Try again.'));
    } finally {
      setAdding(false);
    }
  };

  const handleConfirmOrder = async (orderId: string) => {
    if (!token || confirmingId) return;
    setFeedback(null);
    setConfirmingId(orderId);
    try {
      const resp = await confirmOrder(token, orderId);
      if (!resp.ok) {
        const message = (await resp.text()).trim();
        setFeedback(createFeedback('error', message || 'Order could not be confirmed. Try again.'));
        return;
      }
      setFeedback(createFeedback('success', 'Order confirmed'));
      void loadOrders();
    } catch {
      setFeedback(createFeedback('error', 'Order could not be confirmed. Try again.'));
    } finally {
      setConfirmingId(null);
    }
  };

  const handleLogout = () => {
    clearSession();
    navigate('/');
  };

  const checkoutOrders = orders.filter((order) => order.status === 'checkout');
  const lifecycleOrders = orders.filter((order) => order.status !== 'checkout');

  const renderOrder = (order: Order) => {
    const status = orderStatus(order.status);
    const buyerName = order.buyer_name ?? order.buyer_id;

    return (
      <article className="seller-order-card" key={order.id}>
        <header>
          <div>
            <span className="seller-order-card__label">Order from</span>
            <h3>{buyerName}</h3>
          </div>
          <StatusBadge tone={status.tone}>{status.label}</StatusBadge>
        </header>
        <ul className="seller-order-card__items">
          {order.items.map((item, index) => (
            <li key={`${item.product_id}-${index}`}>
              <span>{item.quantity} × {item.product_name}</span>
              <strong>{formatPrice(item.unit_price)}</strong>
            </li>
          ))}
        </ul>
        <div className="seller-order-card__total">
          <span>Total</span>
          <strong>{formatPrice(order.total_amount)}</strong>
        </div>
        <p className="seller-order-card__address">
          <span>Ship to</span>
          {order.shipping_address}
        </p>
        {order.status === 'checkout' ? (
          <Button
            type="button"
            loading={confirmingId === order.id}
            loadingLabel="Confirming…"
            disabled={confirmingId !== null}
            onClick={() => void handleConfirmOrder(order.id)}
          >
            Confirm order
          </Button>
        ) : null}
      </article>
    );
  };

  return (
    <RoleLayout
      roleLabel="Sell"
      participantName={name ?? 'Participant'}
      pulseKey={events[0]?.event_id ?? 'initial'}
      onLogout={handleLogout}
    >
      <div className="seller-view">
        {feedback ? <FeedbackBanner tone={feedback.tone}>{feedback.message}</FeedbackBanner> : null}

        <div className="seller-workbench">
          <ActionCard
            className="seller-add-card"
            title="Add a product"
            description="Set the price and available stock for your marketplace listing."
          >
            <form
              className="seller-product-form"
              noValidate
              onSubmit={(event) => {
                event.preventDefault();
                void handleAddProduct();
              }}
            >
              <label className="seller-field">
                <span>Product name</span>
                <input
                  ref={nameInputRef}
                  value={newProduct.name}
                  onChange={(event) => setNewProduct({ ...newProduct, name: event.target.value })}
                  aria-invalid={feedback?.message === 'Enter a product name.' || undefined}
                  autoComplete="off"
                />
              </label>
              <div className="seller-form-pair">
                <label className="seller-field">
                  <span>Price in rupiah</span>
                  <input
                    ref={priceInputRef}
                    type="number"
                    min="1"
                    inputMode="numeric"
                    value={newProduct.price}
                    onChange={(event) => setNewProduct({ ...newProduct, price: event.target.value })}
                    aria-invalid={feedback?.message === 'Enter a price greater than zero.' || undefined}
                  />
                </label>
                <label className="seller-field">
                  <span>Stock quantity</span>
                  <input
                    type="number"
                    inputMode="numeric"
                    value={newProduct.quantity}
                    onChange={(event) => setNewProduct({ ...newProduct, quantity: event.target.value })}
                  />
                </label>
              </div>
              <Button type="submit" loading={adding} loadingLabel="Adding…">
                Add product
              </Button>
            </form>
          </ActionCard>

          <ActionCard
            className="seller-orders"
            title="Orders to confirm"
            description="Confirm new checkouts, then follow each order through delivery."
          >
            {checkoutOrders.length > 0 ? (
              <div className="seller-order-list seller-order-list--actionable">
                {checkoutOrders.map(renderOrder)}
              </div>
            ) : (
              <EmptyState title="No orders need confirmation" description="New checkouts will appear here." />
            )}

            {lifecycleOrders.length > 0 ? (
              <div className="seller-lifecycle">
                <h3>Order progress</h3>
                <div className="seller-order-list">
                  {lifecycleOrders.map(renderOrder)}
                </div>
              </div>
            ) : null}
          </ActionCard>

          <section className="seller-products" aria-labelledby="seller-products-title">
            <header className="seller-section-header">
              <div>
                <span className="seller-section-header__eyebrow">Inventory</span>
                <h2 id="seller-products-title">Your products</h2>
              </div>
              <span className="seller-section-header__count">{products.length} listed</span>
            </header>
            {products.length === 0 ? (
              <EmptyState title="No products yet" description="Add your first product to start selling." />
            ) : (
              <div className="seller-product-grid">
                {products.map((product) => (
                  <article className="seller-product-card" key={product.id}>
                    <span className="seller-product-card__stock">{product.quantity} in stock</span>
                    <h3>{product.name}</h3>
                    <strong>{formatPrice(product.price)}</strong>
                  </article>
                ))}
              </div>
            )}
          </section>
        </div>
      </div>
    </RoleLayout>
  );
}
