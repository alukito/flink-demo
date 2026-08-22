import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import { addToCart, checkout, listBuyerOrders, listBuyerProducts } from '../api/client';
import '../styles/base.css';
import Buyer from './Buyer';

vi.mock('../api/client', () => ({
  addToCart: vi.fn(),
  checkout: vi.fn(),
  listBuyerOrders: vi.fn(),
  listBuyerProducts: vi.fn(),
}));

vi.mock('../context/SessionContext', () => ({
  useSession: () => ({
    id: 'buyer-uuid',
    name: 'Ayu',
    token: 'buyer-token',
    role: 'buyer',
    setSession: vi.fn(),
    clearSession: vi.fn(),
  }),
}));

vi.mock('../context/EventContext', () => ({
  useEvents: () => ({ events: [], addEvent: vi.fn(), clearEvents: vi.fn() }),
}));

vi.mock('../hooks/useWebSocket', () => ({ useWebSocket: vi.fn() }));

const products = [
  {
    id: 'coffee',
    name: 'Flores coffee',
    price: 12_000,
    quantity: 8,
    seller_id: 'seller-uuid',
    seller_name: 'Bima',
  },
  {
    id: 'tea',
    name: 'Jasmine tea',
    price: 5_000,
    quantity: 4,
    seller_id: 'seller-two',
    seller_name: 'Citra',
  },
];

const orders = [
  {
    id: 'order-uuid',
    buyer_id: 'buyer-uuid',
    buyer_name: 'Ayu',
    seller_id: 'seller-uuid',
    seller_name: 'Bima',
    items: [{ product_id: 'coffee', product_name: 'Flores coffee', quantity: 2, unit_price: 12_000 }],
    total_amount: 24_000,
    shipping_address: 'Jl. Merdeka 8',
    status: 'confirmed',
    created_at: '2026-08-15T03:00:00Z',
  },
];

function jsonResponse(data: unknown): Response {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

const addToCartMock = vi.mocked(addToCart);
const checkoutMock = vi.mocked(checkout);
const listBuyerOrdersMock = vi.mocked(listBuyerOrders);
const listBuyerProductsMock = vi.mocked(listBuyerProducts);

function renderBuyer() {
  return render(<MemoryRouter><Buyer /></MemoryRouter>);
}

function stylesheetRules(): CSSStyleRule[] {
  const collect = (rules: CSSRuleList): CSSStyleRule[] => Array.from(rules).flatMap((rule) => {
    if (rule instanceof CSSStyleRule) return [rule];
    const nested = (rule as CSSRule & { cssRules?: CSSRuleList }).cssRules;
    return nested ? collect(nested) : [];
  });
  return Array.from(document.styleSheets).flatMap((sheet) => collect(sheet.cssRules));
}

describe('Buyer', () => {
  beforeEach(() => {
    addToCartMock.mockReset().mockImplementation(async () => jsonResponse({ cart_id: 'cart', items: [] }));
    checkoutMock.mockReset().mockImplementation(async () => jsonResponse({ id: 'order-uuid' }));
    listBuyerProductsMock.mockReset().mockImplementation(async () => jsonResponse(products));
    listBuyerOrdersMock.mockReset().mockImplementation(async () => jsonResponse(orders));
  });

  it('puts a two-column-capable catalog before recent orders with lifecycle badges', async () => {
    renderBuyer();

    await screen.findByText('Jasmine tea');
    const catalogHeading = screen.getByRole('heading', { name: /Product catalog/i });
    const ordersHeading = screen.getByRole('heading', { name: /Recent orders/i });
    expect(catalogHeading.compareDocumentPosition(ordersHeading) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(catalogHeading.closest('section')?.querySelector('.buyer-product-grid')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Flores coffee' }).closest('article')).toHaveClass('buyer-product-card');
    expect(screen.getByText('Confirmed')).toHaveClass('status-badge');
    expect(screen.getByText('Confirmed')).toHaveAttribute('data-tone', 'info');
  });

  it('keeps the phone cart rail safe-area aware and moves checkout in-flow on desktop', () => {
    renderBuyer();
    const rules = stylesheetRules();
    const stylesFor = (selector: string) => rules
      .filter((rule) => rule.selectorText === selector)
      .map((rule) => rule.style);

    expect(stylesFor('.buyer-cart-summary').some((style) => (
      style.position === 'fixed' && style.bottom.includes('safe-area-inset-bottom')
    ))).toBe(true);
    expect(stylesFor('.buyer-product-grid').some((style) => (
      style.gridTemplateColumns === 'repeat(2, minmax(0, 1fr))'
    ))).toBe(true);
    expect(stylesFor('.cart-sheet').some((style) => style.height === '100dvh')).toBe(true);
    expect(stylesFor('.cart-sheet-layer').some((style) => style.position === 'sticky')).toBe(true);
    expect(stylesFor('.buyer-workspace[data-cart-open]').some((style) => (
      style.gridTemplateColumns.includes('minmax(18rem, 22rem)')
    ))).toBe(true);
  });

  it('adds quantities to one cart and reports the total item count in the sticky review action', async () => {
    const user = userEvent.setup();
    renderBuyer();
    const addCoffee = await screen.findByRole('button', { name: /Add Flores coffee to cart/i });

    await user.click(addCoffee);
    await waitFor(() => expect(addToCartMock).toHaveBeenCalledTimes(1));
    await user.click(addCoffee);
    await waitFor(() => expect(addToCartMock).toHaveBeenCalledTimes(2));

    expect(addToCartMock.mock.calls[1]).toEqual([
      'buyer-token',
      addToCartMock.mock.calls[0][1],
      'coffee',
      1,
    ]);
    expect(screen.getByRole('status')).toHaveTextContent('Flores coffee added to cart.');
    expect(screen.getByRole('button', { name: /Review cart.*2 items.*Rp 24.000/i })).toHaveClass('buyer-cart-summary');
  });

  it('places the quantity-aware payload, resets checkout state, and reloads recent orders', async () => {
    const user = userEvent.setup();
    renderBuyer();
    const addCoffee = await screen.findByRole('button', { name: /Add Flores coffee to cart/i });

    await user.click(addCoffee);
    await waitFor(() => expect(addToCartMock).toHaveBeenCalledTimes(1));
    const firstCartId = addToCartMock.mock.calls[0][1];
    await user.click(screen.getByRole('button', { name: /Review cart.*1 item.*Rp 12.000/i }));
    await user.type(screen.getByRole('textbox', { name: 'Shipping address' }), 'Jl. Merdeka 8');
    await user.click(screen.getByRole('button', { name: 'Place order' }));

    await waitFor(() => expect(checkoutMock).toHaveBeenCalledWith(
      'buyer-token',
      firstCartId,
      [{ product_id: 'coffee', quantity: 1 }],
      'Jl. Merdeka 8',
    ));
    expect(await screen.findByRole('status')).toHaveTextContent('Order placed');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Review cart.*0 items.*Rp 0/i })).toBeDisabled();
    await waitFor(() => expect(listBuyerOrdersMock).toHaveBeenCalledTimes(2));

    await user.click(addCoffee);
    await waitFor(() => expect(addToCartMock).toHaveBeenCalledTimes(2));
    expect(addToCartMock.mock.calls[1][1]).not.toBe(firstCartId);
    await user.click(screen.getByRole('button', { name: /Review cart.*1 item.*Rp 12.000/i }));
    expect(screen.getByRole('textbox', { name: 'Shipping address' })).toHaveValue('');
  });
});
