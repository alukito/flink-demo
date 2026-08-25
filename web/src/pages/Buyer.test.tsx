import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
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

let buyerEvents: Array<{
  event_id: string;
  event_type: string;
  actor_id: string;
  actor_role: string;
  timestamp: string;
  payload: Record<string, unknown>;
}> = [];

vi.mock('../context/EventContext', () => ({
  useEvents: () => ({ events: buyerEvents, addEvent: vi.fn(), clearEvents: vi.fn() }),
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

function errorResponse(message = ''): Response {
  return new Response(message, {
    status: 503,
    statusText: 'Service Unavailable',
    headers: { 'Content-Type': 'text/plain' },
  });
}

function deferredResponse() {
  let resolve!: (response: Response) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<Response>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function deferredJsonResponse() {
  let resolve!: (data: unknown) => void;
  const body = new Promise<unknown>((resolvePromise) => {
    resolve = resolvePromise;
  });
  const json = vi.fn(() => body);
  return {
    response: { ok: true, json } as unknown as Response,
    json,
    resolve,
  };
}

function buyerEvent(eventId: string, eventType: string) {
  return {
    event_id: eventId,
    event_type: eventType,
    actor_id: 'buyer-uuid',
    actor_role: 'buyer',
    timestamp: '2026-08-15T07:00:00Z',
    payload: { buyer_id: 'buyer-uuid', order_id: 'order-uuid' },
  };
}

const addToCartMock = vi.mocked(addToCart);
const checkoutMock = vi.mocked(checkout);
const listBuyerOrdersMock = vi.mocked(listBuyerOrders);
const listBuyerProductsMock = vi.mocked(listBuyerProducts);

function renderBuyer() {
  return render(<MemoryRouter><Buyer /></MemoryRouter>);
}

function mockMatchMedia(matches: boolean) {
  vi.stubGlobal('matchMedia', vi.fn((query: string): MediaQueryList => ({
    matches,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(() => true),
  })));
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
    buyerEvents = [];
    mockMatchMedia(false);
    addToCartMock.mockReset().mockImplementation(async () => jsonResponse({ cart_id: 'cart', items: [] }));
    checkoutMock.mockReset().mockImplementation(async () => jsonResponse({ id: 'order-uuid' }));
    listBuyerProductsMock.mockReset().mockImplementation(async () => jsonResponse(products));
    listBuyerOrdersMock.mockReset().mockImplementation(async () => jsonResponse(orders));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
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

  it('marks the mobile Buyer background inert and removes inert on ordinary close', async () => {
    const user = userEvent.setup();
    const { container } = renderBuyer();
    const addCoffee = await screen.findByRole('button', { name: /Add Flores coffee to cart/i });

    await user.click(addCoffee);
    await waitFor(() => expect(addToCartMock).toHaveBeenCalledTimes(1));
    const reviewCart = screen.getByRole('button', { name: /Review cart.*1 item/i });
    await user.click(reviewCart);

    const buyerContent = container.querySelector('.buyer-content');
    const roleHeader = container.querySelector('.role-header');
    expect(buyerContent).toHaveAttribute('inert');
    expect(reviewCart).toHaveAttribute('inert');
    expect(roleHeader).toHaveAttribute('inert');

    await user.click(screen.getByRole('button', { name: 'Close cart' }));
    expect(buyerContent).not.toHaveAttribute('inert');
    expect(reviewCart).not.toHaveAttribute('inert');
    expect(roleHeader).not.toHaveAttribute('inert');
    expect(reviewCart).toHaveFocus();
  });

  it('keeps programmatic focus inside the mobile sheet', async () => {
    const user = userEvent.setup();
    renderBuyer();
    const addCoffee = await screen.findByRole('button', { name: /Add Flores coffee to cart/i });

    await user.click(addCoffee);
    await waitFor(() => expect(addToCartMock).toHaveBeenCalledTimes(1));
    await user.click(screen.getByRole('button', { name: /Review cart.*1 item/i }));
    const closeCart = screen.getByRole('button', { name: 'Close cart' });
    expect(closeCart).toHaveFocus();

    addCoffee.focus();
    expect(closeCart).toHaveFocus();
  });

  it('keeps the desktop checkout panel and Buyer workspace simultaneously interactive', async () => {
    mockMatchMedia(true);
    const user = userEvent.setup();
    const { container } = renderBuyer();
    const addCoffee = await screen.findByRole('button', { name: /Add Flores coffee to cart/i });

    await user.click(addCoffee);
    await waitFor(() => expect(addToCartMock).toHaveBeenCalledTimes(1));
    await user.click(screen.getByRole('button', { name: /Review cart.*1 item/i }));
    expect(await screen.findByRole('dialog', { name: 'Review your cart' })).not.toHaveAttribute('aria-modal');
    expect(container.querySelector('.buyer-content')).not.toHaveAttribute('inert');

    addCoffee.focus();
    expect(addCoffee).toHaveFocus();
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

  it('removes an unavailable item from the cart during checkout', async () => {
    const user = userEvent.setup();
    renderBuyer();

    await user.click(await screen.findByRole('button', { name: /Add Flores coffee to cart/i }));
    await user.click(screen.getByRole('button', { name: /Review cart.*1 item/i }));
    await user.click(screen.getByRole('button', { name: /Remove Flores coffee from cart/i }));

    expect(screen.getByRole('button', { name: /Review cart.*0 items.*Rp 0/i })).toBeDisabled();
    expect(screen.queryByText('1 × Flores coffee')).not.toBeInTheDocument();
  });

  it('places the quantity-aware payload, resets checkout state, focuses recent orders, and reloads them', async () => {
    const user = userEvent.setup();
    listBuyerProductsMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(products))
      .mockImplementationOnce(async () => jsonResponse([{ ...products[0], quantity: 0 }, products[1]]));
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
    const emptyCartReview = screen.getByRole('button', { name: /Review cart.*0 items.*Rp 0/i });
    expect(emptyCartReview).toBeDisabled();
    expect(screen.getByRole('heading', { name: 'Recent orders' }).closest('section')).toHaveFocus();
    expect(emptyCartReview).not.toHaveFocus();
    await waitFor(() => expect(listBuyerOrdersMock).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(listBuyerProductsMock).toHaveBeenCalledTimes(2));
    expect(screen.getByText('0 in stock')).toBeVisible();

    await user.click(addCoffee);
    await waitFor(() => expect(addToCartMock).toHaveBeenCalledTimes(2));
    expect(addToCartMock.mock.calls[1][1]).not.toBe(firstCartId);
    await user.click(screen.getByRole('button', { name: /Review cart.*1 item.*Rp 12.000/i }));
    expect(screen.getByRole('textbox', { name: 'Shipping address' })).toHaveValue('');
  });

  it.each([
    ['a non-OK response body', async () => errorResponse('Cart inventory changed.'), 'Cart inventory changed.'],
    ['an empty non-OK response', async () => errorResponse(), 'Could not add Flores coffee to the cart.'],
    ['a transport rejection', async () => { throw new Error('network unavailable'); }, 'Could not add Flores coffee to the cart.'],
  ])('does not mutate the cart or report success after %s', async (_caseName, responseFactory, expectedError) => {
    addToCartMock.mockImplementationOnce(responseFactory);
    const user = userEvent.setup();
    renderBuyer();

    await user.click(await screen.findByRole('button', { name: /Add Flores coffee to cart/i }));

    expect(await screen.findByRole('alert')).toHaveTextContent(expectedError);
    expect(screen.getByRole('button', { name: /Review cart.*0 items.*Rp 0/i })).toBeDisabled();
    expect(screen.queryByText('Flores coffee added to cart.')).not.toBeInTheDocument();
  });

  it.each([
    ['a non-OK response body', async () => errorResponse('Checkout window closed.'), 'Checkout window closed.'],
    ['an empty non-OK response', async () => errorResponse(), 'Could not place order.'],
    ['a transport rejection', async () => { throw new Error('network unavailable'); }, 'Could not place order.'],
  ])('keeps checkout state and reports %s', async (_caseName, responseFactory, expectedError) => {
    checkoutMock.mockImplementationOnce(responseFactory);
    const user = userEvent.setup();
    renderBuyer();

    await user.click(await screen.findByRole('button', { name: /Add Flores coffee to cart/i }));
    await user.click(screen.getByRole('button', { name: /Review cart.*1 item/i }));
    await user.type(screen.getByRole('textbox', { name: 'Shipping address' }), 'Jl. Merdeka 8');
    await user.click(screen.getByRole('button', { name: 'Place order' }));

    const dialog = screen.getByRole('dialog', { name: 'Review your cart' });
    expect(await within(dialog).findByRole('alert')).toHaveTextContent(expectedError);
    expect(screen.getByRole('textbox', { name: 'Shipping address' })).toHaveValue('Jl. Merdeka 8');
    expect(screen.getByRole('button', { name: /Review cart.*1 item/i })).toBeInTheDocument();
    expect(screen.queryByText('Order placed')).not.toBeInTheDocument();
  });

  it('retains products and orders while surfacing persistent refresh failures', async () => {
    listBuyerProductsMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(products))
      .mockImplementationOnce(async () => errorResponse('catalog unavailable'));
    listBuyerOrdersMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(orders))
      .mockImplementationOnce(async () => errorResponse('orders unavailable'));
    const view = renderBuyer();

    await screen.findByRole('heading', { name: 'Flores coffee' });
    buyerEvents = [buyerEvent('product-refresh', 'product.listed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    await waitFor(() => expect(listBuyerProductsMock).toHaveBeenCalledTimes(2));

    buyerEvents = [buyerEvent('order-refresh', 'order.confirmed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    await waitFor(() => expect(listBuyerOrdersMock).toHaveBeenCalledTimes(2));

    const alerts = await screen.findAllByRole('alert');
    expect(alerts.map((alert) => alert.textContent)).toEqual(expect.arrayContaining([
      'Products could not be refreshed. Existing products are still shown.',
      'Orders could not be refreshed. Existing orders are still shown.',
    ]));
    expect(screen.getByRole('heading', { name: 'Flores coffee' })).toBeVisible();
    expect(screen.getByText('Confirmed')).toBeVisible();
  });

  it('keeps the newest product success when an older refresh rejects', async () => {
    const olderRefresh = deferredResponse();
    const newestProducts = [{ ...products[0], id: 'newest-coffee', name: 'Newest coffee' }];
    listBuyerProductsMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(products))
      .mockReturnValueOnce(olderRefresh.promise)
      .mockImplementationOnce(async () => jsonResponse(newestProducts));
    const view = renderBuyer();

    await screen.findByRole('heading', { name: 'Flores coffee' });
    buyerEvents = [buyerEvent('product-refresh-old', 'product.listed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    await waitFor(() => expect(listBuyerProductsMock).toHaveBeenCalledTimes(2));
    buyerEvents = [buyerEvent('product-refresh-new', 'product.listed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    expect(await screen.findByRole('heading', { name: 'Newest coffee' })).toBeVisible();

    await act(async () => olderRefresh.reject(new Error('late catalog failure')));
    expect(screen.getByRole('heading', { name: 'Newest coffee' })).toBeVisible();
    expect(screen.queryByText('Products could not be refreshed. Existing products are still shown.')).not.toBeInTheDocument();
  });

  it('keeps the current product failure when an older refresh succeeds', async () => {
    const olderRefresh = deferredJsonResponse();
    const olderProducts = [{ ...products[0], id: 'older-coffee', name: 'Older coffee' }];
    listBuyerProductsMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(products))
      .mockImplementationOnce(async () => olderRefresh.response)
      .mockImplementationOnce(async () => errorResponse('current failure'));
    const view = renderBuyer();

    await screen.findByRole('heading', { name: 'Flores coffee' });
    buyerEvents = [buyerEvent('product-refresh-old', 'product.listed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    await waitFor(() => expect(olderRefresh.json).toHaveBeenCalledTimes(1));
    buyerEvents = [buyerEvent('product-refresh-new', 'product.listed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    expect(await screen.findByText('Products could not be refreshed. Existing products are still shown.')).toBeVisible();

    await act(async () => olderRefresh.resolve(olderProducts));
    expect(screen.getByText('Products could not be refreshed. Existing products are still shown.')).toBeVisible();
    expect(screen.queryByRole('heading', { name: 'Older coffee' })).not.toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Flores coffee' })).toBeVisible();
  });

  it('keeps the newest order success when an older refresh rejects', async () => {
    const olderRefresh = deferredResponse();
    const newestOrders = [{ ...orders[0], id: 'newest-order', seller_name: 'Newest seller' }];
    listBuyerOrdersMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(orders))
      .mockReturnValueOnce(olderRefresh.promise)
      .mockImplementationOnce(async () => jsonResponse(newestOrders));
    const view = renderBuyer();

    await screen.findByText('Confirmed');
    buyerEvents = [buyerEvent('order-refresh-old', 'order.confirmed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    await waitFor(() => expect(listBuyerOrdersMock).toHaveBeenCalledTimes(2));
    buyerEvents = [buyerEvent('order-refresh-new', 'order.confirmed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    expect(await screen.findByRole('heading', { name: 'Newest seller' })).toBeVisible();

    await act(async () => olderRefresh.reject(new Error('late order failure')));
    expect(screen.getByRole('heading', { name: 'Newest seller' })).toBeVisible();
    expect(screen.queryByText('Orders could not be refreshed. Existing orders are still shown.')).not.toBeInTheDocument();
  });

  it('keeps the current order failure when an older refresh succeeds', async () => {
    const olderRefresh = deferredJsonResponse();
    const olderOrders = [{ ...orders[0], id: 'older-order', seller_name: 'Older seller' }];
    listBuyerOrdersMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(orders))
      .mockImplementationOnce(async () => olderRefresh.response)
      .mockImplementationOnce(async () => errorResponse('current failure'));
    const view = renderBuyer();

    await screen.findByText('Confirmed');
    buyerEvents = [buyerEvent('order-refresh-old', 'order.confirmed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    await waitFor(() => expect(olderRefresh.json).toHaveBeenCalledTimes(1));
    buyerEvents = [buyerEvent('order-refresh-new', 'order.confirmed')];
    view.rerender(<MemoryRouter><Buyer /></MemoryRouter>);
    expect(await screen.findByText('Orders could not be refreshed. Existing orders are still shown.')).toBeVisible();

    await act(async () => olderRefresh.resolve(olderOrders));
    expect(screen.getByText('Orders could not be refreshed. Existing orders are still shown.')).toBeVisible();
    expect(screen.queryByRole('heading', { name: 'Older seller' })).not.toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Bima' })).toBeVisible();
  });

  it('expires success feedback but keeps an action error until a later action replaces it', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    try {
      renderBuyer();
      const addCoffee = await screen.findByRole('button', { name: /Add Flores coffee to cart/i });

      await user.click(addCoffee);
      expect(await screen.findByRole('status')).toHaveTextContent('Flores coffee added to cart.');
      await act(async () => vi.advanceTimersByTime(4_001));
      expect(screen.queryByText('Flores coffee added to cart.')).not.toBeInTheDocument();

      addToCartMock.mockRejectedValueOnce(new Error('network unavailable'));
      await user.click(addCoffee);
      expect(await screen.findByRole('alert')).toHaveTextContent('Could not add Flores coffee to the cart.');
      await act(async () => vi.advanceTimersByTime(4_001));
      expect(screen.getByRole('alert')).toHaveTextContent('Could not add Flores coffee to the cart.');
    } finally {
      vi.useRealTimers();
    }
  });
});
