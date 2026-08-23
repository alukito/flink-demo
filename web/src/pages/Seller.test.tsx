import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import { addProduct, confirmOrder, listSellerOrders, listSellerProducts } from '../api/client';
import '../styles/base.css';
import Seller from './Seller';

vi.mock('../api/client', () => ({
  addProduct: vi.fn(),
  confirmOrder: vi.fn(),
  listSellerOrders: vi.fn(),
  listSellerProducts: vi.fn(),
}));

vi.mock('../context/SessionContext', () => ({
  useSession: () => ({
    id: 'seller-uuid',
    name: 'Bima',
    token: 'seller-token',
    role: 'seller',
    setSession: vi.fn(),
    clearSession: vi.fn(),
  }),
}));

let sellerEvents: Array<{
  event_id: string;
  event_type: string;
  actor_id: string;
  actor_name: string;
  actor_role: string;
  timestamp: string;
  payload: Record<string, unknown>;
}> = [];

vi.mock('../context/EventContext', () => ({
  useEvents: () => ({ events: sellerEvents, addEvent: vi.fn(), clearEvents: vi.fn() }),
}));

vi.mock('../hooks/useWebSocket', () => ({ useWebSocket: vi.fn() }));

const products = [
  {
    id: 'coffee',
    name: 'Flores coffee',
    price: 12_000,
    quantity: 8,
    seller_id: 'seller-uuid',
  },
];

const orders = [
  {
    id: 'checkout-order',
    buyer_id: 'buyer-one',
    buyer_name: 'Ayu',
    seller_id: 'seller-uuid',
    seller_name: 'Bima',
    items: [{ product_id: 'coffee', product_name: 'Flores coffee', quantity: 2, unit_price: 12_000 }],
    total_amount: 24_000,
    shipping_address: 'Jl. Merdeka 8',
    status: 'checkout',
    created_at: '2026-08-15T03:00:00Z',
  },
  {
    id: 'confirmed-order',
    buyer_id: 'buyer-two',
    buyer_name: 'Citra',
    seller_id: 'seller-uuid',
    seller_name: 'Bima',
    items: [{ product_id: 'coffee', product_name: 'Flores coffee', quantity: 1, unit_price: 12_000 }],
    total_amount: 12_000,
    shipping_address: 'Jl. Mawar 3',
    status: 'confirmed',
    created_at: '2026-08-15T04:00:00Z',
  },
  {
    id: 'picked-order',
    buyer_id: 'buyer-three',
    buyer_name: 'Dewi',
    seller_id: 'seller-uuid',
    seller_name: 'Bima',
    items: [{ product_id: 'coffee', product_name: 'Flores coffee', quantity: 1, unit_price: 12_000 }],
    total_amount: 12_000,
    shipping_address: 'Jl. Melati 4',
    status: 'picked',
    created_at: '2026-08-15T05:00:00Z',
  },
  {
    id: 'delivered-order',
    buyer_id: 'buyer-four',
    buyer_name: 'Eka',
    seller_id: 'seller-uuid',
    seller_name: 'Bima',
    items: [{ product_id: 'coffee', product_name: 'Flores coffee', quantity: 1, unit_price: 12_000 }],
    total_amount: 12_000,
    shipping_address: 'Jl. Anggrek 5',
    status: 'delivered',
    created_at: '2026-08-15T06:00:00Z',
  },
];

function jsonResponse(data: unknown): Response {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

function errorResponse(message: string): Response {
  return new Response(message, {
    status: 503,
    statusText: 'Service Unavailable',
    headers: { 'Content-Type': 'text/plain' },
  });
}

function sellerOrderEvent(eventId: string) {
  return {
    event_id: eventId,
    event_type: 'order.confirmed',
    actor_id: 'buyer-uuid',
    actor_name: 'Ayu',
    actor_role: 'buyer',
    timestamp: '2026-08-15T07:00:00Z',
    payload: { seller_id: 'seller-uuid', buyer_id: 'buyer-uuid', order_id: 'checkout-order' },
  };
}

function deferredResponse() {
  let resolve!: (response: Response) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<Response>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

const addProductMock = vi.mocked(addProduct);
const confirmOrderMock = vi.mocked(confirmOrder);
const listSellerOrdersMock = vi.mocked(listSellerOrders);
const listSellerProductsMock = vi.mocked(listSellerProducts);

function renderSeller() {
  return render(<MemoryRouter><Seller /></MemoryRouter>);
}

function stylesheetRules(): CSSStyleRule[] {
  const collect = (rules: CSSRuleList): CSSStyleRule[] => Array.from(rules).flatMap((rule) => {
    if (rule instanceof CSSStyleRule) return [rule];
    const nested = (rule as CSSRule & { cssRules?: CSSRuleList }).cssRules;
    return nested ? collect(nested) : [];
  });
  return Array.from(document.styleSheets).flatMap((sheet) => collect(sheet.cssRules));
}

describe('Seller', () => {
  beforeEach(() => {
    sellerEvents = [];
    addProductMock.mockReset().mockImplementation(async () => jsonResponse({ id: 'new-product' }));
    confirmOrderMock.mockReset().mockImplementation(async () => jsonResponse({ id: 'checkout-order', status: 'confirmed' }));
    listSellerOrdersMock.mockReset().mockImplementation(async () => jsonResponse(orders));
    listSellerProductsMock.mockReset().mockImplementation(async () => jsonResponse(products));
  });

  it('leads with confirmation work, keeps every lifecycle order visible, and uses approved action names', async () => {
    renderSeller();

    await screen.findByRole('heading', { name: 'Flores coffee' });
    expect(screen.getByRole('heading', { name: 'Add a product' })).toBeVisible();
    expect(screen.getByRole('button', { name: 'Add product' })).toBeEnabled();
    expect(screen.queryByRole('button', { name: 'List product' })).not.toBeInTheDocument();

    const ordersHeading = screen.getByRole('heading', { name: 'Orders to confirm' });
    const productsHeading = screen.getByRole('heading', { name: 'Your products' });
    expect(ordersHeading).toBeVisible();
    expect(productsHeading).toBeVisible();
    expect(ordersHeading.compareDocumentPosition(productsHeading) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    expect(screen.getAllByRole('button', { name: 'Confirm order' })).toHaveLength(1);
    expect(screen.getByText('Confirmed')).toHaveClass('status-badge');
    expect(screen.getByText('Picked')).toHaveClass('status-badge');
    expect(screen.getByText('Delivered')).toHaveClass('status-badge');
    expect(screen.getByText('Citra').closest('article')).not.toContainElement(screen.getByRole('button', { name: 'Confirm order' }));
  });

  it('focuses the first invalid product field and explains how to correct it', async () => {
    const user = userEvent.setup();
    renderSeller();

    await user.click(screen.getByRole('button', { name: 'Add product' }));
    expect(screen.getByRole('textbox', { name: 'Product name' })).toHaveFocus();
    expect(screen.getByRole('alert')).toHaveTextContent('Enter a product name.');

    await user.type(screen.getByRole('textbox', { name: 'Product name' }), 'Flores cocoa');
    await user.click(screen.getByRole('button', { name: 'Add product' }));
    expect(screen.getByRole('spinbutton', { name: 'Price in rupiah' })).toHaveFocus();
    expect(screen.getByRole('alert')).toHaveTextContent('Enter a price greater than zero.');
  });

  it('keeps the Add product action named through loading and success while preserving the API payload', async () => {
    const pending = deferredResponse();
    addProductMock.mockReturnValueOnce(pending.promise);
    const user = userEvent.setup();
    renderSeller();

    await user.type(screen.getByRole('textbox', { name: 'Product name' }), 'Flores cocoa');
    await user.type(screen.getByRole('spinbutton', { name: 'Price in rupiah' }), '18000');
    await user.type(screen.getByRole('spinbutton', { name: 'Stock quantity' }), '6');
    await user.click(screen.getByRole('button', { name: 'Add product' }));

    expect(screen.getByRole('button', { name: 'Adding…' })).toBeDisabled();
    expect(addProductMock).toHaveBeenCalledWith('seller-token', 'Flores cocoa', 18_000, 6);

    await act(async () => pending.resolve(jsonResponse({ id: 'cocoa' })));
    expect(await screen.findByRole('status')).toHaveTextContent('Product added');
    expect(screen.getByRole('button', { name: 'Add product' })).toBeEnabled();
    await waitFor(() => expect(listSellerProductsMock).toHaveBeenCalledTimes(2));
  });

  it('keeps the Confirm order action named through loading and success', async () => {
    const pending = deferredResponse();
    confirmOrderMock.mockReturnValueOnce(pending.promise);
    const user = userEvent.setup();
    renderSeller();

    await user.click(await screen.findByRole('button', { name: 'Confirm order' }));
    expect(screen.getByRole('button', { name: 'Confirming…' })).toBeDisabled();
    expect(confirmOrderMock).toHaveBeenCalledWith('seller-token', 'checkout-order');

    await act(async () => pending.resolve(jsonResponse({ id: 'checkout-order', status: 'confirmed' })));
    expect(await screen.findByRole('status')).toHaveTextContent('Order confirmed');
    await waitFor(() => expect(listSellerOrdersMock).toHaveBeenCalledTimes(2));
  });

  it('reports transport failures inline without clearing visible products or orders', async () => {
    addProductMock.mockRejectedValueOnce(new Error('network unavailable'));
    confirmOrderMock.mockRejectedValueOnce(new Error('network unavailable'));
    const user = userEvent.setup();
    renderSeller();

    await screen.findByRole('heading', { name: 'Flores coffee' });
    await user.type(screen.getByRole('textbox', { name: 'Product name' }), 'Flores cocoa');
    await user.type(screen.getByRole('spinbutton', { name: 'Price in rupiah' }), '18000');
    await user.click(screen.getByRole('button', { name: 'Add product' }));

    expect(await screen.findByRole('alert')).toHaveTextContent('Product could not be added. Try again.');
    expect(screen.getByRole('heading', { name: 'Flores coffee' })).toBeVisible();
    expect(screen.getByText('Ayu')).toBeVisible();

    await user.click(screen.getByRole('button', { name: 'Confirm order' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Order could not be confirmed. Try again.');
    expect(screen.getByRole('heading', { name: 'Flores coffee' })).toBeVisible();
    expect(screen.getByText('Ayu')).toBeVisible();
  });

  it('keeps products visible and reports a resolved non-OK product refresh', async () => {
    listSellerProductsMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(products))
      .mockImplementationOnce(async () => errorResponse('inventory unavailable'));
    const user = userEvent.setup();
    renderSeller();

    await screen.findByRole('heading', { name: 'Flores coffee' });
    await user.type(screen.getByRole('textbox', { name: 'Product name' }), 'Flores cocoa');
    await user.type(screen.getByRole('spinbutton', { name: 'Price in rupiah' }), '18000');
    await user.click(screen.getByRole('button', { name: 'Add product' }));

    expect(await screen.findByRole('status')).toHaveTextContent('Product added');
    await waitFor(() => expect(listSellerProductsMock).toHaveBeenCalledTimes(2));
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Products could not be refreshed. Existing products are still shown.',
    );
    expect(screen.getByRole('heading', { name: 'Flores coffee' })).toBeVisible();
  });

  it('keeps orders visible and reports a resolved non-OK order refresh', async () => {
    listSellerOrdersMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(orders))
      .mockImplementationOnce(async () => errorResponse('orders unavailable'));
    const view = renderSeller();

    await screen.findByText('Ayu');
    sellerEvents = [sellerOrderEvent('refresh-orders')];
    view.rerender(<MemoryRouter><Seller /></MemoryRouter>);

    await waitFor(() => expect(listSellerOrdersMock).toHaveBeenCalledTimes(2));
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Orders could not be refreshed. Existing orders are still shown.',
    );
    expect(screen.getByText('Ayu')).toBeVisible();
  });

  it('ignores a stale order rejection after a newer refresh succeeds', async () => {
    const olderRefresh = deferredResponse();
    const newestOrders = [{
      ...orders[1],
      id: 'newest-confirmed-order',
      buyer_id: 'newest-buyer',
      buyer_name: 'Fajar',
      created_at: '2026-08-15T08:00:00Z',
    }];
    listSellerOrdersMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(orders))
      .mockReturnValueOnce(olderRefresh.promise)
      .mockImplementationOnce(async () => jsonResponse(newestOrders));
    const view = renderSeller();

    await screen.findByText('Ayu');
    sellerEvents = [sellerOrderEvent('older-refresh')];
    view.rerender(<MemoryRouter><Seller /></MemoryRouter>);
    await waitFor(() => expect(listSellerOrdersMock).toHaveBeenCalledTimes(2));

    sellerEvents = [sellerOrderEvent('newer-refresh')];
    view.rerender(<MemoryRouter><Seller /></MemoryRouter>);
    expect(await screen.findByText('Fajar')).toBeVisible();

    await act(async () => olderRefresh.reject(new Error('late network failure')));
    expect(screen.getByText('Fajar')).toBeVisible();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('ignores an older product success after a newer refresh publishes products', async () => {
    const olderRefresh = deferredResponse();
    const newestProducts = [{ ...products[0], id: 'newest-cocoa', name: 'Newest cocoa' }];
    const olderProducts = [{ ...products[0], id: 'older-beans', name: 'Older beans' }];
    listSellerProductsMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(products))
      .mockReturnValueOnce(olderRefresh.promise)
      .mockImplementationOnce(async () => jsonResponse(newestProducts));
    const user = userEvent.setup();
    renderSeller();

    await screen.findByRole('heading', { name: 'Flores coffee' });
    await user.type(screen.getByRole('textbox', { name: 'Product name' }), 'First listing');
    await user.type(screen.getByRole('spinbutton', { name: 'Price in rupiah' }), '18000');
    await user.click(screen.getByRole('button', { name: 'Add product' }));
    await waitFor(() => expect(listSellerProductsMock).toHaveBeenCalledTimes(2));

    await user.type(screen.getByRole('textbox', { name: 'Product name' }), 'Second listing');
    await user.type(screen.getByRole('spinbutton', { name: 'Price in rupiah' }), '19000');
    await user.click(screen.getByRole('button', { name: 'Add product' }));
    expect(await screen.findByRole('heading', { name: 'Newest cocoa' })).toBeVisible();

    await act(async () => olderRefresh.resolve(jsonResponse(olderProducts)));
    expect(screen.getByRole('heading', { name: 'Newest cocoa' })).toBeVisible();
    expect(screen.queryByRole('heading', { name: 'Older beans' })).not.toBeInTheDocument();
  });

  it('ignores an older product rejection after a newer refresh succeeds', async () => {
    const olderRefresh = deferredResponse();
    const newestProducts = [{ ...products[0], id: 'newest-cocoa', name: 'Newest cocoa' }];
    listSellerProductsMock
      .mockReset()
      .mockImplementationOnce(async () => jsonResponse(products))
      .mockReturnValueOnce(olderRefresh.promise)
      .mockImplementationOnce(async () => jsonResponse(newestProducts));
    const user = userEvent.setup();
    renderSeller();

    await screen.findByRole('heading', { name: 'Flores coffee' });
    await user.type(screen.getByRole('textbox', { name: 'Product name' }), 'First listing');
    await user.type(screen.getByRole('spinbutton', { name: 'Price in rupiah' }), '18000');
    await user.click(screen.getByRole('button', { name: 'Add product' }));
    await waitFor(() => expect(listSellerProductsMock).toHaveBeenCalledTimes(2));

    await user.type(screen.getByRole('textbox', { name: 'Product name' }), 'Second listing');
    await user.type(screen.getByRole('spinbutton', { name: 'Price in rupiah' }), '19000');
    await user.click(screen.getByRole('button', { name: 'Add product' }));
    expect(await screen.findByRole('heading', { name: 'Newest cocoa' })).toBeVisible();

    await act(async () => olderRefresh.reject(new Error('late inventory failure')));
    expect(screen.getByRole('heading', { name: 'Newest cocoa' })).toBeVisible();
    expect(screen.queryByText('Products could not be refreshed. Existing products are still shown.')).not.toBeInTheDocument();
  });

  it('uses phone-safe controls and switches to a desktop workbench at 960px', () => {
    renderSeller();
    const rules = stylesheetRules();
    const stylesFor = (selector: string) => rules
      .filter((rule) => rule.selectorText === selector)
      .map((rule) => rule.style);

    expect(stylesFor('.seller-product-form input').some((style) => style.width === '100%')).toBe(true);
    expect(stylesFor('.seller-form-pair').some((style) => style.gridTemplateColumns === 'repeat(2, minmax(0, 1fr))')).toBe(true);
    expect(stylesFor('.seller-workbench').some((style) => (
      style.gridTemplateColumns === 'minmax(18rem, 0.8fr) minmax(0, 1.2fr)'
    ))).toBe(true);
    expect(stylesFor('.seller-view').some((style) => style.overflowX === 'hidden')).toBe(true);
  });
});
