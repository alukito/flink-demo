import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import {
  deliverJob,
  listShipperDeliveries,
  listShipperJobs,
  pickJob,
} from '../api/client';
import type { Delivery, ShipperDeliveries } from '../lib/deliveries';
import '../styles/base.css';
import Shipper from './Shipper';

vi.mock('../api/client', () => ({
  deliverJob: vi.fn(),
  listShipperDeliveries: vi.fn(),
  listShipperJobs: vi.fn(),
  pickJob: vi.fn(),
}));

vi.mock('../context/SessionContext', () => ({
  useSession: () => ({
    id: 'shipper-uuid',
    name: 'Raka',
    token: 'shipper-token',
    role: 'shipper',
    setSession: vi.fn(),
    clearSession: vi.fn(),
  }),
}));

let shipperEvents: Array<{
  event_id: string;
  event_type: string;
  actor_id: string;
  actor_name: string;
  actor_role: string;
  timestamp: string;
  payload: Record<string, unknown>;
}> = [];

vi.mock('../context/EventContext', () => ({
  useEvents: () => ({ events: shipperEvents, addEvent: vi.fn(), clearEvents: vi.fn() }),
}));

vi.mock('../hooks/useWebSocket', () => ({ useWebSocket: vi.fn() }));

function delivery(overrides: Partial<Delivery> = {}): Delivery {
  return {
    id: 'delivery-one',
    buyer_id: 'buyer-one',
    buyer_name: 'Ayu',
    seller_id: 'seller-one',
    seller_name: 'Bima',
    items: [{ product_id: 'coffee', product_name: 'Flores coffee', quantity: 2, unit_price: 12_000 }],
    shipping_address: 'Jl. Merdeka 8, Jakarta',
    status: 'picked',
    created_at: '2026-08-15T02:00:00Z',
    picked_at: '2026-08-15T03:00:00Z',
    ready_at: '2000-01-01T00:00:00Z',
    ...overrides,
  };
}

const availableJob = delivery({
  id: 'available-job',
  buyer_id: 'buyer-job',
  buyer_name: 'Fajar',
  status: 'confirmed',
  picked_at: undefined,
  ready_at: undefined,
});

const activeReady = delivery();

const historyLatest = delivery({
  id: 'history-latest',
  buyer_id: 'buyer-history-one',
  buyer_name: 'Eka',
  status: 'delivered',
  picked_at: '2026-08-15T03:00:00Z',
  delivered_at: '2026-08-15T03:01:35Z',
});

const historyOlder = delivery({
  id: 'history-older',
  buyer_id: 'buyer-history-two',
  buyer_name: 'Dewi',
  shipping_address: 'Jl. Melati 4, Bandung',
  status: 'delivered',
  picked_at: '2026-08-15T01:00:00Z',
  delivered_at: '2026-08-15T01:00:42Z',
});

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

function deferredResponse() {
  let resolve!: (response: Response) => void;
  const promise = new Promise<Response>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function queueSnapshot(jobs: Delivery[], deliveries: ShipperDeliveries) {
  listShipperJobsMock.mockImplementationOnce(async () => jsonResponse(jobs));
  listShipperDeliveriesMock.mockImplementationOnce(async () => jsonResponse(deliveries));
}

function renderShipper() {
  return render(<MemoryRouter><Shipper /></MemoryRouter>);
}

function stylesheetRules(): CSSStyleRule[] {
  const collect = (rules: CSSRuleList): CSSStyleRule[] => Array.from(rules).flatMap((rule) => {
    if (rule instanceof CSSStyleRule) return [rule];
    const nested = (rule as CSSRule & { cssRules?: CSSRuleList }).cssRules;
    return nested ? collect(nested) : [];
  });
  return Array.from(document.styleSheets).flatMap((sheet) => collect(sheet.cssRules));
}

const deliverJobMock = vi.mocked(deliverJob);
const listShipperDeliveriesMock = vi.mocked(listShipperDeliveries);
const listShipperJobsMock = vi.mocked(listShipperJobs);
const pickJobMock = vi.mocked(pickJob);

describe('Shipper', () => {
  beforeEach(() => {
    shipperEvents = [];
    deliverJobMock.mockReset().mockImplementation(async () => jsonResponse({ status: 'delivered' }));
    listShipperDeliveriesMock.mockReset().mockImplementation(async () => jsonResponse({ active: [], history: [] }));
    listShipperJobsMock.mockReset().mockImplementation(async () => jsonResponse([]));
    pickJobMock.mockReset().mockImplementation(async () => jsonResponse({ status: 'picked' }));
  });

  it('leads with active work, then available jobs, then reload-safe completed history', async () => {
    queueSnapshot([availableJob], {
      active: [activeReady],
      history: [historyOlder, historyLatest],
    });
    renderShipper();

    const activeHeading = await screen.findByRole('heading', { name: 'Active delivery' });
    const jobsHeading = screen.getByRole('heading', { name: 'Available jobs' });
    const historyHeading = screen.getByRole('heading', { name: 'Completed deliveries' });
    expect(activeHeading.compareDocumentPosition(jobsHeading) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(jobsHeading.compareDocumentPosition(historyHeading) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    const historyCards = historyHeading.closest('section')?.querySelectorAll('article');
    expect(historyCards?.[0]).toHaveTextContent('Eka');
    expect(historyCards?.[1]).toHaveTextContent('Dewi');
    expect(historyCards?.[0]).toHaveTextContent('Jl. Merdeka 8, Jakarta');
    expect(historyCards?.[0]).toHaveTextContent('2 × Flores coffee');
    expect(historyCards?.[0]).toHaveTextContent('Picked');
    expect(historyCards?.[0]).toHaveTextContent('Delivered');
    expect(historyCards?.[0]).toHaveTextContent('1m 35s');
  });

  it('keeps invalid and early readiness disabled while enabling elapsed server readiness', async () => {
    queueSnapshot([], {
      active: [
        delivery({ id: 'invalid-ready', buyer_name: 'Ayu', ready_at: 'invalid' }),
        delivery({ id: 'waiting-ready', buyer_name: 'Citra', ready_at: '2099-01-01T00:00:00Z' }),
        delivery({ id: 'elapsed-ready', buyer_name: 'Dewi', ready_at: '2000-01-01T00:00:00Z' }),
      ],
      history: [],
    });
    renderShipper();

    const invalidCard = (await screen.findByRole('heading', { name: 'Ayu' })).closest('article');
    const waitingCard = screen.getByRole('heading', { name: 'Citra' }).closest('article');
    const readyCard = screen.getByRole('heading', { name: 'Dewi' }).closest('article');

    expect(within(invalidCard!).getByText('Readiness unavailable')).toBeVisible();
    expect(within(invalidCard!).queryByText('Ready in 0s')).not.toBeInTheDocument();
    expect(within(invalidCard!).getByRole('button', { name: 'Mark delivered' })).toBeDisabled();
    expect(within(waitingCard!).getByText(/Ready in \d+s/)).toBeVisible();
    expect(within(waitingCard!).getByRole('button', { name: 'Mark delivered' })).toBeDisabled();
    expect(within(readyCard!).getByText('Ready to deliver')).toBeVisible();
    expect(within(readyCard!).getByRole('button', { name: 'Mark delivered' })).toBeEnabled();
  });

  it('uses Pick up job through loading and reports Job picked up after success', async () => {
    const pending = deferredResponse();
    queueSnapshot([availableJob], { active: [], history: [] });
    queueSnapshot([], { active: [delivery({ id: availableJob.id, buyer_name: availableJob.buyer_name })], history: [] });
    pickJobMock.mockReturnValueOnce(pending.promise);
    const user = userEvent.setup();
    renderShipper();

    await user.click(await screen.findByRole('button', { name: 'Pick up job' }));
    expect(screen.getByRole('button', { name: 'Picking up…' })).toBeDisabled();

    await act(async () => pending.resolve(jsonResponse({ status: 'picked' })));
    expect((await screen.findByText('Job picked up')).closest('.feedback-banner')).toHaveAttribute('role', 'status');
    expect(await screen.findByRole('heading', { name: 'Fajar' })).toBeVisible();
  });

  it('uses Mark delivered through loading and reports Delivery completed after success', async () => {
    const pending = deferredResponse();
    queueSnapshot([], { active: [activeReady], history: [] });
    queueSnapshot([], { active: [], history: [historyLatest] });
    deliverJobMock.mockReturnValueOnce(pending.promise);
    const user = userEvent.setup();
    renderShipper();

    await user.click(await screen.findByRole('button', { name: 'Mark delivered' }));
    expect(screen.getByRole('button', { name: 'Delivering…' })).toBeDisabled();

    await act(async () => pending.resolve(jsonResponse({ status: 'delivered' })));
    expect((await screen.findByText('Delivery completed')).closest('.feedback-banner')).toHaveAttribute('role', 'status');
    expect(await screen.findByRole('heading', { name: 'Eka' })).toBeVisible();
  });

  it('retains existing cards and shows a persistent current refresh error', async () => {
    queueSnapshot([availableJob], { active: [activeReady], history: [historyLatest] });
    listShipperJobsMock.mockImplementationOnce(async () => errorResponse('jobs unavailable'));
    listShipperDeliveriesMock.mockImplementationOnce(async () => jsonResponse({ active: [], history: [] }));
    const view = renderShipper();

    await screen.findByRole('heading', { name: 'Fajar' });
    shipperEvents = [{
      event_id: 'queue-refresh',
      event_type: 'order.confirmed',
      actor_id: 'seller-one',
      actor_name: 'Bima',
      actor_role: 'seller',
      timestamp: '2026-08-15T04:00:00Z',
      payload: { order_id: 'available-job', seller_id: 'seller-one', buyer_id: 'buyer-job' },
    }];
    view.rerender(<MemoryRouter><Shipper /></MemoryRouter>);

    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to refresh delivery data');
    expect(screen.getByRole('heading', { name: 'Fajar' })).toBeVisible();
    expect(screen.getByRole('heading', { name: 'Ayu' })).toBeVisible();
    expect(screen.getByRole('heading', { name: 'Eka' })).toBeVisible();

    view.rerender(<MemoryRouter><Shipper /></MemoryRouter>);
    expect(screen.getByRole('alert')).toHaveTextContent('Unable to refresh delivery data');
  });

  it('uses phone-safe flow styles and forms a 960px dispatch workbench', async () => {
    queueSnapshot([availableJob], { active: [activeReady], history: [historyLatest] });
    renderShipper();
    await screen.findByRole('heading', { name: 'Active delivery' });

    const rules = stylesheetRules();
    const stylesFor = (selector: string) => rules
      .filter((rule) => rule.selectorText === selector)
      .map((rule) => rule.style);

    expect(stylesFor('.shipper-view').some((style) => style.overflowX === 'hidden')).toBe(true);
    expect(stylesFor('.shipper-workbench').some((style) => (
      style.gridTemplateColumns === 'minmax(0, 1.15fr) minmax(18rem, 0.85fr)'
    ))).toBe(true);
    expect(stylesFor('.shipper-card .button').some((style) => style.width === '100%')).toBe(true);
    expect(screen.getAllByRole('button').every((button) => getComputedStyle(button).minHeight === '44px')).toBe(true);
  });

  it('does not refresh this shipper for another UUID delivery event', async () => {
    queueSnapshot([], { active: [activeReady], history: [] });
    const view = renderShipper();
    await screen.findByRole('heading', { name: 'Ayu' });

    shipperEvents = [{
      event_id: 'other-delivery',
      event_type: 'shipment.delivered',
      actor_id: 'other-shipper',
      actor_name: 'Another shipper',
      actor_role: 'shipper',
      timestamp: '2026-08-15T04:00:00Z',
      payload: { shipper_id: 'other-shipper', order_id: 'other-order' },
    }];
    view.rerender(<MemoryRouter><Shipper /></MemoryRouter>);

    await waitFor(() => expect(listShipperJobsMock).toHaveBeenCalledTimes(1));
    expect(screen.getByRole('heading', { name: 'Ayu' })).toBeVisible();
  });
});
