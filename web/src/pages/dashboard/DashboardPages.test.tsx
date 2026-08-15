import { fireEvent, render, screen, within } from '@testing-library/react';
import { beforeEach, expect, test, vi } from 'vitest';
import { AlertCountChart } from '../../dashboard/AlertCountChart';
import { DeliveryDurationChart } from '../../dashboard/DeliveryDurationChart';
import { MetricBarChart } from '../../components/MetricBarChart';
import { useDashboard } from '../../dashboard/DashboardContext';
import { DashboardLivePage } from './DashboardLivePage';
import { DashboardPatternsPage } from './DashboardPatternsPage';
import { DashboardWindowsPage } from './DashboardWindowsPage';
import '../../styles/base.css';

vi.mock('../../dashboard/DashboardContext', () => ({
  useDashboard: vi.fn(),
}));

const now = new Date('2026-08-15T03:04:30.000Z');

function cssRule(selector: string): CSSStyleRule {
  const rule = Array.from(document.styleSheets)
    .flatMap((sheet) => Array.from(sheet.cssRules))
    .find((candidate): candidate is CSSStyleRule =>
      candidate instanceof CSSStyleRule && candidate.selectorText.replace(/\s+/g, ' ') === selector);
  if (!rule) throw new Error(`Missing CSS rule: ${selector}`);
  return rule;
}

beforeEach(() => {
  vi.mocked(useDashboard).mockReturnValue({
    alerts: [],
    clearAll: vi.fn(),
    connectionState: 'live',
    events: [{
      event_id: 'event-1',
      event_type: 'cart.item.added',
      actor_id: 'buyer-7',
      actor_name: 'Rani',
      actor_role: 'buyer',
      timestamp: '2026-08-15T03:02:00.000Z',
      payload: { product_id: 'product-1', quantity: 2 },
    }],
    groupedStats: {
      listings_count: [{ metric: 'listings_count', scope: 'window', window_end: '2026-08-15T03:05:00.000Z', value: 8, detail: {} }],
      cart_adds_count: [{ metric: 'cart_adds_count', scope: 'window', window_end: '2026-08-15T03:05:00.000Z', value: 5, detail: {} }],
      tx_count: [
        { metric: 'tx_count', scope: 'window', window_end: '2026-08-15T03:05:00.000Z', value: 3, detail: {} },
        { metric: 'tx_count', scope: 'daily', window_end: '2026-08-15T04:00:00.000Z', value: 21, detail: {} },
      ],
      confirmed_orders: [{ metric: 'confirmed_orders', scope: 'window', window_end: '2026-08-15T03:05:00.000Z', value: 2, detail: {} }],
      delivered_orders: [
        { metric: 'delivered_orders', scope: 'window', window_end: '2026-08-15T03:05:00.000Z', value: 1, detail: {} },
        { metric: 'delivered_orders', scope: 'daily', window_end: '2026-08-15T04:00:00.000Z', value: 11, detail: {} },
      ],
      top_product: [{ metric: 'top_product', scope: 'window', window_end: '2026-08-15T03:05:00.000Z', value: 4, detail: { name: 'Kopi Gayo' } }],
      revenue: [{ metric: 'revenue', scope: 'daily', window_end: '2026-08-15T04:00:00.000Z', value: 1_250_000, detail: {} }],
    },
    jakartaDay: '2026-08-15',
    now,
    recentAlerts: [
      { alert_id: 'alert-1', pattern: 'abandoned_cart', detected_at: '2026-08-15T02:20:00.000Z', detail: {} },
      { alert_id: 'alert-2', pattern: 'slow_delivery', detected_at: '2026-08-15T02:30:00.000Z', detail: {} },
      { alert_id: 'alert-3', pattern: 'trending_product', detected_at: '2026-08-15T02:40:00.000Z', detail: { product_id: 'product-1', product_name: 'Kopi Gayo', buyer_id: 'buyer-7' } },
      { alert_id: 'alert-4', pattern: 'order_surge', detected_at: '2026-08-15T02:50:00.000Z', detail: {} },
      { alert_id: 'alert-5', pattern: 'delivery_completed', detected_at: '2026-08-15T03:00:00.000Z', detail: { order_id: 'order-1', elapsed_seconds: 780 } },
    ],
    sessionStart: '2026-08-15T03:05:00.000Z',
    stats: [],
  });
});

test('renders the live event feed as a labelled semantic table', () => {
  render(<DashboardLivePage />);

  expect(screen.getByRole('heading', { name: 'Live event feed' })).toBeVisible();
  expect(screen.getByRole('table', { name: 'Live event feed' })).toHaveTextContent('cart.item.added');
});

test('renders the seven five-minute metric cards with the revenue total', () => {
  render(<DashboardWindowsPage />);

  expect(screen.getByRole('heading', { name: 'Five-minute windows' })).toBeVisible();
  expect(screen.getAllByRole('article')).toHaveLength(7);
  expect(screen.getByRole('article', { name: 'Today’s revenue' })).toHaveTextContent('Rp');
});

test('renders exactly the five CEP representations without exposing buyer data in trends', () => {
  render(<DashboardPatternsPage />);

  expect(screen.getByRole('heading', { name: 'CEP pattern signals' })).toBeVisible();
  for (const name of ['Abandoned carts', 'Slow delivery', 'Trending products', 'Order surge', 'Checkout to delivery']) {
    expect(screen.getByRole('article', { name })).toBeVisible();
  }
  const trending = screen.getByRole('table', { name: 'Trending products' });
  expect(within(trending).getByText('Kopi Gayo')).toBeVisible();
  expect(trending).not.toHaveTextContent('Buyer');
  expect(trending).not.toHaveTextContent('buyer-7');
});

test('exposes exact alert bucket counts and gives a zero-count bucket zero height', () => {
  render(<AlertCountChart
    label="Abandoned carts"
    points={[
      { start: '2026-08-15T02:20:00.000Z', count: 0 },
      { start: '2026-08-15T02:30:00.000Z', count: 2 },
    ]}
  />);

  const buckets = within(screen.getByRole('list', { name: 'Abandoned carts ten-minute count history' })).getAllByRole('listitem');
  expect(buckets[0]).toHaveAccessibleName(/0 alerts/);
  expect(buckets[0]).toHaveStyle({ height: '0%' });
  expect(buckets[1]).toHaveAccessibleName(/2 alerts/);
});

test('exposes each completed order and elapsed seconds in the duration history', () => {
  render(<DeliveryDurationChart points={[{
    alertId: 'alert-5',
    orderId: 'order-1',
    detectedAt: '2026-08-15T03:00:00.000Z',
    elapsedSeconds: 780,
  }]} />);

  const history = screen.getByRole('list', { name: 'Checkout to delivery elapsed seconds per completed order' });
  expect(within(history).getByRole('listitem')).toHaveTextContent('780s');
  expect(within(history).getByRole('listitem')).toHaveTextContent('order-1');
});

test('keeps the focused metric tooltip visible when the pointer leaves the same bar', () => {
  render(<MetricBarChart
    title="Listings five-minute aligned-window history"
    formatValue={(value) => String(value)}
    buckets={[{
      start: '2026-08-15T03:00:00.000Z',
      windowEnd: '2026-08-15T03:05:00.000Z',
      value: 8,
      detail: {},
    }]}
  />);

  const bar = screen.getByLabelText(/10:00–10:05 WIB: 8/);
  const tooltip = screen.getByRole('status');
  fireEvent.focus(bar);
  fireEvent.pointerEnter(bar);
  fireEvent.pointerLeave(bar);
  expect(tooltip).toHaveTextContent('10:00–10:05 WIB: 8');
});

test('represents the empty duration message as an item in its labelled list', () => {
  render(<DeliveryDurationChart points={[]} />);

  expect(within(screen.getByRole('list', { name: 'Checkout to delivery elapsed seconds per completed order' }))
    .getByRole('listitem')).toHaveTextContent('Waiting for a completed delivery');
});

test('keeps Level 3 histories out of internal scroll containers', () => {
  expect(cssRule('.trending-products').style.overflow).toBe('hidden');
  expect(cssRule('.delivery-duration-chart').style.overflowX).toBe('hidden');
});

test('uses a twelve-pixel minimum for projector chart labels and metadata', () => {
  expect(cssRule('.metric-bucket-y-tick text, .metric-bucket-x-label').style.fontSize).toBe('12px');
  expect(cssRule('.metric-bucket-tooltip').style.fontSize).toBe('0.75rem');
  expect(cssRule('.dashboard-metric-card__header span, .dashboard-pattern-card > header span').style.fontSize).toBe('0.75rem');
  expect(cssRule('.delivery-duration-chart__item small').style.fontSize).toBe('0.75rem');
});

test('uses the supplied raised-shadow token for dashboard cards', () => {
  expect(cssRule('.dashboard-metric-card, .dashboard-pattern-card').style.boxShadow).toBe('var(--shadow-raised)');
});

test('bounds trending products to a clearly labelled top-five view', () => {
  const dashboard = vi.mocked(useDashboard)();
  vi.mocked(useDashboard).mockReturnValue({
    ...dashboard,
    recentAlerts: Array.from({ length: 6 }, (_, index) => ({
      alert_id: `trend-${index}`,
      pattern: 'trending_product',
      detected_at: `2026-08-15T02:${String(40 + index).padStart(2, '0')}:00.000Z`,
      detail: { product_id: `product-${index}`, product_name: `Product ${String.fromCharCode(65 + index)}` },
    })),
  });

  render(<DashboardPatternsPage />);

  const table = screen.getByRole('table', { name: 'Trending products' });
  expect(screen.getByText('Top 5 by count')).toBeVisible();
  expect(within(table).getAllByRole('row')).toHaveLength(6);
  expect(within(table).queryByText('Product F')).not.toBeInTheDocument();
});
