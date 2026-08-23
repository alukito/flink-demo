import { StrictMode } from 'react';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, expect, test, vi } from 'vitest';
import { createMemoryRouter, Link, Outlet, RouterProvider } from 'react-router-dom';
import { EventProvider, useEvents } from '../context/EventContext';
import type { DashboardMessage } from '../context/EventContext';
import { createSession } from '../api/client';
import { useWebSocket } from '../hooks/useWebSocket';
import { DashboardProvider, useDashboard } from './DashboardContext';

vi.mock('../api/client', () => ({
  createSession: vi.fn(),
}));

vi.mock('../hooks/useWebSocket', () => ({
  useWebSocket: vi.fn(),
}));

let onMessage: ((message: DashboardMessage) => void) | undefined;

function Probe() {
  const { clearAll, connectionState, events, recentAlerts, stats } = useDashboard();
  return (
    <>
      <output data-testid="event-count">{events.length}</output>
      <output data-testid="stat-count">{stats.length}</output>
      <output data-testid="alert-count">{recentAlerts.length}</output>
      <output data-testid="connection-state">{connectionState}</output>
      <button onClick={clearAll}>reset probe</button>
    </>
  );
}

function RoleFeedProbe() {
  const { addEvent, events } = useEvents();
  return (
    <>
      <output data-testid="role-event-count">{events.length}</output>
      <button onClick={() => addEvent({
        event_id: 'role-event',
        event_type: 'product.listed',
        actor_id: 'seller-1',
        actor_role: 'seller',
        timestamp: '2026-08-15T03:02:00.000Z',
        payload: {},
      })}>receive role event</button>
      <Link to="/dashboard/live">Open dashboard</Link>
    </>
  );
}

function DashboardFeedProbe() {
  const { events } = useDashboard();
  return (
    <>
      <output data-testid="dashboard-event-count">{events.length}</output>
      <Link to="/dashboard/windows">Window metrics</Link>
    </>
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  onMessage = undefined;
  vi.mocked(createSession).mockResolvedValue({ id: 'dashboard-id', token: 'dashboard-token', name: 'dashboard', role: 'dashboard' });
  vi.mocked(useWebSocket).mockImplementation((callback) => {
    onMessage = callback as (message: DashboardMessage) => void;
    return { connected: true };
  });
});

test('owns one dashboard token and clears raw events with derived dashboard data', async () => {
  const user = userEvent.setup();
  render(
    <EventProvider>
      <DashboardProvider><Probe /></DashboardProvider>
    </EventProvider>,
  );

  await waitFor(() => expect(createSession).toHaveBeenCalledTimes(1));
  await waitFor(() => expect(onMessage).toBeDefined());
  act(() => onMessage?.({
    event_id: 'event-1',
    event_type: 'cart.added',
    actor_id: 'buyer-1',
    actor_role: 'buyer',
    timestamp: '2026-08-15T03:02:00.000Z',
    payload: {},
  }));
  act(() => onMessage?.({
    metric: 'listings_count',
    scope: 'window',
    window_end: '2026-08-15T03:05:00.000Z',
    value: 1,
    detail: {},
  }));

  expect(screen.getByTestId('event-count')).toHaveTextContent('1');
  expect(screen.getByTestId('stat-count')).toHaveTextContent('1');
  await user.click(screen.getByRole('button', { name: 'reset probe' }));
  expect(screen.getByTestId('event-count')).toHaveTextContent('0');
  expect(screen.getByTestId('stat-count')).toHaveTextContent('0');
});

test('shares one dashboard token request across the StrictMode effect replay', async () => {
  render(
    <StrictMode>
      <EventProvider>
        <DashboardProvider><Probe /></DashboardProvider>
      </EventProvider>
    </StrictMode>,
  );

  await waitFor(() => expect(createSession).toHaveBeenCalledTimes(1));
  await waitFor(() => expect(screen.getByTestId('connection-state')).toHaveTextContent('live'));
});

test('drops explicit replay while retaining a live daily metric received before any raw event', async () => {
  render(
    <EventProvider>
      <DashboardProvider><Probe /></DashboardProvider>
    </EventProvider>,
  );

  await waitFor(() => expect(onMessage).toBeDefined());
  act(() => onMessage?.({
    metric: 'tx_count',
    scope: 'daily',
    window_end: '2026-08-16T17:00:00.000Z',
    value: 9,
    detail: {},
    replay: true,
  }));
  act(() => onMessage?.({
    alert_id: 'replayed-alert',
    pattern: 'order_surge',
    detected_at: '2026-08-15T03:00:00.000Z',
    detail: { checkout_count: 3 },
    replay: true,
  }));

  expect(screen.getByTestId('stat-count')).toHaveTextContent('0');
  expect(screen.getByTestId('alert-count')).toHaveTextContent('0');

  act(() => onMessage?.({
    metric: 'tx_count',
    scope: 'daily',
    window_end: '2026-08-16T17:00:00.000Z',
    value: 1,
    detail: {},
  }));

  expect(screen.getByTestId('event-count')).toHaveTextContent('0');
  expect(screen.getByTestId('stat-count')).toHaveTextContent('1');
});

test('does not restore cleared state from replay messages on a later connection', async () => {
  const user = userEvent.setup();
  render(
    <EventProvider>
      <DashboardProvider><Probe /></DashboardProvider>
    </EventProvider>,
  );

  await waitFor(() => expect(onMessage).toBeDefined());
  act(() => onMessage?.({
    metric: 'listings_count',
    scope: 'window',
    window_end: '2026-08-15T03:05:00.000Z',
    value: 1,
    detail: {},
  }));
  expect(screen.getByTestId('stat-count')).toHaveTextContent('1');

  await user.click(screen.getByRole('button', { name: 'reset probe' }));
  act(() => onMessage?.({
    metric: 'listings_count',
    scope: 'window',
    window_end: '2026-08-15T03:05:00.000Z',
    value: 1,
    detail: {},
    replay: true,
  }));

  expect(screen.getByTestId('stat-count')).toHaveTextContent('0');
});

test('starts a role-to-dashboard navigation with an empty dashboard feed and preserves dashboard events between levels', async () => {
  const user = userEvent.setup();
  const router = createMemoryRouter([{
    element: <EventProvider><Outlet /></EventProvider>,
    children: [
      { path: '/buyer', element: <RoleFeedProbe /> },
      {
        path: '/dashboard',
        element: <DashboardProvider><Outlet /></DashboardProvider>,
        children: [
          { path: 'live', element: <DashboardFeedProbe /> },
          { path: 'windows', element: <DashboardFeedProbe /> },
        ],
      },
    ],
  }], { initialEntries: ['/buyer'] });
  render(<RouterProvider router={router} />);

  await user.click(screen.getByRole('button', { name: 'receive role event' }));
  expect(screen.getByTestId('role-event-count')).toHaveTextContent('1');
  await user.click(screen.getByRole('link', { name: 'Open dashboard' }));

  expect(screen.getByTestId('dashboard-event-count')).toHaveTextContent('0');
  await waitFor(() => expect(onMessage).toBeDefined());
  act(() => onMessage?.({
    event_id: 'dashboard-event',
    event_type: 'cart.checkout',
    actor_id: 'buyer-1',
    actor_role: 'buyer',
    timestamp: '2026-08-15T03:03:00.000Z',
    payload: {},
  }));
  expect(screen.getByTestId('dashboard-event-count')).toHaveTextContent('1');

  await user.click(screen.getByRole('link', { name: 'Window metrics' }));
  expect(router.state.location.pathname).toBe('/dashboard/windows');
  expect(screen.getByTestId('dashboard-event-count')).toHaveTextContent('1');
});
