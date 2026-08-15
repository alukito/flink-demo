import { StrictMode } from 'react';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, expect, test, vi } from 'vitest';
import { EventProvider } from '../context/EventContext';
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
  const { clearAll, connectionState, events, stats } = useDashboard();
  return (
    <>
      <output data-testid="event-count">{events.length}</output>
      <output data-testid="stat-count">{stats.length}</output>
      <output data-testid="connection-state">{connectionState}</output>
      <button onClick={clearAll}>reset probe</button>
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
