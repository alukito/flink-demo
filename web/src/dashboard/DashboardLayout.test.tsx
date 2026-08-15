import { createContext, useContext, useState } from 'react';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { createMemoryRouter, Navigate, RouterProvider } from 'react-router-dom';
import { beforeEach, expect, test, vi } from 'vitest';
import { DashboardLayout } from './DashboardLayout';
import { useDashboard } from './DashboardContext';
import '../styles/base.css';

vi.mock('./DashboardContext', () => ({
  useDashboard: vi.fn(),
}));

const MarkerContext = createContext('');

function ControlledDashboardShell() {
  const [marker] = useState('retained marker');
  return (
    <MarkerContext.Provider value={marker}>
      <DashboardLayout />
    </MarkerContext.Provider>
  );
}

function StubPage() {
  return <p>{useContext(MarkerContext)}</p>;
}

function renderDashboard(initialEntry = '/dashboard') {
  const router = createMemoryRouter([
    {
      path: '/dashboard',
      element: <ControlledDashboardShell />,
      children: [
        { index: true, element: <Navigate to="live" replace /> },
        { path: 'live', element: <StubPage /> },
        { path: 'windows', element: <StubPage /> },
        { path: 'patterns', element: <StubPage /> },
      ],
    },
  ], { initialEntries: [initialEntry] });
  render(<RouterProvider router={router} />);
  return router;
}

const clearAll = vi.fn();

function cssRule(selector: string): CSSStyleRule {
  const rule = Array.from(document.styleSheets)
    .flatMap((sheet) => Array.from(sheet.cssRules))
    .find((candidate): candidate is CSSStyleRule =>
      candidate instanceof CSSStyleRule && candidate.selectorText === selector);
  if (!rule) throw new Error(`Missing CSS rule: ${selector}`);
  return rule;
}

beforeEach(() => {
  clearAll.mockReset();
  vi.mocked(useDashboard).mockReturnValue({
    connectionState: 'live',
    clearAll,
  } as unknown as ReturnType<typeof useDashboard>);
});

test('redirects to the live level and keeps controlled dashboard content across link navigation', async () => {
  const user = userEvent.setup();
  const router = renderDashboard();

  await waitFor(() => expect(router.state.location.pathname).toBe('/dashboard/live'));
  expect(screen.getByText('retained marker')).toBeInTheDocument();
  expect(screen.getByRole('status')).toHaveTextContent('Live connection');
  expect(screen.getByText(/WIB$/)).toBeInTheDocument();

  await user.click(screen.getByRole('link', { name: /02.*Window metrics/i }));

  expect(router.state.location.pathname).toBe('/dashboard/windows');
  expect(screen.getByText('retained marker')).toBeInTheDocument();
});

test('cancelling the all-level Clear confirmation preserves dashboard state', async () => {
  const user = userEvent.setup();
  renderDashboard('/dashboard/live');

  await user.click(screen.getByRole('button', { name: 'Clear dashboard' }));

  const dialog = screen.getByRole('dialog', { name: 'Clear dashboard data?' });
  expect(dialog).toHaveTextContent('all three dashboard levels');
  await user.click(screen.getByRole('button', { name: 'Cancel' }));

  expect(clearAll).not.toHaveBeenCalled();
  expect(screen.getByText('retained marker')).toBeInTheDocument();
});

test('confirms and clears all dashboard data once', async () => {
  const user = userEvent.setup();
  renderDashboard('/dashboard/patterns');

  await user.click(screen.getByRole('button', { name: 'Clear dashboard' }));
  await user.click(within(screen.getByRole('dialog', { name: 'Clear dashboard data?' })).getByRole('button', { name: 'Clear dashboard' }));

  expect(clearAll).toHaveBeenCalledTimes(1);
  expect(screen.queryByRole('dialog', { name: 'Clear dashboard data?' })).not.toBeInTheDocument();
});

test('pins the header and reserves its height before dashboard content', () => {
  const shell = cssRule('.dashboard-shell').style;
  const header = cssRule('.dashboard-shell__header').style;

  expect(header.position).toBe('fixed');
  expect(header.inset).toBe('0 0 auto');
  expect(header.minBlockSize).toBe('var(--dashboard-header-height)');
  expect(shell.paddingTop).toBe('var(--dashboard-header-height)');
});
