import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { createSession } from '../api/client';
import { SessionProvider } from '../context/SessionContext';
import '../styles/base.css';
import Landing from './Landing';

vi.mock('../api/client', () => ({
  createSession: vi.fn(),
}));

const createSessionMock = vi.mocked(createSession);

function renderLanding() {
  return render(
    <SessionProvider>
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<Landing />} />
          <Route path="/buyer" element={<h1>Buyer workspace</h1>} />
        </Routes>
      </MemoryRouter>
    </SessionProvider>,
  );
}

describe('Landing', () => {
  beforeEach(() => {
    localStorage.clear();
    createSessionMock.mockReset();
  });

  it('puts the display name before three equally weighted role choices', () => {
    renderLanding();

    const nameInput = screen.getByRole('textbox', { name: 'Display name' });
    const roleButtons = [
      screen.getByRole('button', { name: /Shop as buyer/ }),
      screen.getByRole('button', { name: /Sell products/ }),
      screen.getByRole('button', { name: /Deliver orders/ }),
    ];

    expect(nameInput.compareDocumentPosition(roleButtons[0]) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(new Set(roleButtons.map((button) => button.className)).size).toBe(1);
    expect(roleButtons.every((button) => button.classList.contains('landing-role-card'))).toBe(true);
    expect(screen.getByText('Browse, add, and place an order.')).toBeInTheDocument();
    expect(screen.getByText('Add products and confirm orders.')).toBeInTheDocument();
    expect(screen.getByText('Pick jobs and complete delivery.')).toBeInTheDocument();
  });

  it('keeps the presenter dashboard as a secondary link', () => {
    renderLanding();

    expect(screen.getByRole('link', { name: 'Open presenter dashboard' })).toHaveAttribute(
      'href',
      '/dashboard',
    );
  });

  it('keeps role copy and the presenter link inside narrow layout tracks', () => {
    renderLanding();

    const rules = Array.from(document.styleSheets).flatMap((sheet) => Array.from(sheet.cssRules));
    const styleFor = (selector: string) => rules.find(
      (rule): rule is CSSStyleRule => rule instanceof CSSStyleRule && rule.selectorText === selector,
    )?.style;

    expect(styleFor('.landing-role-card')?.justifyContent).toBe('stretch');
    expect(styleFor('.landing-dashboard-link')?.maxWidth).toBe('100%');
    expect(styleFor('.landing-dashboard-link')?.overflowWrap).toBe('anywhere');
  });

  it('focuses the display name and shows the exact missing-name error', async () => {
    const user = userEvent.setup();
    renderLanding();

    await user.click(screen.getByRole('button', { name: /Shop as buyer/ }));

    expect(screen.getByRole('alert')).toHaveTextContent('Enter your display name to continue.');
    expect(screen.getByRole('textbox', { name: 'Display name' })).toHaveFocus();
    expect(createSessionMock).not.toHaveBeenCalled();
  });

  it('creates a buyer session and navigates to the buyer workspace', async () => {
    createSessionMock.mockResolvedValue({
      id: 'participant-1',
      token: 'token-1',
      name: 'Ayu',
      role: 'buyer',
    });
    const user = userEvent.setup();
    renderLanding();

    await user.type(screen.getByRole('textbox', { name: 'Display name' }), '  Ayu  ');
    await user.click(screen.getByRole('button', { name: /Shop as buyer/ }));

    expect(createSessionMock).toHaveBeenCalledWith('Ayu', 'buyer');
    expect(await screen.findByRole('heading', { name: 'Buyer workspace' })).toBeInTheDocument();
  });
});
