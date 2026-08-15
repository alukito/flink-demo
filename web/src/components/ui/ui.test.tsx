import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { Button } from './Button';
import { EmptyState } from './EmptyState';
import { FeedbackBanner } from './FeedbackBanner';
import { SignalTrace } from './SignalTrace';
import { StatusBadge } from './StatusBadge';

describe('shared UI primitives', () => {
  it('keeps a loading button disabled with its action name', () => {
    render(<Button loading loadingLabel="Adding…">Add product</Button>);
    expect(screen.getByRole('button', { name: 'Adding…' })).toBeDisabled();
  });

  it('exposes lifecycle state as text instead of color alone', () => {
    render(<StatusBadge tone="success">Delivered</StatusBadge>);
    expect(screen.getByText('Delivered')).toHaveAttribute('data-tone', 'success');
  });

  it('uses durable semantic regions for feedback and empty states', () => {
    render(
      <>
        <FeedbackBanner tone="error">Unable to refresh orders</FeedbackBanner>
        <EmptyState title="No orders" description="New checkouts appear here." />
      </>,
    );

    expect(screen.getByRole('alert')).toHaveTextContent('Unable to refresh orders');
    expect(screen.getByRole('status')).toHaveTextContent('New checkouts appear here.');
  });

  it('keeps decorative signal motion out of the accessibility tree', () => {
    const { container } = render(<SignalTrace pulseKey="order-42" />);
    expect(container.firstElementChild).toHaveAttribute('aria-hidden', 'true');
  });
});
