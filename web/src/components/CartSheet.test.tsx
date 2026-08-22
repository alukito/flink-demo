import { createRef, useRef, useState } from 'react';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { CartSheet, type CartItemView } from './CartSheet';

const items: CartItemView[] = [
  { product: { id: 'coffee', name: 'Flores coffee', price: 12_000 }, quantity: 2 },
  { product: { id: 'tea', name: 'Jasmine tea', price: 5_000 }, quantity: 1 },
];

function SheetHarness({ onPlaceOrder = vi.fn() }: { onPlaceOrder?: () => void }) {
  const [open, setOpen] = useState(false);
  const [address, setAddress] = useState('');
  const triggerRef = useRef<HTMLButtonElement>(null);

  return (
    <>
      <button ref={triggerRef} onClick={() => setOpen(true)}>Review cart</button>
      <CartSheet
        open={open}
        items={items}
        total={29_000}
        address={address}
        submitting={false}
        returnFocusRef={triggerRef}
        onAddressChange={setAddress}
        onClose={() => setOpen(false)}
        onPlaceOrder={onPlaceOrder}
      />
    </>
  );
}

describe('CartSheet', () => {
  it('opens as a labelled modal and traps forward and backward focus', async () => {
    const user = userEvent.setup();
    render(<SheetHarness />);

    await user.click(screen.getByRole('button', { name: 'Review cart' }));

    const dialog = screen.getByRole('dialog', { name: 'Review your cart' });
    const close = screen.getByRole('button', { name: 'Close cart' });
    const address = screen.getByRole('textbox', { name: 'Shipping address' });
    expect(dialog).toHaveAttribute('aria-modal', 'true');
    expect(close).toHaveFocus();

    await user.type(address, 'Jl. Merdeka 8');
    const placeOrder = screen.getByRole('button', { name: 'Place order' });
    placeOrder.focus();
    await user.tab();
    expect(close).toHaveFocus();

    await user.tab({ shift: true });
    expect(placeOrder).toHaveFocus();
  });

  it('closes from Escape, backdrop, and the explicit control and restores trigger focus', async () => {
    const user = userEvent.setup();
    const { container } = render(<SheetHarness />);
    const trigger = screen.getByRole('button', { name: 'Review cart' });

    await user.click(trigger);
    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();

    await user.click(trigger);
    await user.click(container.querySelector('.cart-sheet-backdrop') as HTMLElement);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();

    await user.click(trigger);
    await user.click(screen.getByRole('button', { name: 'Close cart' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });

  it('shows quantities and line totals, retains the address draft, and gates submission', async () => {
    const user = userEvent.setup();
    const onPlaceOrder = vi.fn();
    render(<SheetHarness onPlaceOrder={onPlaceOrder} />);

    await user.click(screen.getByRole('button', { name: 'Review cart' }));
    expect(screen.getByText('2 × Flores coffee')).toBeInTheDocument();
    expect(screen.getByText('Rp 24.000')).toBeInTheDocument();
    expect(screen.getByText('Rp 29.000')).toBeInTheDocument();

    const placeOrder = screen.getByRole('button', { name: 'Place order' });
    expect(placeOrder).toBeDisabled();
    await user.type(screen.getByRole('textbox', { name: 'Shipping address' }), 'Jl. Merdeka 8');
    expect(placeOrder).toBeEnabled();

    await user.click(screen.getByRole('button', { name: 'Close cart' }));
    await user.click(screen.getByRole('button', { name: 'Review cart' }));
    expect(screen.getByRole('textbox', { name: 'Shipping address' })).toHaveValue('Jl. Merdeka 8');
    await user.click(screen.getByRole('button', { name: 'Place order' }));
    expect(onPlaceOrder).toHaveBeenCalledTimes(1);
  });

  it('does not render when closed', () => {
    render(
      <CartSheet
        open={false}
        items={items}
        total={29_000}
        address=""
        submitting={false}
        returnFocusRef={createRef<HTMLButtonElement>()}
        onAddressChange={vi.fn()}
        onClose={vi.fn()}
        onPlaceOrder={vi.fn()}
      />,
    );

    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });
});
