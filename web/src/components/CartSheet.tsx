import {
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type KeyboardEvent,
  type RefObject,
} from 'react';
import { FOCUSABLE_SELECTOR, nextFocusIndex } from '../lib/focusTrap';
import { Button } from './ui/Button';

export interface CartItemView {
  product: {
    id: string;
    name: string;
    price: number;
  };
  quantity: number;
}

export interface CartSheetProps {
  open: boolean;
  items: CartItemView[];
  total: number;
  address: string;
  submitting: boolean;
  returnFocusRef: RefObject<HTMLButtonElement | null>;
  shouldRestoreFocus?: () => boolean;
  onAddressChange: (value: string) => void;
  onClose: () => void;
  onPlaceOrder: () => void;
}

function formatPrice(price: number): string {
  return `Rp ${price.toLocaleString('id-ID')}`;
}

function desktopPanelMatches(): boolean {
  return typeof window.matchMedia === 'function'
    && window.matchMedia('(min-width: 60rem)').matches;
}

function alwaysRestoreFocus(): boolean {
  return true;
}

function isolateModalBackground(layer: HTMLElement): () => void {
  const changedElements: HTMLElement[] = [];
  let foreground: HTMLElement | null = layer;

  while (foreground?.parentElement) {
    const parent: HTMLElement = foreground.parentElement;
    Array.from(parent.children).forEach((sibling) => {
      if (sibling === foreground || !(sibling instanceof HTMLElement) || sibling.hasAttribute('inert')) return;
      sibling.setAttribute('inert', '');
      changedElements.push(sibling);
    });
    foreground = parent;
    if (foreground === document.body) break;
  }

  return () => {
    changedElements.forEach((element) => {
      element.removeAttribute('inert');
    });
  };
}

export function CartSheet({
  open,
  items,
  total,
  address,
  submitting,
  returnFocusRef,
  shouldRestoreFocus = alwaysRestoreFocus,
  onAddressChange,
  onClose,
  onPlaceOrder,
}: CartSheetProps) {
  const headingId = useId();
  const sheetRef = useRef<HTMLElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);
  const [isDesktopPanel, setIsDesktopPanel] = useState(desktopPanelMatches);

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return undefined;
    const query = window.matchMedia('(min-width: 60rem)');
    const update = () => setIsDesktopPanel(query.matches);
    update();
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);

  useEffect(() => {
    if (!open) return undefined;
    const returnFocusElement = returnFocusRef.current;
    closeRef.current?.focus();
    return () => {
      if (shouldRestoreFocus()) returnFocusElement?.focus();
    };
  }, [open, returnFocusRef, shouldRestoreFocus]);

  useLayoutEffect(() => {
    if (!open || isDesktopPanel) return undefined;
    const layer = sheetRef.current?.parentElement;
    if (!layer) return undefined;

    const restoreBackground = isolateModalBackground(layer);
    const keepFocusInside = (event: FocusEvent) => {
      if (event.target instanceof Node && !layer.contains(event.target)) {
        closeRef.current?.focus();
      }
    };
    document.addEventListener('focusin', keepFocusInside);

    return () => {
      document.removeEventListener('focusin', keepFocusInside);
      restoreBackground();
    };
  }, [isDesktopPanel, open]);

  if (!open) return null;

  const handleKeyDown = (event: KeyboardEvent<HTMLElement>) => {
    if (event.key === 'Escape') {
      event.preventDefault();
      onClose();
      return;
    }
    if (event.key !== 'Tab') return;

    const focusable = Array.from(
      sheetRef.current?.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR) ?? [],
    );
    if (focusable.length === 0) {
      event.preventDefault();
      return;
    }

    const activeIndex = focusable.indexOf(document.activeElement as HTMLElement);
    const isAtEdge = event.shiftKey ? activeIndex <= 0 : activeIndex === focusable.length - 1;
    if (!isAtEdge) return;

    event.preventDefault();
    focusable[nextFocusIndex(activeIndex, focusable.length, event.shiftKey)]?.focus();
  };

  return (
    <div
      className="cart-sheet-layer"
      data-layout={isDesktopPanel ? 'panel' : 'sheet'}
    >
      <div
        className="cart-sheet-backdrop"
        aria-hidden="true"
        onClick={(event) => {
          if (event.currentTarget === event.target) onClose();
        }}
      />
      <section
        ref={sheetRef}
        className="cart-sheet"
        role="dialog"
        aria-modal={isDesktopPanel ? undefined : true}
        aria-labelledby={headingId}
        onKeyDown={handleKeyDown}
      >
        <header className="cart-sheet__header">
          <div>
            <span className="cart-sheet__eyebrow">Checkout</span>
            <h2 id={headingId}>Review your cart</h2>
          </div>
          <button ref={closeRef} type="button" className="button button--ghost" aria-label="Close cart" onClick={onClose}>
            Close
          </button>
        </header>

        <div className="cart-sheet__body">
          <ul className="cart-sheet__items" aria-label="Cart items">
            {items.map((item) => (
              <li key={item.product.id}>
                <span>{item.quantity} × {item.product.name}</span>
                <strong>{formatPrice(item.product.price * item.quantity)}</strong>
              </li>
            ))}
          </ul>

          <div className="cart-sheet__total">
            <span>Total</span>
            <strong>{formatPrice(total)}</strong>
          </div>

          <label className="cart-sheet__address">
            <span>Shipping address</span>
            <textarea
              rows={3}
              value={address}
              onChange={(event) => onAddressChange(event.target.value)}
              autoComplete="street-address"
              placeholder="Street, area, and city"
            />
          </label>
        </div>

        <footer className="cart-sheet__footer">
          <Button
            type="button"
            loading={submitting}
            loadingLabel="Placing order…"
            disabled={!address.trim() || items.length === 0}
            onClick={onPlaceOrder}
          >
            Place order
          </Button>
        </footer>
      </section>
    </div>
  );
}
