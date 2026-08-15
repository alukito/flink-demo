# Responsive Demo UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign every audience-facing role page for phone interaction and split the stream-processing dashboard into three projector-ready routes without changing backend or stream semantics.

**Architecture:** Introduce a small CSS-token design system and focused shared UI components, then move dashboard connection/state above nested dashboard routes so navigation preserves one in-memory session. Refactor each existing role page around the approved mobile task hierarchy while retaining its current API calls, UUID filtering, and refresh guards.

**Tech Stack:** React 19, React Router 7, TypeScript 6, D3 7, Vite 8, Vitest, Testing Library, CSS custom properties, bundled Fontsource packages, Go embedded frontend, Docker Compose.

## Global Constraints

- Keep all backend APIs, event schemas, Kafka/Flink behavior, UUID authorization, window definitions, and CEP definitions unchanged.
- Use bundled Sora, IBM Plex Sans, and IBM Plex Mono fonts; do not fetch fonts at runtime.
- Core colors are Ink `#10243C`, Deep ink `#142B46`, Canvas `#EDF3F7`, Surface `#FFFFFF`, Steel `#54708B`, and Brass `#F2BD58`.
- Semantic colors are Success `#16775A`, Warning `#8A5A00`, and Error `#B42318`.
- Brass is a small accent, not a primary button surface; any text on Brass uses Ink.
- Role pages are mobile-first from 320px, use 44×44px minimum targets, and must not create horizontal page scrolling.
- Dashboard presentation mode applies only at viewports of at least 1600×900; it must fit one viewport at 1920×1080.
- Dashboard navigation preserves state; browser reload and confirmed Clear reset all three levels.
- Retain existing event-ID deduplication, 100-event cap, fixed 24-slot D3 metric grids, WIB labels, and eight-hour immutable CEP retention.
- Use strict TDD: capture an expected failing test before production implementation in every task.
- Preserve the existing untracked `.superpowers/brainstorm/` directory and never stage it.
- Regenerate and commit `app/web/dist` only in the final integration task.

---

## File Structure

### New foundation files

- `web/src/styles/tokens.css` — color, typography, spacing, radius, shadow, and motion tokens.
- `web/src/styles/base.css` — reset, body, form, focus, utility, and responsive base rules.
- `web/src/test/setup.ts` — DOM matchers, ResizeObserver stub, and deterministic browser-test cleanup.
- `web/vitest.config.ts` — jsdom component-test configuration.
- `web/src/components/ui/Button.tsx` — typed button variants and loading label.
- `web/src/components/ui/StatusBadge.tsx` — text-plus-color lifecycle badge.
- `web/src/components/ui/FeedbackBanner.tsx` — persistent success/error status region.
- `web/src/components/ui/EmptyState.tsx` — actionable empty-state copy.
- `web/src/components/ui/SignalTrace.tsx` — event-keyed pulse with reduced-motion CSS fallback.

### New dashboard files

- `web/src/dashboard/dashboardState.ts` — pure metric/alert reducer and reset state.
- `web/src/dashboard/DashboardContext.tsx` — token, WebSocket, timers, events, derived data, and all-level reset.
- `web/src/dashboard/dashboardRoutes.ts` — ordered route metadata and adjacent-route helpers.
- `web/src/dashboard/DashboardLayout.tsx` — header, rail/stepper, Outlet, controls, and clear dialog.
- `web/src/dashboard/DashboardMetricCard.tsx` — projector metric card wrapper around existing D3 charts.
- `web/src/dashboard/AlertCountChart.tsx` — abandoned-cart and slow-delivery count history.
- `web/src/dashboard/DeliveryDurationChart.tsx` — checkout-to-delivery duration history.
- `web/src/pages/dashboard/DashboardLivePage.tsx` — Level 1 canvas.
- `web/src/pages/dashboard/DashboardWindowsPage.tsx` — Level 2 canvas.
- `web/src/pages/dashboard/DashboardPatternsPage.tsx` — Level 3 canvas.

### New role files

- `web/src/components/RoleLayout.tsx` — shared mobile header, identity menu, content shell, and signal trace.
- `web/src/components/ActionCard.tsx` — task-priority panel.
- `web/src/components/CartSheet.tsx` — focus-managed checkout sheet/panel.
- `web/src/lib/focusTrap.ts` — testable focusable-element and Tab-cycle helpers.
- `web/src/lib/feedback.ts` — stable action-feedback state helpers.

### Existing files substantially modified

- `web/package.json`, `web/package-lock.json` — local fonts and UI-test tooling.
- `web/src/index.css` — imports only, with old global dashboard rules removed after migration.
- `web/src/App.tsx` — nested dashboard routes and page imports.
- `web/src/pages/Landing.tsx` — audience entry hierarchy.
- `web/src/pages/Buyer.tsx` — catalog, cart sheet, and recent-order hierarchy.
- `web/src/pages/Seller.tsx` — add-product and actionable-order hierarchy.
- `web/src/pages/Shipper.tsx` — active-first delivery hierarchy.
- `web/src/components/MetricBarChart.tsx` — responsive projector sizing and existing accessible tooltip preserved.
- `Makefile` — include the new UI component suite in canonical verification.

---

### Task 1: Design Tokens, Fonts, and UI Test Harness

**Files:**
- Modify: `web/package.json`
- Modify: `web/package-lock.json`
- Create: `web/vitest.config.ts`
- Create: `web/src/test/setup.ts`
- Create: `web/src/styles/tokens.css`
- Create: `web/src/styles/base.css`
- Modify: `web/src/index.css`
- Create: `web/src/components/ui/Button.tsx`
- Create: `web/src/components/ui/StatusBadge.tsx`
- Create: `web/src/components/ui/FeedbackBanner.tsx`
- Create: `web/src/components/ui/EmptyState.tsx`
- Create: `web/src/components/ui/SignalTrace.tsx`
- Test: `web/src/components/ui/ui.test.tsx`
- Modify: `Makefile`

**Interfaces:**
- Produces: `Button`, `StatusBadge`, `FeedbackBanner`, `EmptyState`, and `SignalTrace` components used by every later task.
- Produces: `npm run test:ui`, which runs Vitest once under jsdom.

- [ ] **Step 1: Install exact testing and local-font dependencies**

Run from `web/`:

```bash
npm install @fontsource-variable/sora @fontsource-variable/ibm-plex-sans @fontsource/ibm-plex-mono
npm install --save-dev vitest jsdom @testing-library/react @testing-library/user-event @testing-library/jest-dom
```

Add scripts without changing the existing Node test command:

```json
{
  "scripts": {
    "test": "node --test src/lib/*.test.ts",
    "test:ui": "vitest run",
    "test:ui:watch": "vitest"
  }
}
```

- [ ] **Step 2: Add the component-test configuration**

Create `web/vitest.config.ts`:

```ts
import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    include: ['src/**/*.test.tsx'],
    css: true,
  },
});
```

Create `web/src/test/setup.ts`:

```ts
import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach, vi } from 'vitest';

afterEach(() => cleanup());

class TestResizeObserver {
  observe = vi.fn();
  unobserve = vi.fn();
  disconnect = vi.fn();
}

vi.stubGlobal('ResizeObserver', TestResizeObserver);
```

- [ ] **Step 3: Write failing primitive tests**

Create `web/src/components/ui/ui.test.tsx`:

```tsx
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { Button } from './Button';
import { EmptyState } from './EmptyState';
import { FeedbackBanner } from './FeedbackBanner';
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
    render(<><FeedbackBanner tone="error">Unable to refresh orders</FeedbackBanner><EmptyState title="No orders" description="New checkouts appear here." /></>);
    expect(screen.getByRole('alert')).toHaveTextContent('Unable to refresh orders');
    expect(screen.getByRole('status')).toHaveTextContent('New checkouts appear here.');
  });
});
```

- [ ] **Step 4: Run the UI test to verify RED**

Run: `npm run test:ui -- src/components/ui/ui.test.tsx`
Expected: FAIL because the five shared components do not exist.

- [ ] **Step 5: Implement the typed primitives**

Use these public signatures:

```tsx
// Button.tsx
export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger';
export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  loading?: boolean;
  loadingLabel?: string;
}
export function Button({ variant = 'primary', loading = false, loadingLabel = 'Working…', children, disabled, ...props }: ButtonProps) {
  return <button className={`button button--${variant}`} disabled={disabled || loading} aria-busy={loading || undefined} {...props}>{loading ? loadingLabel : children}</button>;
}
```

```tsx
// StatusBadge.tsx
export type StatusTone = 'neutral' | 'warning' | 'info' | 'success' | 'error';
export function StatusBadge({ tone, children }: { tone: StatusTone; children: React.ReactNode }) {
  return <span className="status-badge" data-tone={tone}>{children}</span>;
}
```

```tsx
// FeedbackBanner.tsx
export function FeedbackBanner({ tone, children }: { tone: 'success' | 'error'; children: React.ReactNode }) {
  return <div className="feedback-banner" data-tone={tone} role={tone === 'error' ? 'alert' : 'status'}>{children}</div>;
}
```

`EmptyState` must render one `role="status"` region. `SignalTrace` accepts `{ pulseKey: string | number }`, uses the key only to restart a CSS animation, and sets `aria-hidden="true"`.

- [ ] **Step 6: Add local fonts and exact tokens**

Make `web/src/index.css` import:

```css
@import '@fontsource-variable/sora';
@import '@fontsource-variable/ibm-plex-sans';
@import '@fontsource/ibm-plex-mono/400.css';
@import './styles/tokens.css';
@import './styles/base.css';
```

Define the approved values in `tokens.css`:

```css
:root {
  --color-ink: #10243c;
  --color-deep-ink: #142b46;
  --color-canvas: #edf3f7;
  --color-surface: #ffffff;
  --color-steel: #54708b;
  --color-brass: #f2bd58;
  --color-success: #16775a;
  --color-warning: #8a5a00;
  --color-error: #b42318;
  --font-display: 'Sora Variable', sans-serif;
  --font-body: 'IBM Plex Sans Variable', sans-serif;
  --font-data: 'IBM Plex Mono', monospace;
  --focus-ring: 0 0 0 3px rgb(46 111 206 / 35%);
  --radius-sm: 0.5rem;
  --radius-md: 0.75rem;
  --shadow-raised: 0 12px 32px rgb(16 36 60 / 10%);
}
```

In `base.css`, set a 44px minimum control height, visible `:focus-visible`, Canvas body background, and `@media (prefers-reduced-motion: reduce)` rules that disable signal and highlight animations.

- [ ] **Step 7: Add UI tests to canonical verification and run GREEN**

Add `cd web && npm run test:ui` after the existing Node tests in `Makefile`'s `verify` target.

Run:

```bash
npm run test:ui -- src/components/ui/ui.test.tsx
npm test
npm run lint
npm run build
```

Expected: component tests PASS, existing Node tests PASS, lint has zero errors, and build succeeds.

- [ ] **Step 8: Commit**

```bash
git add web/package.json web/package-lock.json web/vitest.config.ts web/src/test/setup.ts web/src/styles web/src/index.css web/src/components/ui Makefile
git commit -m "feat: add responsive UI foundations"
```

---

### Task 2: Dashboard State Provider and Reset Semantics

**Files:**
- Create: `web/src/dashboard/dashboardState.ts`
- Create: `web/src/dashboard/DashboardContext.tsx`
- Test: `web/src/lib/dashboardState.test.ts`
- Test: `web/src/dashboard/DashboardContext.test.tsx`
- Modify: `web/src/pages/Dashboard.tsx` only to consume the provider temporarily until Task 3 removes it

**Interfaces:**
- Consumes: existing `DashboardMessage`, `WindowStat`, `CepAlert`, `useEvents`, `useWebSocket`, `requestFreshDashboardToken`, and Jakarta/metric/CEP helpers.
- Produces: `DashboardProvider`, `useDashboard()`, `dashboardReducer()`, and `initialDashboardData()`.

- [ ] **Step 1: Write failing pure reducer tests**

Create `web/src/lib/dashboardState.test.ts` with these cases:

```ts
test('replaces one metric snapshot by metric scope and window end', () => {
  const first = stat({ value: 2 });
  const latest = stat({ value: 9 });
  const state = dashboardReducer(dashboardReducer(initialDashboardData(), { type: 'message', message: first }), { type: 'message', message: latest });
  assert.deepEqual(state.stats, [latest]);
});

test('clear returns empty metrics and alerts while preserving session timing', () => {
  const openedAt = new Date('2026-08-15T03:02:00Z');
  const state = initialDashboardData(openedAt);
  const cleared = dashboardReducer({ ...state, stats: [stat()], alerts: [alert()] }, { type: 'clear' });
  assert.equal(cleared.sessionStart, state.sessionStart);
  assert.deepEqual(cleared.stats, []);
  assert.deepEqual(cleared.alerts, []);
});
```

Also assert 24-window retention per metric and immutable alert upsert/retention through existing helpers.

- [ ] **Step 2: Run reducer tests to verify RED**

Run: `node --test src/lib/dashboardState.test.ts`
Expected: FAIL because `dashboardState.ts` is missing.

- [ ] **Step 3: Implement the pure dashboard reducer**

Use exact types:

```ts
export interface DashboardData {
  stats: WindowStat[];
  alerts: CepAlert[];
  sessionStart: string;
  jakartaDay: string;
  now: Date;
}

export type DashboardAction =
  | { type: 'message'; message: DashboardMessage }
  | { type: 'tick'; now: Date }
  | { type: 'jakarta-day'; day: string }
  | { type: 'clear' };

export function initialDashboardData(now = new Date()): DashboardData;
export function dashboardReducer(state: DashboardData, action: DashboardAction): DashboardData;
```

Move the existing Dashboard component's metric replacement, 24-window retention, alert upsert, and alert pruning logic into this reducer without changing behavior.

- [ ] **Step 4: Run reducer tests GREEN**

Run: `node --test src/lib/dashboardState.test.ts`
Expected: all dashboard reducer cases PASS.

- [ ] **Step 5: Write a failing provider lifecycle test**

Mock `createSession` and `useWebSocket` with Vitest. Render a probe inside `DashboardProvider`, deliver an event and metric through the captured message callback, then click Clear.

```tsx
expect(createSession).toHaveBeenCalledTimes(1);
expect(screen.getByTestId('event-count')).toHaveTextContent('1');
expect(screen.getByTestId('stat-count')).toHaveTextContent('1');
await user.click(screen.getByRole('button', { name: 'reset probe' }));
expect(screen.getByTestId('event-count')).toHaveTextContent('0');
expect(screen.getByTestId('stat-count')).toHaveTextContent('0');
```

- [ ] **Step 6: Run provider test RED**

Run: `npm run test:ui -- src/dashboard/DashboardContext.test.tsx`
Expected: FAIL because `DashboardProvider` and `useDashboard` do not exist.

- [ ] **Step 7: Implement one provider for all dashboard levels**

Expose:

```ts
export type DashboardConnectionState = 'connecting' | 'reconnecting' | 'live';

export interface DashboardContextValue extends DashboardData {
  events: EventEnvelope[];
  connectionState: DashboardConnectionState;
  clearAll: () => void;
  groupedStats: Record<MetricName, WindowStat[]>;
  recentAlerts: CepAlert[];
}
```

The provider must request one fresh dashboard token on mount, open one WebSocket, reuse `EventContext` for raw-event deduplication, schedule the current five-second tick and Jakarta rollover, and make `clearAll()` clear EventContext plus reducer metrics/alerts. Do not persist dashboard state in localStorage or sessionStorage.

- [ ] **Step 8: Run provider and full frontend tests GREEN**

Run:

```bash
npm run test:ui -- src/dashboard/DashboardContext.test.tsx
npm test
npm run test:ui
npm run lint
npm run build
```

- [ ] **Step 9: Commit**

```bash
git add web/src/dashboard web/src/lib/dashboardState.test.ts web/src/pages/Dashboard.tsx
git commit -m "refactor: centralize dashboard session state"
```

---

### Task 3: Nested Dashboard Routes and Presentation Shell

**Files:**
- Create: `web/src/dashboard/dashboardRoutes.ts`
- Create: `web/src/dashboard/DashboardLayout.tsx`
- Test: `web/src/lib/dashboardRoutes.test.ts`
- Test: `web/src/dashboard/DashboardLayout.test.tsx`
- Modify: `web/src/App.tsx`
- Delete: `web/src/pages/Dashboard.tsx` after its content is migrated to Task 4 page stubs

**Interfaces:**
- Consumes: `DashboardProvider`, `useDashboard`, `Button`, `SignalTrace`, and React Router `Outlet`.
- Produces: ordered `DASHBOARD_STEPS`, `dashboardAdjacentPath()`, and a nested route shell.

- [ ] **Step 1: Write route metadata tests RED**

```ts
assert.deepEqual(DASHBOARD_STEPS.map((step) => step.path), [
  '/dashboard/live',
  '/dashboard/windows',
  '/dashboard/patterns',
]);
assert.equal(dashboardAdjacentPath('/dashboard/windows', 1), '/dashboard/patterns');
assert.equal(dashboardAdjacentPath('/dashboard/live', -1), null);
```

Run: `node --test src/lib/dashboardRoutes.test.ts`
Expected: FAIL because route metadata is missing.

- [ ] **Step 2: Implement route metadata GREEN**

```ts
export interface DashboardStep {
  number: '01' | '02' | '03';
  path: string;
  shortLabel: string;
  title: string;
  eyebrow: string;
}
export const DASHBOARD_STEPS: readonly DashboardStep[];
export function dashboardAdjacentPath(pathname: string, direction: -1 | 1): string | null;
```

Run the focused Node test and confirm PASS.

- [ ] **Step 3: Write failing shell/navigation tests**

Use `createMemoryRouter` and stub child pages. Verify:

```tsx
expect(router.state.location.pathname).toBe('/dashboard/live');
await user.click(screen.getByRole('link', { name: /02.*Window metrics/i }));
expect(router.state.location.pathname).toBe('/dashboard/windows');
expect(screen.getByText('retained marker')).toBeInTheDocument();
```

Also verify the Clear button opens an accessible confirmation dialog whose copy says all three levels reset, Cancel preserves state, and Clear dashboard invokes `clearAll()` once.

- [ ] **Step 4: Run shell tests RED**

Run: `npm run test:ui -- src/dashboard/DashboardLayout.test.tsx`
Expected: FAIL because the layout and nested routes do not exist.

- [ ] **Step 5: Implement nested dashboard routing**

Update `App.tsx` to this structure:

```tsx
<Route path="/dashboard" element={<DashboardProvider><DashboardLayout /></DashboardProvider>}>
  <Route index element={<Navigate to="live" replace />} />
  <Route path="live" element={<DashboardLivePage />} />
  <Route path="windows" element={<DashboardWindowsPage />} />
  <Route path="patterns" element={<DashboardPatternsPage />} />
</Route>
```

`DashboardLayout` renders the fixed projector header, connection state, WIB clock, vertical rail at 1280px+, horizontal stepper below 1280px, `Outlet`, adjacent controls, and a native accessible confirmation dialog or equivalent tested dialog component. Direct level navigation must use links, not buttons.

- [ ] **Step 6: Add responsive shell CSS**

Use a presentation class guarded by:

```css
@media (min-width: 1600px) and (min-height: 900px) {
  .dashboard-shell { height: 100dvh; overflow: hidden; }
  .dashboard-canvas { min-height: 0; overflow: hidden; }
}
@media (max-width: 1279px) {
  .dashboard-shell__body { grid-template-columns: 1fr; }
  .dashboard-steps { display: grid; grid-template-columns: repeat(3, 1fr); }
}
```

- [ ] **Step 7: Run shell and regression gates GREEN**

```bash
npm run test:ui -- src/dashboard/DashboardLayout.test.tsx
npm test
npm run test:ui
npm run lint
npm run build
```

- [ ] **Step 8: Commit**

```bash
git add web/src/App.tsx web/src/dashboard web/src/lib/dashboardRoutes.test.ts web/src/pages/dashboard web/src/pages/Dashboard.tsx web/src/styles
git commit -m "feat: split dashboard into presentation routes"
```

---

### Task 4: Three Projector-Ready Dashboard Canvases

**Files:**
- Create: `web/src/dashboard/DashboardMetricCard.tsx`
- Create: `web/src/dashboard/AlertCountChart.tsx`
- Create: `web/src/dashboard/DeliveryDurationChart.tsx`
- Create: `web/src/pages/dashboard/DashboardLivePage.tsx`
- Create: `web/src/pages/dashboard/DashboardWindowsPage.tsx`
- Create: `web/src/pages/dashboard/DashboardPatternsPage.tsx`
- Modify: `web/src/components/MetricBarChart.tsx`
- Test: `web/src/pages/dashboard/DashboardPages.test.tsx`
- Modify: `web/src/styles/base.css`

**Interfaces:**
- Consumes: `useDashboard()`, existing `metricBuckets`, Jakarta formatters, and CEP selectors.
- Produces: three complete route canvases with no document scroll at 1920×1080.

- [ ] **Step 1: Write failing semantic page tests**

Render each page under a test dashboard context and assert:

```tsx
expect(screen.getByRole('heading', { name: 'Live event feed' })).toBeVisible();
expect(screen.getByRole('table', { name: 'Live event feed' })).toHaveTextContent('cart.item.added');
expect(screen.getByRole('heading', { name: 'Five-minute windows' })).toBeVisible();
expect(screen.getByRole('article', { name: 'Today’s revenue' })).toHaveTextContent('Rp');
expect(screen.getByRole('heading', { name: 'CEP pattern signals' })).toBeVisible();
expect(screen.getByRole('table', { name: 'Trending products' })).not.toHaveTextContent('Buyer');
```

Also assert the Level 3 cards expose Abandoned carts, Slow delivery, Trending products, Order surge, and Checkout to delivery.

- [ ] **Step 2: Run page tests RED**

Run: `npm run test:ui -- src/pages/dashboard/DashboardPages.test.tsx`
Expected: FAIL because the pages are still stubs or missing.

- [ ] **Step 3: Implement Level 1**

Render a semantic table with WIB time, Event, Actor, and Payload headers. Preserve newest-first order and the existing 100-event cap. Put overflow on `.event-feed__scroller`, not the document. Format payload JSON for scanning and expose the untruncated value through an accessible `<details>` interaction on narrow widths.

- [ ] **Step 4: Implement Level 2**

Use the exact ordered metric definition:

```ts
export const METRICS = [
  { name: 'listings_count', label: 'Listings', window: true, daily: false },
  { name: 'cart_adds_count', label: 'Cart adds', window: true, daily: false },
  { name: 'tx_count', label: 'Checkouts', window: true, daily: true },
  { name: 'confirmed_orders', label: 'Confirmed', window: true, daily: false },
  { name: 'delivered_orders', label: 'Delivered', window: true, daily: true },
  { name: 'top_product', label: 'Top product', window: true, daily: false },
  { name: 'revenue', label: 'Today’s revenue', window: false, daily: true, rupiah: true },
] as const;
```

At projector width render four cards on row one; row two contains Delivered, Top product, and a two-column Revenue card. Preserve Current 5 min and Today values where defined. Keep `MetricBarChart`'s 24 fixed x slots, pointer/focus tooltip, WIB ranges, and `ResizeObserver`; only adjust CSS/viewBox sizing for presentation legibility.

- [ ] **Step 5: Implement Level 3**

Extract the existing alert count and duration renderers without changing selectors. Keep representations exact: two ten-minute count histories, product/count-only table, detected/not-detected surge state, and elapsed seconds per completed order.

- [ ] **Step 6: Add projector and narrow responsive CSS**

At 1600×900+, ensure `.dashboard-page` and its grids fit the available shell height, with event-feed internal overflow. At smaller sizes, remove fixed heights and use one-column cards at phone width. Do not hide data columns solely to fit mobile; use responsive details/card treatment.

- [ ] **Step 7: Run focused and full frontend gates GREEN**

```bash
npm run test:ui -- src/pages/dashboard/DashboardPages.test.tsx
npm test
npm run test:ui
npm run lint
npm run build
```

- [ ] **Step 8: Perform browser visual probe before commit**

Run Vite with the Go API proxy and use a headless or visible Chromium/Edge session to capture:

- 1920×1080 screenshots of all three routes with `document.documentElement.scrollHeight === window.innerHeight`;
- 1440px screenshots with readable rail/stepper;
- 375px screenshots with no horizontal overflow;
- one pointer and one keyboard-focus D3 tooltip assertion.

Save temporary screenshots outside tracked source or under ignored `.superpowers/`; do not commit them.

- [ ] **Step 9: Commit**

```bash
git add web/src/dashboard web/src/pages/dashboard web/src/components/MetricBarChart.tsx web/src/styles
git commit -m "feat: compose projector dashboard levels"
```

---

### Task 5: Mobile Audience Entry and Shared Role Shell

**Files:**
- Create: `web/src/components/RoleLayout.tsx`
- Create: `web/src/components/ActionCard.tsx`
- Create: `web/src/lib/feedback.ts`
- Test: `web/src/lib/feedback.test.ts`
- Test: `web/src/pages/Landing.test.tsx`
- Modify: `web/src/pages/Landing.tsx`
- Modify: `web/src/pages/Buyer.tsx`
- Modify: `web/src/pages/Seller.tsx`
- Modify: `web/src/pages/Shipper.tsx`
- Modify: `web/src/styles/base.css`

**Interfaces:**
- Consumes: Task 1 primitives and existing `useSession`/`createSession` behavior.
- Produces: `RoleLayout`, `ActionCard`, and deterministic three-second action feedback used by Tasks 6–8.

- [ ] **Step 1: Write feedback-state tests RED**

```ts
test('new feedback replaces prior feedback and expires only its own generation', () => {
  const first = createFeedback('success', 'Product added');
  const second = createFeedback('success', 'Order confirmed');
  assert.equal(expireFeedback(second, first.id), second);
  assert.equal(expireFeedback(second, second.id), null);
});
```

Run: `node --test src/lib/feedback.test.ts`
Expected: FAIL because `feedback.ts` is missing.

- [ ] **Step 2: Implement feedback helper GREEN**

```ts
export interface ActionFeedback { id: string; tone: 'success' | 'error'; message: string }
export function createFeedback(tone: ActionFeedback['tone'], message: string): ActionFeedback;
export function expireFeedback(current: ActionFeedback | null, id: string): ActionFeedback | null;
```

Use `crypto.randomUUID()` for IDs. Components schedule the three-second expiration and verify the current ID before clearing.

- [ ] **Step 3: Write Landing tests RED**

Test that the name input precedes three equally weighted role buttons, the role descriptions match the spec, dashboard is a secondary link, and missing-name submission focuses the input with exact text “Enter your display name to continue.” Mock `createSession` and verify successful Buyer selection navigates to `/buyer`.

- [ ] **Step 4: Implement RoleLayout and Landing GREEN**

`RoleLayout` public interface:

```tsx
export interface RoleLayoutProps {
  roleLabel: 'Shop' | 'Sell' | 'Deliver';
  participantName: string;
  pulseKey: string;
  onLogout: () => void;
  children: React.ReactNode;
}
```

It renders one mobile header, identity overflow menu, logout action, SignalTrace, and responsive main container. Replace inline page-shell styles in Buyer/Seller/Shipper with `RoleLayout`, but defer their internal content changes to later tasks.

Landing uses a semantic form plus three role buttons. Do not add backend role assignment.

- [ ] **Step 5: Add mobile/desktop layout CSS and run GREEN**

Run:

```bash
node --test src/lib/feedback.test.ts
npm run test:ui -- src/pages/Landing.test.tsx
npm test
npm run test:ui
npm run lint
npm run build
```

- [ ] **Step 6: Browser-check landing at 320px, 375px, 768px, and 1440px**

Verify no horizontal overflow, every role target is at least 44px high, error focus returns to name, and the presenter link is visually secondary.

- [ ] **Step 7: Commit**

```bash
git add web/src/components/RoleLayout.tsx web/src/components/ActionCard.tsx web/src/lib/feedback.ts web/src/lib/feedback.test.ts web/src/pages/Landing.tsx web/src/pages/Landing.test.tsx web/src/pages/Buyer.tsx web/src/pages/Seller.tsx web/src/pages/Shipper.tsx web/src/styles
git commit -m "feat: redesign mobile audience entry"
```

---

### Task 6: Buyer Catalog and Accessible Cart Sheet

**Files:**
- Create: `web/src/lib/focusTrap.ts`
- Test: `web/src/lib/focusTrap.test.ts`
- Create: `web/src/components/CartSheet.tsx`
- Test: `web/src/components/CartSheet.test.tsx`
- Modify: `web/src/pages/Buyer.tsx`
- Test: `web/src/pages/Buyer.test.tsx`
- Modify: `web/src/styles/base.css`

**Interfaces:**
- Consumes: `RoleLayout`, shared primitives, existing cart helpers and API calls.
- Produces: `CartSheet` with mobile dialog and desktop panel behavior.

- [ ] **Step 1: Write focus-cycle tests RED**

```ts
test('wraps focus from last to first and first to last', () => {
  assert.equal(nextFocusIndex(2, 3, false), 0);
  assert.equal(nextFocusIndex(0, 3, true), 2);
});
```

Run: `node --test src/lib/focusTrap.test.ts`
Expected: FAIL because `focusTrap.ts` is missing.

- [ ] **Step 2: Implement focus helpers GREEN**

```ts
export const FOCUSABLE_SELECTOR = 'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
export function nextFocusIndex(current: number, length: number, backwards: boolean): number;
```

- [ ] **Step 3: Write CartSheet interaction tests RED**

Test open focus on the close button or heading, Tab and Shift+Tab cycling, Escape close, explicit close, focus restoration to the trigger, quantities/line totals, disabled Place order without an address, and submitted callback payload. Closing and reopening must retain the address draft; successful submission clears it through parent state.

- [ ] **Step 4: Implement CartSheet GREEN**

Use:

```tsx
export interface CartSheetProps {
  open: boolean;
  items: CartItemView[];
  total: number;
  address: string;
  submitting: boolean;
  returnFocusRef: React.RefObject<HTMLButtonElement | null>;
  onAddressChange: (value: string) => void;
  onClose: () => void;
  onPlaceOrder: () => void;
}
```

Render `role="dialog"`, `aria-modal="true"` on mobile, labelled heading, backdrop close, explicit close control, and the tested focus trap. CSS renders it full-height from the bottom on mobile and as an in-flow/right-side panel at 960px+.

- [ ] **Step 5: Write Buyer page tests RED**

Mock API/session hooks and assert catalog first, two-column-capable product cards, Add to cart action feedback, sticky Review cart summary using total item quantities, recent orders below catalog, and lifecycle badges.

- [ ] **Step 6: Refactor Buyer without changing domain behavior**

Preserve `cartId`, quantity aggregation, API payloads, `cartItemCount`, UUID order-event filtering, and order reload behavior. Replace inline styles with classes/components. On successful checkout: clear cart, create a new cart UUID, clear address, close sheet, show “Order placed,” and reload orders.

- [ ] **Step 7: Run focused and full gates GREEN**

```bash
node --test src/lib/focusTrap.test.ts
npm run test:ui -- src/components/CartSheet.test.tsx src/pages/Buyer.test.tsx
npm test
npm run test:ui
npm run lint
npm run build
```

- [ ] **Step 8: Browser-check buyer touch and keyboard flow**

At 375px verify sticky cart respects safe-area inset, product controls are 44px, cart sheet has no background focus leakage, address survives close/reopen, and no horizontal overflow. At 960px+ verify the checkout panel does not cover the catalog.

- [ ] **Step 9: Commit**

```bash
git add web/src/lib/focusTrap.ts web/src/lib/focusTrap.test.ts web/src/components/CartSheet.tsx web/src/components/CartSheet.test.tsx web/src/pages/Buyer.tsx web/src/pages/Buyer.test.tsx web/src/styles
git commit -m "feat: redesign buyer shopping flow"
```

---

### Task 7: Seller Action-First Workbench

**Files:**
- Modify: `web/src/pages/Seller.tsx`
- Test: `web/src/pages/Seller.test.tsx`
- Modify: `web/src/styles/base.css`

**Interfaces:**
- Consumes: `RoleLayout`, `ActionCard`, feedback primitives, existing `loadLatestSellerOrders`, product APIs, and UUID event predicate.
- Produces: mobile seller flow with exact approved action vocabulary.

- [ ] **Step 1: Write Seller tests RED**

Mock session/API/WebSocket inputs and assert:

```tsx
expect(screen.getByRole('heading', { name: 'Add a product' })).toBeVisible();
expect(screen.getByRole('button', { name: 'Add product' })).toBeEnabled();
expect(screen.queryByRole('button', { name: 'List product' })).not.toBeInTheDocument();
expect(screen.getByRole('heading', { name: 'Orders to confirm' })).toBeVisible();
expect(screen.getByRole('heading', { name: 'Your products' })).toBeVisible();
```

Also test validation focus, Adding… state, Product added feedback, Confirming… state, Order confirmed feedback, and that only checkout orders have Confirm order buttons.

- [ ] **Step 2: Run Seller tests RED**

Run: `npm run test:ui -- src/pages/Seller.test.tsx`
Expected: FAIL against the old inline layout and copy.

- [ ] **Step 3: Refactor Seller GREEN**

Use one compact ActionCard for fields, put price and stock side-by-side where width permits, then render actionable checkout orders before inventory. Keep confirmed/picked/delivered orders visible with `StatusBadge` and no action. Preserve the existing latest-generation order guard and UUID event predicate exactly.

Validation copy remains specific. Focus the first invalid field. Catch transport failures and show an inline error without clearing existing products/orders.

- [ ] **Step 4: Run focused and full gates GREEN**

```bash
npm run test:ui -- src/pages/Seller.test.tsx
npm test
npm run test:ui
npm run lint
npm run build
```

- [ ] **Step 5: Browser-check Seller at 320px, 375px, 768px, and 1440px**

Verify fields remain labelled, targets are 44px, new orders precede inventory, status changes highlight without moving buttons, and desktop becomes a workbench at 960px+.

- [ ] **Step 6: Commit**

```bash
git add web/src/pages/Seller.tsx web/src/pages/Seller.test.tsx web/src/styles
git commit -m "feat: redesign seller order workbench"
```

---

### Task 8: Shipper Active-Delivery Flow and History

**Files:**
- Modify: `web/src/pages/Shipper.tsx`
- Test: `web/src/pages/Shipper.test.tsx`
- Modify: `web/src/lib/deliveries.ts`
- Test: `web/src/lib/deliveries.test.ts`
- Modify: `web/src/styles/base.css`

**Interfaces:**
- Consumes: `RoleLayout`, `ActionCard`, feedback primitives, existing reload-safe `loadLatestShipperSnapshot`, readiness APIs, and delivery-copy helpers.
- Produces: active-first shipper UI with explicit invalid-readiness state.

- [ ] **Step 1: Add readiness-label tests RED**

Extend `deliveries.test.ts`:

```ts
test('describes missing or invalid readiness without claiming zero seconds', () => {
  assert.deepEqual(deliveryReadiness(undefined, new Date()), { kind: 'unavailable', label: 'Readiness unavailable' });
  assert.deepEqual(deliveryReadiness('invalid', new Date()), { kind: 'unavailable', label: 'Readiness unavailable' });
});
```

Also assert future readiness returns `{ kind: 'waiting', seconds, label: 'Ready in Ns' }` and elapsed readiness returns `{ kind: 'ready', label: 'Ready to deliver' }`.

- [ ] **Step 2: Implement readiness model GREEN**

```ts
export type DeliveryReadiness =
  | { kind: 'unavailable'; label: 'Readiness unavailable' }
  | { kind: 'waiting'; label: string; seconds: number }
  | { kind: 'ready'; label: 'Ready to deliver' };

export function deliveryReadiness(readyAt: string | undefined, now: Date): DeliveryReadiness;
```

Keep `secondsUntilReady` if existing consumers/tests still need it, implemented through the new parser where practical.

- [ ] **Step 3: Write Shipper page tests RED**

Assert active deliveries render before available jobs and history, countdown text is not color-only, invalid readiness disables Mark delivered with “Readiness unavailable,” early jobs remain disabled, ready jobs enable Mark delivered, and API failures retain existing cards while showing an alert.

Test Pick up job → Picking up… → Job picked up and Mark delivered → Delivering… → Delivery completed.

- [ ] **Step 4: Refactor Shipper GREEN**

Use the strongest Ink card for the current active delivery, with Brass only for the countdown label. Available jobs use secondary outlined actions. History uses compact completed cards/definition lists. Preserve owner-only data, server readiness enforcement, latest-generation refresh behavior, sort order, and UUID event filters.

- [ ] **Step 5: Run focused and full gates GREEN**

```bash
node --test src/lib/deliveries.test.ts
npm run test:ui -- src/pages/Shipper.test.tsx
npm test
npm run test:ui
npm run lint
npm run build
```

- [ ] **Step 6: Browser-check countdown and reload flow**

At 375px: pick a job, reload, confirm the same `ready_at` countdown resumes, wait until ready, deliver, and confirm history appears. Verify active action remains above jobs and history, no horizontal overflow, and every action is 44px.

- [ ] **Step 7: Commit**

```bash
git add web/src/pages/Shipper.tsx web/src/pages/Shipper.test.tsx web/src/lib/deliveries.ts web/src/lib/deliveries.test.ts web/src/styles
git commit -m "feat: redesign shipper delivery flow"
```

---

### Task 9: Cross-Page Accessibility, Embedded Bundle, and Live Verification

**Files:**
- Modify: `web/src/styles/base.css` only for findings proven by final browser checks
- Modify: `app/web/dist/index.html`
- Replace generated: `app/web/dist/assets/index-*.css`
- Replace generated: `app/web/dist/assets/index-*.js`
- Test: existing frontend, Go, and Flink suites

**Interfaces:**
- Consumes: all prior tasks.
- Produces: reviewed production bundle embedded by Go and a clean, live 12-job demo stack.

- [ ] **Step 1: Run fresh complete frontend gates**

Use pinned Node 24:

```bash
npm ci
npm test
npm run test:ui
npm run lint
npm run build
```

Expected: all Node and Vitest tests pass, lint has zero errors, TypeScript passes, and Vite writes the new bundle to `app/web/dist`.

- [ ] **Step 2: Run fresh Go and Java gates**

Use Go 1.23 and Java 11/Maven 3.9:

```bash
cd app && go test ./... -race
cd ../flink && mvn clean verify
```

Expected: Go race suite PASS and Maven reports all tests with zero failures/errors.

- [ ] **Step 3: Verify generated bundle coherence**

Confirm `app/web/dist/index.html` references the newly generated hashed CSS/JS files, old hashes are removed, bundled output contains the new dashboard routes and “Add product,” and it does not contain “List product.” Run `git diff --check`.

- [ ] **Step 4: Start a clean local stack**

After resolving the exact Compose project and obtaining approval before deleting volumes:

```bash
docker compose -p flink-demo down -v
docker compose -p flink-demo up -d --build
```

Poll conditions rather than sleeping blindly: app `/api/health` is `ok`, Kafka/ZooKeeper/JobManager are healthy, TaskManager is running, submitter exits zero, and Flink reports exactly 12 RUNNING jobs.

- [ ] **Step 5: Run the complete mobile role browser matrix**

With real API actions, verify at 375×812 and 768×1024:

- landing self-service role selection and name-error focus;
- buyer catalog, quantity-based sticky cart, cart-sheet focus/Escape/restoration, checkout, and lifecycle updates;
- seller Add product copy and feedback, new-order priority, confirmation, and delivered status refresh;
- shipper active-first countdown, reload persistence, delivery, and history;
- no horizontal overflow and at least 44px action targets.

- [ ] **Step 6: Run the complete dashboard browser matrix**

Open a fresh dashboard session and verify:

- `/dashboard` redirects to `/dashboard/live`;
- navigation among levels keeps one dashboard WebSocket/token and accumulated data;
- Level 1 deduplicates repeated event IDs and caps at 100;
- Level 2 retains 24 fixed slots, same-window replacement, WIB tooltip/focus, zero boundary bucket, and equal bar widths;
- Level 3 shows all five approved pattern representations;
- Clear confirmation cancels safely and confirmed Clear resets all levels;
- reload on any level resets all levels;
- 1920×1080 pages have no document scroll;
- 1440px and 375px layouts have no horizontal overflow.

Capture and visually inspect screenshots for all pages at their required widths. Store them under ignored `.superpowers/` and do not commit them.

- [ ] **Step 7: Fix only verified visual/accessibility defects and rerun affected gates**

For every discovered defect, add a failing automated assertion when practical, make the smallest CSS/component correction, rerun its focused test, then rerun Steps 1–3. Do not add unapproved features.

- [ ] **Step 8: Clean generated test artifacts and review repository hygiene**

Restore/remove only verified generated Maven artifacts under `flink/target`. Preserve `.superpowers/brainstorm/`. Confirm `git status --short` contains only intended `app/web/dist` changes and any source/test correction from Step 7.

- [ ] **Step 9: Commit the embedded bundle**

```bash
git add app/web/dist web/src web/package.json web/package-lock.json web/vitest.config.ts Makefile
git commit -m "build: embed responsive demo UI"
```

- [ ] **Step 10: Request final whole-branch review**

Review the implementation range against `docs/superpowers/specs/2026-08-15-responsive-demo-ui-design.md`. Fix all Critical and Important findings with fresh failing tests and scoped re-review. Record Minor findings explicitly for user disposition.

---

## Completion Gate

The branch is ready for integration only when:

- all existing Node tests and new Vitest component tests pass;
- lint has zero errors and the production build succeeds;
- Go race and Java Maven suites pass;
- generated embedded assets match source;
- real mobile buyer/seller/shipper flows pass;
- all three dashboard routes pass projector and responsive checks;
- exactly 12 Flink jobs are RUNNING;
- independent review has no unresolved Critical or Important findings;
- the worktree is clean except ignored `.superpowers/` artifacts.
