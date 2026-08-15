# Responsive Demo UI Redesign

**Date:** 2026-08-15  
**Status:** Approved design, pending written-spec review

## Purpose

Redesign the existing e-commerce stream-processing demo so it feels coherent, deliberate, and easy to use while preserving its current backend behavior.

The product serves two related audiences:

- Audience participants use the landing, buyer, seller, and shipper pages from their phones.
- The presenter shows the dashboard on a 16:9 projector, usually at 1920×1080.

The role pages must feel like a credible commerce product. The dashboard must explain the three processing levels as a clear presentation sequence. The redesign is frontend-only: APIs, authentication semantics, Kafka events, Flink jobs, window behavior, CEP definitions, and ownership rules do not change.

## Design Direction

The approved direction is **Market Signal**: a commerce interface shaped by live operational data.

### Core palette

| Token | Value | Use |
|---|---:|---|
| Ink | `#10243C` | Primary actions, headings, high-contrast text |
| Deep ink | `#142B46` | Dashboard rail and projector header |
| Canvas | `#EDF3F7` | Page background |
| Surface | `#FFFFFF` | Forms, cards, sheets, tables |
| Steel | `#54708B` | Secondary text and chart context |
| Brass | `#F2BD58` | Small live-state and exceptional-signal accents |

Success, warning, and error colors are separate semantic tokens with WCAG AA contrast. Brass is not a general button background. When brass contains text, the text is Ink; gray or white text on brass is prohibited.

Semantic colors are Success `#16775A`, Warning `#8A5A00`, and Error `#B42318`. Text, icons, borders, and backgrounds must use combinations that pass WCAG AA; semantic color is never the only status cue.

### Typography

- **Sora** for restrained display headings.
- **IBM Plex Sans** for interface labels, controls, and body copy.
- **IBM Plex Mono** for timestamps, event types, identifiers, and chart utilities.

Fonts must be bundled with the frontend build rather than fetched at runtime so the local Docker demo remains self-contained.

### Signature element

A thin signal trace appears in the shared header or dashboard step rail. It performs one brief pulse when a new event arrives. It is the design's single expressive gesture. It must not obscure content, cause layout movement, or run when `prefers-reduced-motion: reduce` is active.

## Information Architecture

### Public and role routes

- `/` — audience entry and self-service role selection
- `/buyer` — catalog, cart and checkout, recent orders
- `/seller` — product creation, actionable orders, product inventory
- `/shipper` — active deliveries, available jobs, delivery history

Audience members enter a display name and choose any role. There is no assignment feature in the backend. Buyer, seller, and shipper options therefore receive equal visual weight. The presenter dashboard is a visually separate secondary link.

### Dashboard routes

- `/dashboard` redirects to `/dashboard/live`
- `/dashboard/live` — Level 1, raw event feed
- `/dashboard/windows` — Level 2, five-minute and daily metrics
- `/dashboard/patterns` — Level 3, CEP pattern signals

The dashboard uses separate URLs rather than tabs or one long page. A persistent numbered rail communicates the presentation sequence and allows direct navigation during questions. Previous and next controls support the normal presentation path.

## State and Data Flow

A dashboard state provider sits above the three nested dashboard routes. It owns:

- the fresh dashboard session token;
- the single dashboard WebSocket connection;
- deduplicated raw events;
- window and daily metrics;
- immutable CEP alerts;
- Jakarta-day and dashboard-session timing.

Client-side navigation among the three levels preserves all accumulated dashboard state and does not reconnect the WebSocket.

A browser reload on any dashboard level creates a fresh dashboard session and resets all three levels. The backend and Flink jobs continue running; only that browser's accumulated dashboard view resets. The dashboard Clear action has the same all-level reset scope and requires a lightweight confirmation.

The role pages retain their current UUID-based sessions, WebSocket filtering, refresh generation guards, server-owned readiness, and API behavior. Presentation components may be extracted, but business logic must not be rewritten merely for styling.

## Shared Component Boundaries

### Dashboard components

- `DashboardLayout`: projector header, connection state, live clock, numbered rail or responsive stepper, previous/next controls, and clear confirmation.
- `DashboardProvider`: dashboard token, WebSocket, accumulated data, reset action, and derived level-specific selectors.
- `DashboardLivePage`: Level 1 title, explanation, bounded event table, and feed empty state.
- `DashboardWindowsPage`: Level 2 metric composition using the existing D3 bucket charts and daily values.
- `DashboardPatternsPage`: Level 3 pattern-specific visualizations.

Each page consumes derived state through the provider and does not open its own connection.

### Role components

- `RoleLayout`: page header, role identity, overflow menu, logout, responsive content container, and live signal trace.
- `ActionCard`: primary task container used for product creation, order confirmation, and delivery actions.
- `StatusBadge`: consistent checkout, confirmed, picked, delivered, ready, and error states.
- `FeedbackBanner`: persistent inline success or error feedback near the affected section.
- `EmptyState`: plain-language explanation of what action or upstream event will populate a section.
- `CartSheet`: accessible buyer checkout sheet on mobile and right-side panel on desktop.

Components expose presentation concerns only. Existing data helpers remain the source of identity and lifecycle decisions.

## Page Designs

### Landing

The landing page introduces the demo in one sentence: participant actions become live stream events. It presents:

1. one display-name field;
2. three equal role cards with clear outcomes;
3. a secondary presenter-dashboard link.

Role copy:

- **Shop as buyer** — Browse, add, and place an order.
- **Sell products** — Add products and confirm orders.
- **Deliver orders** — Pick jobs and complete delivery.

Selecting a role without a name keeps focus on the name field and displays a precise inline error.

### Buyer

Mobile hierarchy:

1. catalog;
2. sticky cart summary and Review cart action;
3. recent orders with live lifecycle status.

Product cards use a compact two-column grid where width permits and one column on very narrow devices. Add-to-cart feedback is immediate and does not move surrounding controls.

Review cart opens a full-height mobile sheet containing quantities, line totals, address entry, order total, and Place order. The catalog remains underneath. On desktop, the same component becomes a right-side panel. The sheet traps focus, closes with Escape and an explicit close control, restores focus to Review cart, and warns before dismissing only if entered checkout data would be lost.

### Seller

Mobile hierarchy:

1. compact Add a product card;
2. orders requiring confirmation, with new-count emphasis;
3. product inventory.

The product action vocabulary is:

- Add product
- Adding…
- Product added

“List product” is prohibited because it can mean retrieving a list. “Your products” names the existing inventory section.

Checkout orders remain actionable. Confirmed, picked, and delivered orders remain visible with status badges but do not retain a confirmation button.

### Shipper

Mobile hierarchy:

1. active delivery, countdown, and delivery action;
2. available jobs;
3. completed-delivery history.

An active delivery uses the strongest card treatment because it is the next task. The readiness countdown is readable without relying on color. Mark delivered remains disabled until a valid `ready_at` has elapsed. Invalid or missing readiness data shows “Readiness unavailable,” not “Ready in 0s.”

History remains reload-safe and shows destination, products, picked time, delivered time, and elapsed duration.

## Dashboard Presentation Design

At 1920×1080, each level fits within one viewport without document scrolling. The header and numbered rail stay fixed. If a component contains more data than fits, only that component scrolls.

### Level 1 — Live events

The page explains what enters the stream and displays a bounded event table with:

- WIB time;
- event type;
- actor display name with identifier available as secondary detail;
- formatted payload.

The table is newest-first and internally scrollable. Long payloads truncate visually but remain available through expansion, title text, or an accessible detail interaction. The row limit and existing event-ID deduplication remain unchanged.

### Level 2 — Window metrics

The page explains what the stream remembers. The approved composition is four cards on the first row and three positions on the second:

- Listings
- Cart adds
- Checkouts
- Confirmed
- Delivered
- Top product
- Today's revenue, occupying two card columns

Each window metric preserves the existing fixed 24-slot D3 grid, aligned five-minute WIB ranges, same-window replacement, zero-filled elapsed windows, and hover/focus values. Revenue remains daily cumulative. Projector labels must be legible at viewing distance.

### Level 3 — Pattern signals

The page explains what the stream recognizes. Each pattern keeps its distinct approved representation:

- Abandoned carts: ten-minute count history
- Slow delivery: ten-minute count history
- Trending products: product and count table, with no buyer column
- Order surge: detected/not-detected state
- Checkout to delivery: elapsed seconds per completed order

Immutable alert facts remain retained in browser memory for eight hours or until Clear/reload. The page does not imply that users react to or manage alerts.

## Responsive Rules

### Role pages

- Mobile-first from 320px upward.
- Minimum interactive target size is 44×44px.
- Single-column task priority is the default.
- Sticky actions account for safe-area insets.
- Tables become cards or definition lists; horizontal page scrolling is prohibited.
- At 768px and wider, product grids may add columns. At 960px and wider, buyer checkout becomes a right-side panel and seller/shipper task groups may form a two-column workbench. Breakpoint changes do not introduce a separate information architecture.

### Dashboard

- Default composition targets a 16:9 projector at 1920×1080.
- At 1280px and wider, the numbered rail is vertical.
- Below 1280px, the rail becomes a horizontal top stepper.
- The fixed one-viewport presentation composition applies only at a viewport of at least 1600×900. Below either dimension, fixed presentation heights are removed and normal vertical scrolling returns.
- Event and chart content must remain usable at 375px, even though projector use is primary.

## Interaction and Copy Rules

Actions keep the same verb through loading and completion:

- Add product → Adding… → Product added
- Confirm order → Confirming… → Order confirmed
- Add to cart → Adding… → Added to cart
- Place order → Placing order… → Order placed
- Pick up job → Picking up… → Job picked up
- Mark delivered → Delivering… → Delivery completed

Direct action outcomes use a compact `role="status"` feedback strip near the affected section. Success feedback remains for at least three seconds and is also reflected by the durable UI change. No toast library is introduced. Background WebSocket updates do not create feedback strips. Instead, updated rows or statuses receive a brief, non-layout-shifting highlight.

Loading disables only the relevant action rather than the entire page. Controls must not jump position when labels change.

## Error, Empty, and Connection States

- Errors appear in persistent inline banners near the affected section.
- Refresh failures retain already rendered data.
- Stale asynchronous failures must not replace newer success state or show obsolete errors.
- Empty states explain the upstream action that creates data.
- The dashboard header distinguishes Connecting, Reconnecting, and Live.
- A disconnected dashboard preserves accumulated data.
- Clear requires confirmation and explains that all three dashboard levels reset.

Examples:

- Buyer catalog empty: “No products yet. Ask a seller to add one.”
- Seller orders empty: “No orders to confirm. New checkouts will appear here.”
- Shipper jobs empty: “No delivery jobs yet. Jobs appear after a seller confirms an order.”
- Level 1 empty: “Waiting for the first audience action.”

## Accessibility

- WCAG AA contrast for text and interactive controls.
- Visible `:focus-visible` treatment on every control.
- Semantic headings and landmarks.
- Accessible names for icon-only controls.
- Color is never the only carrier of status.
- Cart sheet has correct dialog semantics, focus trapping, Escape behavior, and focus restoration.
- D3 chart values are available through pointer hover and keyboard focus.
- Live updates use restrained announcements; the raw feed does not continuously overwhelm assistive technology.
- All nonessential motion respects reduced-motion preferences.

## Verification Strategy

Automated coverage must include:

- `/dashboard` redirect and all three level routes;
- previous, next, and direct level navigation;
- one dashboard token and WebSocket across client-side level navigation;
- state preservation across level navigation;
- reload and Clear resetting all dashboard levels;
- cart sheet open, close, focus trap, Escape, focus restoration, and checkout behavior;
- role-page loading, success, error, and empty states;
- existing UUID event filtering and stale-refresh guards;
- accessibility names and keyboard behavior for new controls;
- a stylesheet/browser assertion that reduced-motion mode removes the signal-trace pulse and update-highlight animation;
- current metric bucket, CEP, session, and lifecycle unit suites.

Browser verification must cover:

- landing, buyer, seller, and shipper at 375px and 768px;
- dashboard levels at 375px, 1440px, and 1920×1080;
- no horizontal page overflow;
- 44px mobile targets;
- projector pages fitting without document scroll at 1920×1080;
- D3 hover and keyboard-focus tooltips;
- cart sheet keyboard and touch flow;
- representative loading, empty, error, active, and completed states;
- contrast and visual hierarchy under the final bundled fonts.

The final production frontend bundle must be regenerated into `app/web/dist`, and the Go embedding tests must pass.

## Non-goals

- Backend or event-schema changes
- Role assignment or invitations
- Persisting dashboard history across reloads
- Replacing D3 or changing metric/CEP definitions
- Product imagery upload or media storage
- Order cancellation, cart quantity editing, or inventory management beyond existing behavior
- A separate native-mobile application

## Success Criteria

The redesign succeeds when:

1. a participant can join and complete the relevant role flow comfortably on a phone;
2. each role page presents its next task before secondary history or inventory;
3. the presenter can move through three distinct dashboard URLs without losing live state;
4. reload or Clear predictably resets the entire dashboard session;
5. each dashboard level fits a 1080p presentation frame without document scrolling;
6. typography, color, copy, feedback, and interaction feel like one product across every page;
7. existing stream-processing behavior and tests remain intact.
