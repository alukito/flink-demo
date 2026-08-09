# UUID Identity, Live Order Lifecycle, and Session Timeline Design

**Date:** 2026-08-09
**Status:** Approved for implementation planning

## Objective

Resolve four related demo issues without introducing a database:

1. Human-entered buyer, seller, and shipper names must not serve as unique identities.
2. Level 2 charts must begin with the current dashboard session instead of showing empty historical windows.
3. A seller's order inbox must update through pickup and delivery without a page reload.
4. A shipper must retain active delivery state across reloads and see completed delivery history.

The application remains an in-memory demo. Restarting the application clears sessions, products, orders, and delivery history.

## Chosen Approach

Use server-owned UUID identity and server-owned order lifecycle state. Names are non-unique display labels. JWT claims, ownership checks, WebSocket targeting, and event relationships use UUIDs. The frontend renders names while using UUIDs for comparisons.

This retains the current lightweight architecture while fixing the identity and reload boundaries at their source. Client-generated identity, browser-owned delivery history, polling, and a persistent database are out of scope.

## Identity Model

### Session creation

`POST /api/session` accepts the existing `name` and `role` fields. It generates a new UUID for every successful request and returns:

```json
{
  "id": "user-uuid",
  "name": "display name",
  "role": "buyer|seller|shipper|dashboard",
  "token": "signed JWT"
}
```

Duplicate display names are allowed, including two sessions with the same role and name. The in-memory session store is keyed by UUID rather than name.

JWT claims carry the UUID, name, and role. Authenticated handlers obtain all three from verified claims. The frontend stores all four session response fields in tab-scoped session storage. Role headers continue to show the display name.

### Domain ownership

All ownership and authorization comparisons use UUIDs:

- products use `seller_id`,
- orders use `buyer_id` and `seller_id`,
- pickups use `picked_by`,
- WebSocket clients are targeted by UUID,
- event relationships use UUID-valued `*_id` fields.

Products and orders also store immutable name snapshots for display:

- `seller_name`,
- `buyer_name`,
- `picked_by_name`.

Name snapshots avoid adding a user-directory lookup and remain unchanged for the lifetime of the in-memory entity. Profile editing is not part of this demo.

## Event Schema and Routing

The raw event envelope uses the session UUID in `actor_id` and adds `actor_name`. Relationship payloads carry both identity and display fields where applicable:

- `buyer_id` and `buyer_name`,
- `seller_id` and `seller_name`,
- `shipper_id` and `shipper_name`.

Existing correlation identifiers remain unchanged: product patterns use product UUIDs, cart patterns use cart UUIDs, and order patterns use order UUIDs. Flink aggregation and CEP behavior must therefore remain semantically unchanged after parsing the enriched events.

The WebSocket hub compares client UUIDs with event UUID fields. Routing by display name is removed.

Lifecycle routing is:

- `cart.checkout`: matching buyer and seller,
- `order.confirmed`: matching buyer and seller, plus all shippers for available-job refresh,
- `shipment.picked`: matching buyer and seller, plus all shippers so claimed work disappears,
- `shipment.delivered`: matching buyer and seller, plus the shipper who owns the delivery.

The dashboard continues receiving every raw event.

## Seller Order Inbox

The order store remains the source of truth. WebSocket events are invalidation notifications rather than replacement order records.

The seller page reloads its order list when it receives a relevant:

- `cart.checkout`,
- `shipment.picked`,
- `shipment.delivered`.

Events are relevant when their `seller_id` equals the seller session UUID. Confirmation already reloads after a successful API response.

The inbox consequently transitions live through:

```text
checkout -> confirmed -> picked -> delivered
```

Pickup and delivery events must include the seller UUID and name so the hub can route them and the seller can validate relevance. No polling is introduced.

## Shipper Delivery Lifecycle

### Server-owned pickup readiness

Picking an order records:

- `picked_by` UUID,
- `picked_by_name`,
- `picked_at`,
- `ready_at`, set to a server-selected delay of 5–15 seconds after pickup.

The server is authoritative for readiness. Delivery succeeds only when:

1. the order is in `picked` status,
2. the authenticated shipper UUID equals `picked_by`,
3. server time is at or after `ready_at`.

An early delivery returns HTTP `409 Conflict`. A different shipper attempting delivery returns HTTP `403 Forbidden`. Existing transition conflicts remain `409 Conflict`.

The order store must expose time and delay seams so tests can use a deterministic clock and delay rather than sleeps or random outcomes.

### Shipper APIs

`GET /api/shipper/jobs` continues returning available confirmed orders.

Add `GET /api/shipper/deliveries` for the authenticated shipper. It returns:

```json
{
  "active": [],
  "history": []
}
```

`active` contains orders picked by this shipper that are not delivered. `history` contains this shipper's delivered orders, newest delivery first. Orders include the identity display snapshots, items, destination, lifecycle timestamps, and status.

### Shipper page

The shipper page removes the browser-owned `pickedOrders` map. It loads available jobs and the current shipper's deliveries from the server.

The page renders:

- **Available Jobs** for unclaimed confirmed orders,
- **My Active Deliveries** for server-owned picked orders,
- **My Delivery History** for completed orders.

The active countdown is derived from `ready_at - current browser time` and updates once per second. It is presentation only; the server still enforces readiness. Reloading or reconnecting reconstructs the same countdown and ownership from the API.

After successful delivery, the page refreshes delivery state so the order moves immediately from active deliveries into history. History shows buyer name, destination, items, pickup time, delivery time, and elapsed time.

## Level 2 Dashboard Timeline

### Session boundary

When the dashboard mounts, it captures the current aligned five-minute window as its visible session start. For example, opening at 14:02 WIB starts with the `14:00–14:05` window.

Only windows from that captured start through the current aligned window are considered elapsed and eligible for labels or values. Cached snapshots for earlier windows may arrive from the WebSocket hub but remain hidden.

At every five-minute boundary, the chart appends the new aligned window with value zero. Subsequent Flink snapshots for that window replace the zero value. The existing five-second dashboard clock continues updating the current window and detecting new boundaries.

Reloading the dashboard establishes a new session start from the then-current aligned window.

### Fixed 24-slot canvas

Each chart always reserves 24 equal-width visual slots. Bars therefore never stretch when only one or two windows have elapsed.

- Elapsed windows occupy slots from left to right.
- Elapsed slots receive WIB range labels, hover/focus targets, and values.
- Unused capacity is rendered as neutral background columns without timestamps, values, or interactive semantics.
- Once more than 24 windows have elapsed, the oldest elapsed window is removed and the chart becomes a rolling 24-window history.

The current tooltip and keyboard behavior remains: focusing or hovering an elapsed bar reveals its exact aligned WIB range and formatted value.

## Error Handling and Concurrency

- JWT signing failure returns HTTP `500`; the session handler must not leave a partially created session in the store.
- Picking remains atomic so only one shipper can claim a confirmed order.
- Delivery validates ownership and readiness while holding the order-store lock, preventing a time-of-check/time-of-use transition race.
- Frontend API failures retain the current rendered state and show the existing error area rather than inventing local lifecycle state.
- WebSocket messages trigger API refreshes; if a message is duplicated, the idempotent list reload produces the same state.
- Display names are never used as authorization fallbacks.

## Testing Strategy

### Go tests

- Identical names produce distinct session UUIDs and valid independent JWTs.
- JWT verification preserves UUID, name, and role.
- Same-name sellers cannot read, confirm, or otherwise mutate each other's products and orders.
- API responses include the expected UUID and display-name fields.
- WebSocket filtering compares UUIDs and routes seller lifecycle events correctly.
- Pickup stores deterministic `picked_at` and `ready_at` values.
- Reload-style delivery queries return the same active order.
- Early delivery, wrong-shipper delivery, and invalid transitions return the intended errors.
- Delivery history is filtered by shipper UUID and sorted newest first.

### Frontend tests

- Session serialization retains the UUID.
- Event relevance helpers compare UUID fields, including same-name sessions.
- The metric timeline begins at the dashboard-opening aligned window.
- One, two, and three elapsed windows use the first slots of a fixed 24-slot canvas.
- Unused slots have no time label or tooltip semantics.
- Active window snapshots update the existing bar; boundaries append a zero-valued bar.
- Shipper readiness is derived from `ready_at`, including ready and not-ready cases.
- Delivery API state partitions into active and newest-first history views.

### Flink tests

- Enriched UUID/name event fixtures pass existing parsing, aggregation, and CEP suites.
- Correlation remains based on product, cart, and order UUIDs rather than display names.

### Live verification

Run a fresh Docker Compose stack and use two same-name sessions to verify identity isolation. Complete one checkout-to-delivery lifecycle and verify:

1. only the owning seller sees and confirms the order,
2. seller status changes to picked and delivered without reload,
3. shipper reload preserves the active countdown,
4. early delivery is rejected,
5. successful delivery moves the order into the same shipper's history,
6. another same-name shipper cannot deliver it,
7. the dashboard begins at its current window with fixed-width bars and no historical labels.

Run the full Go, frontend, and Flink verification suites before completion.

## Out of Scope

- Persistent users, sessions, products, orders, or delivery history
- Login by existing identity
- Display-name changes
- Database migrations
- Historical dashboard windows from before the current dashboard page load
- Client-side authority over delivery readiness
- Polling as a substitute for WebSocket lifecycle notifications
