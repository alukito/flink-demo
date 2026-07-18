# Stream Processing Demo — Design Spec

## Purpose

A demo project to complement a 30-minute tech talk about stream processing. The demo highlights three levels of stream processing:

1. **Stateless operators** — simple Kafka consumers reacting to events
2. **Stateful operators** — sliding window aggregations via Apache Flink
3. **Complex event processing (CEP)** — pattern detection across event sequences via Flink CEP

The simulation is a 3-role e-commerce system: **Buyer**, **Seller**, and **Shipper**. Audience members (10–15 people) participate live by scanning a QR code, picking a display name, and choosing a role.

## Stack

| Component | Technology |
|-----------|-----------|
| Stream processing | Apache Flink (Java) |
| Backend | Go (REST API + WebSocket server + Kafka producer/consumer) |
| Frontend | React + TypeScript |
| Event bus | Kafka (single broker) |
| Deployment | Docker Compose on a cloud VPS |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Browser (React + TS)                      │
│   Landing │  Seller UI │  Buyer UI │  Shipper UI │  Dashboard   │
└──────────────────────────┬──────────────────────────────────────┘
                           │ HTTP (actions) + WebSocket (events)
┌──────────────────────────▼──────────────────────────────────────┐
│                      Go Server                                    │
│   REST API (role actions) │ WebSocket Hub (broadcast events)     │
│   Kafka Producer          │ Kafka Consumer (input + output)      │
│   JWT auth middleware      │ In-memory state (catalog, orders)   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────▼────────────────┐
          │         Kafka (single broker)    │
          │  input topics │  output topics   │
          └────────────────┬────────────────┘
                           │
          ┌────────────────▼────────────────┐
          │        Apache Flink (Java)       │
          │  Level 2: Stateful windows       │
          │  Level 3: CEP patterns           │
          └─────────────────────────────────┘
```

**Data flow for Flink results:** Flink reads from input Kafka topics, computes results, writes to output Kafka topics. The Go server consumes output topics and fans results out to WebSocket subscribers. This reinforces the talk's narrative: events flow through Kafka, Flink processes them, outputs land in Kafka, and consumers (including the dashboard) react.

## Kafka Topics & Event Schema

### Input topics (produced by Go API from role actions)

| Topic | Producer action | Level 1 consumers | Flink consumers |
|-------|-----------------|-------------------|-----------------|
| `product.listed` | Seller adds product | Buyer UI | Job 1 |
| `cart.item.added` | Buyer adds to cart | — | Job 1, Job 2 |
| `cart.checkout` | Buyer checks out | Seller UI | Job 1, Job 2 |
| `order.confirmed` | Seller confirms order | Shipper UI | Job 1, Job 2 |
| `shipment.picked` | Shipper accepts job | — | Job 2 |
| `shipment.delivered` | Shipper marks delivered | Buyer UI | Job 1, Job 2 |

### Output topics (produced by Flink, consumed by Go → WebSocket → Dashboard)

| Topic | Produced by | Content |
|-------|-------------|---------|
| `flink.window.stats` | Job 1 (Level 2) | Sliding window aggregates |
| `flink.cep.alerts` | Job 2 (Level 3) | CEP pattern matches |

### Event envelope

All events share this shape:

```json
{
  "event_id": "uuid",
  "event_type": "cart.checkout",
  "actor_id": "buyer-display-name",
  "actor_role": "buyer",
  "timestamp": "2026-07-18T10:00:00Z",
  "payload": { ... }
}
```

The Go server is both a Kafka producer (forwarding role actions) and a Kafka consumer (forwarding Kafka events to WebSocket clients). Level 1 routing is done in Go with plain Kafka consumers — no Flink involved — which is intentional: it lets the talk say *"this is just a Kafka consumer, no state needed"* before introducing Flink.

## Order Lifecycle

```
Buyer checkout → [cart.checkout, one event per per-seller order]
                       ↓
              Seller sees new order (filtered to their products)
              Seller confirms order → [order.confirmed]
                                           ↓
                                  Shipper sees pickup job (all unpicked jobs visible to all shippers)
                                  Shipper picks job → [shipment.picked]
                                  Shipper delivers → [shipment.delivered]
                                                          ↓
                                                  Buyer gets notified (filtered to their orders)
```

## Data Structures & Filtering

### Multi-Seller Cart Splitting

A buyer's cart can contain products from multiple sellers. At checkout, the Go server splits the cart by `SellerID` and produces **one `cart.checkout` event per seller**. The buyer receives N order IDs (one per seller) and sees N order cards in their UI, each tracking its own lifecycle independently.

### Data Model

```go
type Product struct {
    ID, Name            string
    Price               int  // cents
    Quantity            int
    SellerID            string  // ownership
    ListedAt            time.Time
}

type Order struct {
    ID                  string
    BuyerID             string  // who placed it
    SellerID            string  // which seller owns this sub-order
    Items               []OrderItem
    TotalAmount         int
    ShippingAddress     string
    Status              OrderStatus  // checkout → confirmed → picked → delivered
    PickedBy            string  // shipper name (set on pick)
    CreatedAt, ConfirmedAt, PickedAt, DeliveredAt  time.Time
}
```

### In-Memory Collections & Indexes

Go server maintains one primary store plus derived indexes, all guarded by a single `sync.RWMutex`:

```go
type Store struct {
    mu        sync.RWMutex

    // Primary stores (source of truth)
    products  map[string]*Product     // productID → product
    orders    map[string]*Order       // orderID → order
    sessions  map[string]*Session     // sessionID → session

    // Derived indexes (kept in sync on every mutation, same locked region)
    productsBySeller map[string][]string          // sellerID → []productID
    ordersByBuyer    map[string][]string          // buyerID → []orderID
    ordersBySeller   map[string][]string          // sellerID → []orderID
    ordersByStatus   map[OrderStatus][]string     // status → []orderID (e.g., "confirmed" → shipper job board)
}
```

The two primary maps hold source of truth; lookup by ID is O(1). The derived indexes support role-specific queries without scanning the primary maps. **Update rule:** every mutation (add product, create order, confirm, pick, deliver) updates the primary map AND all affected indexes in the same locked region. Reads from indexes are lock-free reads of the slice reference.

The extra memory (multiple slices pointing to the same IDs) is trivial for a 10–15 person demo. The upside is clean separation between primary state and lookup, predictable read performance, and code that's easy to reason about.

### Filtering Rules

| Role | REST query | WebSocket push |
|------|------------|----------------|
| **Buyer** | `ordersByBuyer[buyerID]` for own orders | `product.listed` (all); `cart.checkout`, `order.confirmed`, `shipment.picked`, `shipment.delivered` filtered to `BuyerID == self` |
| **Seller** | `productsBySeller[sellerID]` for own products; `ordersBySeller[sellerID]` filtered by status for incoming orders | `cart.checkout` filtered to `SellerID == self`; `order.confirmed` echoes for own orders |
| **Shipper** | `ordersByStatus[confirmed]` for the job board | `order.confirmed` for ALL unpicked orders (any shipper can claim) |
| **Dashboard** | — | Everything — raw events + Flink outputs |

**Key distinction:** REST queries use indexes for O(1) lookup. WebSocket subscription filtering is push-based and does **per-client predicate checks** on each event (e.g., "does this event's `buyer_id` match this client's name?"). No indexes are needed for push — just a predicate per client per event.

### Race Conditions

| Race | Solution |
|------|----------|
| **Two shippers pick the same job** | Go mutex on order; check `status == confirmed` under lock, set to `picked` + assign `PickedBy`, then produce `shipment.picked` event. Second caller gets 409 Conflict. |
| **Seller double-clicks confirm** | Same mutex pattern; `status == checkout` check, idempotent. |
| **Buyer cart concurrent mods** | Single browser, single buyer — not an issue. |

**Critical design point:** the Go server's in-memory state is the **source of truth** for order lifecycle. Kafka events are *notifications* of state changes, not the state itself. The mutex protects state synchronously; the Kafka event is produced *after* the state change succeeds. This keeps ordering correct: no consumer ever sees an event for a state that hasn't already been committed in Go.

### Event Payloads (with relationship fields)

Events carry the relationship fields needed for filtering:

```json
// product.listed
{ "actor_id": "seller-name", "payload": {
    "product_id": "uuid", "name": "Widget", "price": 500, "quantity": 10 }}

// cart.item.added
{ "actor_id": "buyer-name", "payload": {
    "product_id": "uuid", "seller_id": "seller-name", "quantity": 1 }}

// cart.checkout (one event per per-seller order)
{ "actor_id": "buyer-name", "payload": {
    "order_id": "uuid", "seller_id": "seller-name",
    "items": [...], "total_amount": 1500, "shipping_address": "..." }}

// order.confirmed
{ "actor_id": "seller-name", "payload": {
    "order_id": "uuid", "buyer_id": "buyer-name" }}

// shipment.picked
{ "actor_id": "shipper-name", "payload": {
    "order_id": "uuid", "buyer_id": "buyer-name", "seller_id": "seller-name" }}

// shipment.delivered
{ "actor_id": "shipper-name", "payload": {
    "order_id": "uuid", "buyer_id": "buyer-name" }}
```

## Role UIs & User Flows

### Landing Page
- User enters a display name
- Chooses a role: Buyer / Seller / Shipper
- `POST /api/session` → returns a signed JWT containing `{ name, role }`
- JWT is stored in the browser and sent with every subsequent request

### Seller UI
- **Product panel:** Form to add a product (name, price, quantity). On submit → `POST /api/seller/products` → Go produces `product.listed`.
- **Order inbox:** Live list of incoming checkouts filtered to this seller's products (fed via WebSocket from `cart.checkout` where `seller_id == self`). Each order has a "Confirm Order" button → `POST /api/seller/orders/:id/confirm` → Go produces `order.confirmed`.

### Buyer UI
- **Product catalog:** Live-updating grid of available products (fed via WebSocket from `product.listed`).
- **Cart:** Add items from any sellers, view cart total. Cart can mix products from multiple sellers.
- **Checkout:** Enter shipping address → `POST /api/buyer/cart/checkout` → Go splits cart by seller and produces one `cart.checkout` event per seller. Buyer receives N order IDs (one per seller in the cart).
- **Order status:** Shows N order cards (one per seller), each tracking its own state (Confirmed / Picked / Delivered), updated live via WebSocket. Each card shows the seller name, items from that seller, and current status.

### Shipper UI
- **Job board:** Live list of confirmed orders ready for pickup (fed via WebSocket from `order.confirmed`). Each job shows buyer name, address, items. All shippers see all unpicked jobs; once a job is picked, it disappears from the board for all other shippers (enforced server-side via mutex; second picker gets 409 Conflict).
- **Accept job** → `POST /api/shipper/jobs/:id/pick` → Go produces `shipment.picked`. After picking, a random **5–15 second countdown timer** runs in the browser. The "Mark Delivered" button is disabled and shows the countdown. This simulates transit time and prevents the shipper from clicking delivered too quickly.
- **Mark delivered** → `POST /api/shipper/jobs/:id/deliver` → Go produces `shipment.delivered`.

### Dashboard (speaker-projected)
Three panels side by side, each illustrating a level:
- **Level 1 — Live event feed:** scrolling list of raw events as they happen
- **Level 2 — Aggregations:** Subscribes to `flink.window.stats` and splits events by `metric` field. For each metric:
  - Stat card: latest window value (`scope: "window"`)
  - Stat card: total of the day (`scope: "daily"`)
  - Bar chart: sliding window history (last N `scope: "window"` events, shows evolution)
- **Level 3 — CEP alerts:** Subscribes to `flink.cep.alerts`, renders each alert by `pattern` field with `detail` formatted per pattern type (abandoned cart in amber, order surge in red, etc.)

## Authentication

- `POST /api/session` accepts `{ name, role }` and returns a signed JWT containing `{ name, role }`
- Every role-namespaced endpoint verifies the JWT signature and the role claim via middleware
- Role enforcement is real, not just structural — buyer calling seller endpoints gets 403
- JWT is stateless and self-contained; no session store required
- This is a talking point in the talk: "even our demo enforces role separation"

## API Design

Role-namespaced REST endpoints:

```
# Session
POST /api/session                      → register name + role, return JWT

# Seller (requires role=seller)
POST /api/seller/products              → produces product.listed
GET  /api/seller/products              → seller's own listings
GET  /api/seller/orders                → incoming checkout orders
POST /api/seller/orders/:id/confirm    → produces order.confirmed

# Buyer (requires role=buyer)
GET  /api/buyer/products               → full product catalog
POST /api/buyer/cart/items             → produces cart.item.added
POST /api/buyer/cart/checkout          → produces cart.checkout
GET  /api/buyer/orders/:id/status      → current order status

# Shipper (requires role=shipper)
GET  /api/shipper/jobs                 → confirmed orders awaiting pickup
POST /api/shipper/jobs/:id/pick        → produces shipment.picked
POST /api/shipper/jobs/:id/deliver     → produces shipment.delivered
```

Each namespace has a single `requireRole(role)` middleware applied to all its routes. The structure is auth-ready for production — in a real deployment the middleware would also check additional claims (tenant, permissions, etc.).

## Flink Jobs

### Job 1 — Sliding Window Aggregations (Level 2)

**Structure:** One Flink job per metric. Each job has a single source topic and a single aggregation, making it simple to explain and reason about. All jobs write to the same `flink.window.stats` topic with a generalized envelope; the dashboard consumer splits by the `metric` field.

**Window:** 5-minute sliding window, emits every 5 seconds. The 5-second emit frequency makes the dashboard animate quickly during the demo while still accurately demonstrating the 5-minute sliding window concept.

**Metric jobs (window scope):**

| Job (metric) | Source topic | What it shows |
|--------|-------------|---------------|
| `listings_count` | `product.listed` | Products listed by sellers |
| `cart_adds_count` | `cart.item.added` | Items added to carts |
| `tx_count` | `cart.checkout` | Checkouts (headline metric) |
| `confirmed_orders` | `order.confirmed` | Orders seller confirmed |
| `delivered_orders` | `shipment.delivered` | Orders shipper delivered |
| `top_product` | `cart.item.added` | Product with most cart adds in the window |

**Daily cumulative jobs** (keyed by calendar day, for the "total of day" stat cards):

| Job (metric) | Source topic | What it shows |
|--------|-------------|---------------|
| `tx_count` (daily scope) | `cart.checkout` | Total checkouts today |
| `revenue` (daily scope) | `cart.checkout` | Total revenue today (a satisfying number to watch climb) |
| `delivered_orders` (daily scope) | `shipment.delivered` | Total deliveries today |

Metrics that have both a windowed value and a daily cumulative (e.g., `tx_count`, `delivered_orders`) are emitted by the same job with a `scope` field distinguishing the two. The dashboard renders the window value as a bar chart entry and the daily value as a stat card.

This turns the Level 2 dashboard into a stacked/grouped bar chart per window — the audience literally sees the funnel of events flowing through the system.

**Writes to:** `flink.window.stats`

**Generalized envelope:**

```json
{
  "metric": "tx_count",
  "scope": "window",
  "window_end": "2026-07-18T10:05:00Z",
  "value": 7,
  "detail": {}
}
```

- `metric` — identifies which metric this is
- `scope` — `"window"` (5-minute sliding) or `"daily"` (cumulative for the day)
- `window_end` — window boundary timestamp (day boundary for daily scope)
- `value` — the primary numeric value
- `detail` — metric-specific extras (empty for simple counts)

**Examples by metric:**

```json
// Simple count metric (window)
{ "metric": "tx_count", "scope": "window", "window_end": "...", "value": 7, "detail": {} }

// Same metric, daily cumulative
{ "metric": "tx_count", "scope": "daily", "window_end": "...", "value": 23, "detail": {} }

// Daily revenue (daily scope only)
{ "metric": "revenue", "scope": "daily", "window_end": "...", "value": 489000, "detail": {} }

// Top product — value is the count, product info in detail
{ "metric": "top_product", "scope": "window", "window_end": "...", "value": 4,
  "detail": { "product_id": "uuid", "name": "Widget" } }
```

### Job 2 — CEP Patterns (Level 3)

**Structure:** One Flink CEP job per pattern. Each job reads the topics it needs and detects one specific pattern. All jobs write to the same `flink.cep.alerts` topic with a generalized envelope; pattern-specific fields live inside `detail`.

**Reads (union across all pattern jobs):** `cart.item.added`, `cart.checkout`, `order.confirmed`, `shipment.picked`, `shipment.delivered`

**Patterns:**

| Pattern | Trigger | Window | How to demo live |
|---------|---------|--------|------------------|
| Abandoned cart | `cart.item.added` with no `cart.checkout` per buyer | 2 min | One volunteer adds to cart and does not check out |
| Trending product | Same `product_id` in `cart.item.added` by 3+ distinct buyers | 60s | Speaker tells audience to all add the same item |
| Slow shipper | `shipment.picked` with no `shipment.delivered` per order | 60s after countdown ends | Happens naturally if shipper waits |
| Order surge | 3+ `cart.checkout` events from distinct buyers | 30s | Speaker signals audience to all check out at once |
| Checkout-to-delivery time | `cart.checkout` → `shipment.delivered` per order | — (measures elapsed time) | Completes naturally |

**Writes to:** `flink.cep.alerts`

**Generalized envelope:**

```json
{
  "pattern": "<pattern_name>",
  "detected_at": "2026-07-18T10:07:00Z",
  "detail": { ... pattern-specific fields ... }
}
```

All pattern-specific data lives inside `detail`, so the envelope is uniform across all patterns. The dashboard renders the `pattern` field as the alert type, `detected_at` as the timestamp, and formats the `detail` object per pattern.

**Examples by pattern:**

```json
// Abandoned cart
{ "pattern": "abandoned_cart", "detected_at": "...",
  "detail": { "actor_id": "buyer-display-name" } }

// Trending product
{ "pattern": "trending_product", "detected_at": "...",
  "detail": { "product_id": "uuid", "product_name": "Widget", "distinct_buyers": 4 } }

// Slow shipper
{ "pattern": "slow_shipper", "detected_at": "...",
  "detail": { "order_id": "uuid", "shipper_id": "shipper-display-name" } }

// Order surge
{ "pattern": "order_surge", "detected_at": "...",
  "detail": { "checkout_count": 4 } }

// Checkout-to-delivery time
{ "pattern": "delivery_completed", "detected_at": "...",
  "detail": { "order_id": "uuid", "checkout_to_delivery_seconds": 47 } }
```

## Go Server Structure

**Two responsibilities, cleanly separated:**

1. **REST API** — handles role actions, produces to Kafka. Auth via JWT middleware per namespace.
2. **WebSocket Hub** — fans events to connected clients by role:
   - Consumes all input Kafka topics → routes to relevant role subscribers (Level 1)
   - Consumes `flink.window.stats` → broadcasts to dashboard subscribers (Level 2)
   - Consumes `flink.cep.alerts` → broadcasts to dashboard subscribers (Level 3)

**State:** In-memory only (product catalog, order list). No database — sufficient for a demo.

**Frontend serving:** React is built to static files and embedded in the Go binary using `embed.FS`. No separate frontend container; one binary, one port.

## Deployment (Docker Compose)

All services run as containers on a single cloud VPS. One `docker compose up` brings everything up. Audience accesses the app via a public URL (or QR code).

```yaml
services:
  zookeeper:              # Kafka dependency
  kafka:                  # Single broker, port 9092 internal
  flink-jobmanager:       # Flink master, Web UI on port 8081
  flink-taskmanager:      # Flink worker
  flink-job-submit:       # One-shot: submits both Flink JARs via REST API on startup, then exits
  app:                    # Go server: REST API + WebSocket, port 8080; serves embedded React static files
```

**Startup order:** Zookeeper → Kafka → (Go app + Flink managers in parallel) → flink-job-submit

**Why 3 Flink containers (session cluster):** This mirrors real-world Flink deployments where the cluster (JobManager + TaskManager) is long-running and shared, and jobs are submitted to it independently by CI/CD systems or data engineers. The `flink-job-submit` container is a one-shot that calls the JobManager REST API with the JAR, then exits. It's a teaching moment during the talk and shows the production-like separation between cluster and job lifecycle.

**Why bake the submit into a separate container instead of the JobManager:** Keeps the JobManager image generic (no demo JAR baked in). The JAR lives in the submit container, which is closer to how CI/CD pipelines submit jobs to a shared cluster.

**Kafka topics:** Auto-created by the Go server on startup via the Kafka admin API — no manual setup needed.

**Flink Web UI** (port 8081) shows running jobs, parallelism, and operator graphs — visually impressive during the talk and reinforces the Flink narrative.

## Error Handling

Demo-appropriate, not over-engineered:

- **Kafka unavailable on startup:** Go server and Flink jobs retry with backoff. Docker Compose `depends_on` + health checks ensure Kafka is ready before app starts.
- **Flink job failure:** JobManager auto-restarts failed jobs (configured with `restart-strategy: fixed-delay`).
- **WebSocket disconnect:** Client reconnects automatically with exponential backoff.
- **Invalid role action** (e.g., buyer calling seller endpoint): JWT middleware returns 403.
- **Duplicate session name:** `/api/session` rejects names already taken — keeps the demo clean.

## Testing

- **Flink jobs:** Unit tests using Flink's `MiniClusterWithClientResource` — runs jobs in-process, no Docker needed.
- **Go API:** Standard `net/http/httptest` for REST endpoints; mock Kafka producer for unit tests.
- **Integration test:** A single script that spins up Docker Compose and runs a scripted scenario (list product → add to cart → checkout → confirm → pick → deliver) and asserts the expected Kafka events appear on the output topics.
- **Frontend:** No tests — scope is a demo, not a production app.

## Out of Scope (YAGNI)

- No database — in-memory state only
- No user accounts or persistent sessions across server restarts
- No frontend tests
- No production-grade observability (logging/metrics/tracing)
- No multi-tenant support
- No horizontal scaling — single VPS, single instance of each service
- No product catalog seeding tools — seller adds products live or via the UI
