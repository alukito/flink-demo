# UUID Identity, Live Lifecycle, and Dashboard Timeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace display-name identity with UUID identity, make delivery lifecycle state reload-safe and live across roles, add shipper history, and render Level 2 metrics on a fixed 24-slot session timeline.

**Architecture:** Go owns UUID sessions, authorization, lifecycle timestamps, readiness, and history. Kafka events carry UUID relationships plus display-name snapshots; WebSockets invalidate role views, which reload authoritative API state. React owns only the dashboard-visible session start and D3 capacity rendering.

**Tech Stack:** Go 1.23, JWT v5, React 19, TypeScript 6, D3 7, Node 22, Flink 1.19/Java 11, Kafka, Docker Compose.

## Global Constraints

- Duplicate names are allowed and never authorize access.
- State remains in memory and resets with the app.
- `*_id` and `actor_id` are UUIDs; matching names are snapshots.
- Readiness is server-owned, 5–15 seconds, with deterministic test seams.
- Charts expose only elapsed windows while reserving 24 stable slots.
- Preserve `.superpowers/brainstorm/`; never stage it.

---

### Task 1: UUID Sessions and JWT Claims

**Files:**
- Modify: `app/internal/auth/jwt.go`, `jwt_test.go`, `middleware_test.go`
- Modify: `app/internal/session/store.go`, `handler.go`, `handler_test.go`
- Modify: `app/internal/server/server_test.go`

**Interfaces:**
- Produces: `Claims{ID,Name,Role}`
- Produces: `Sign(id, name, role string) (string, error)`
- Produces: session response `{id,name,role,token}`

- [ ] **Step 1: Write failing tests**

Assert two `{"name":"alex","role":"seller"}` requests return 201 with different nonempty IDs. Assert a signed token round-trips ID, name, and role.

- [ ] **Step 2: Run RED**

Run: `cd app; go test ./internal/auth ./internal/session -v`

Expected: missing ID/three-argument signer and duplicate-name conflict.

- [ ] **Step 3: Implement**

Use:
```go
type Claims struct {
    ID string `json:"id"`
    Name string `json:"name"`
    Role string `json:"role"`
    jwt.RegisteredClaims
}
type Session struct { ID, Name, Role string }
func (s *Store) Create(session Session) error
```

Generate `uuid.New().String()`, sign before insertion, key the store by ID, and replace duplicate-name handling with duplicate-ID protection.

- [ ] **Step 4: Verify and commit**

Run: `cd app; go test ./... -race`

Commit: `feat: issue UUID-backed sessions`

---

### Task 2: UUID Domain Ownership and Event Routing

**Files:**
- Modify: `app/internal/event/event.go`, tests
- Modify: `app/internal/product/*`, `buyer/*`, `order/*`, `shipper/*`
- Modify: `app/internal/ws/handler.go`, `hub.go`, tests
- Modify: `flink/src/main/java/com/flinkdemo/level2/model/EventEnvelope.java`
- Modify: `flink/src/test/java/com/flinkdemo/level2/JsonEnvelopeTest.java`

**Interfaces:**
- Consumes: Task 1 claims
- Produces: `NewEvent(type, actorID, actorName, actorRole, payload)`
- Produces: product/order name snapshots and `Client{ID,Name,Role}`

- [ ] **Step 1: Write failing isolation/schema tests**

Create same-name seller IDs `seller-a` and `seller-b`; assert product/order indexes and confirmation remain isolated. Assert `actor_name` serializes and Java deserializes it. Assert seller A alone receives lifecycle events whose `seller_id` is A.

- [ ] **Step 2: Run RED**

Run Go package tests plus `mvn -B -Dtest=JsonEnvelopeTest test`.

- [ ] **Step 3: Implement enriched schema**

Add `ActorName`; add `SellerName`, `BuyerName`, and later `PickedByName`. Use `claims.ID` for indexes, ownership, actors, and WebSocket comparison; use names only for display fields. Update every `NewEvent` call and Java constructor.

- [ ] **Step 4: Implement routing**

Route checkout to matching buyer/seller, confirmed to matching buyer/seller plus all shippers, picked to buyer/seller plus all shippers, and delivered to buyer/seller plus owning shipper.

- [ ] **Step 5: Verify and commit**

Run: `cd app; go test ./... -race`; focused Flink schema/CEP tests.

Commit: `feat: route domain events by UUID identity`

---

### Task 3: Server-Owned Pickup Readiness and History

**Files:**
- Modify: `app/internal/order/store.go`, `store_test.go`
- Modify: `app/internal/shipper/handler.go`, tests

**Interfaces:**
- Produces: `NewStore(options ...StoreOption)`
- Produces: `WithClock(func() time.Time)`, `WithReadyDelay(func() time.Duration)`
- Produces: `Pick(orderID, shipperID, shipperName string)`
- Produces: `Deliver(orderID, shipperID string)`
- Produces: `ByShipper(id) (active, history []Order)`
- Produces: `ErrWrongShipper`, `ErrNotReady`

- [ ] **Step 1: Write deterministic RED tests**

With fixed clock and 10-second delay, assert `PickedAt`, `ReadyAt`, early rejection, wrong-owner rejection, successful delivery at readiness, and newest-first history.

- [ ] **Step 2: Run RED**

Run: `cd app; go test ./internal/order -run 'TestStore(Pick|Deliver|ByShipper)' -v`

- [ ] **Step 3: Implement atomically**

Under the store mutex, check status, owner, and readiness; update indexes and timestamps using the injected clock. Default delay returns an inclusive 5–15 seconds. Return copied/sorted orders.

- [ ] **Step 4: Verify and commit**

Run order/shipper race tests and `go test ./... -race`.

Commit: `feat: persist shipper readiness and history`

---

### Task 4: Shipper Deliveries API

**Files:**
- Modify: `app/internal/shipper/handler.go`, tests
- Modify: `app/internal/server/server.go`, `server_test.go`

**Interfaces:**
- Produces: `GET /api/shipper/deliveries`
- Returns: `{"active":[],"history":[]}`

- [ ] **Step 1: Write failing route, filtering, payload, and status tests**

Assert only the authenticated shipper's records return. Assert pickup/delivery events include buyer, seller, shipper UUID/name pairs and `ready_at`. Assert early delivery is 409 and wrong shipper is 403.

- [ ] **Step 2: Run RED**

Run: `cd app; go test ./internal/shipper ./internal/server -v`

- [ ] **Step 3: Implement**

Add `ListDeliveries`, register its authenticated route, and build events from the post-transition order fetched from the store.

- [ ] **Step 4: Verify and commit**

Run full Go race suite.

Commit: `feat: expose shipper delivery state`

---

### Task 5: Frontend UUID Sessions and Live Order Refresh

**Files:**
- Modify: `web/src/api/client.ts`, `lib/session.ts`, tests
- Create: `web/src/lib/orderEvents.ts`, `orderEvents.test.ts`
- Modify: `web/src/context/SessionContext.tsx`, `EventContext.tsx`
- Modify: `web/src/pages/Landing.tsx`, `Buyer.tsx`, `Seller.tsx`

**Interfaces:**
- Produces: frontend `Session{id,token,name,role}`
- Produces: UUID predicates `isBuyerOrderEvent`, `isSellerOrderEvent`, `isShipperQueueEvent`

- [ ] **Step 1: Write RED tests**

Assert session ID round-trip and same-name/different-ID event relevance. Seller delivery for `seller-a` must not match `seller-b`.

- [ ] **Step 2: Run RED**

Run: `cd web; npm test`

- [ ] **Step 3: Implement**

Persist/expose ID, display `actor_name ?? actor_id`, show entity name snapshots, and compare UUIDs. React only to `events[0]`; do not use `events.some`, which retriggers forever after an old match.

- [ ] **Step 4: Verify and commit**

Run `npm test`, `npm run lint`, `npm run build`, and Go tests.

Commit: `feat: refresh role pages by UUID events`

---

### Task 6: Reload-Safe Shipper UI

**Files:**
- Modify: `web/src/api/client.ts`
- Create: `web/src/lib/deliveries.ts`, `deliveries.test.ts`
- Modify: `web/src/pages/Shipper.tsx`

**Interfaces:**
- Produces: `listShipperDeliveries(token)`
- Produces: `secondsUntilReady(readyAt, now)`

- [ ] **Step 1: Write RED tests**

Assert a 5.8-second remainder rounds up to 6, ready/invalid timestamps return 0, and response arrays are copied without mutation.

- [ ] **Step 2: Run RED**

Run: `cd web; node --test src/lib/deliveries.test.ts`

- [ ] **Step 3: Implement**

Remove browser-owned picked/random state. Load jobs and deliveries on mount, derive countdown from `ready_at` with one page timer, reload after API actions and relevant newest WebSocket events. Render Available Jobs, My Active Deliveries, and newest-first My Delivery History with names, destination, items, timestamps, and elapsed seconds.

- [ ] **Step 4: Verify and commit**

Run all frontend gates.

Commit: `feat: show reload-safe shipper delivery history`

---

### Task 7: Fixed 24-Slot D3 Session Timeline

**Files:**
- Modify: `web/src/lib/metricBuckets.ts`, tests
- Modify: `web/src/components/MetricBarChart.tsx`
- Modify: `web/src/pages/Dashboard.tsx`, `index.css`

**Interfaces:**
- Produces: `dashboardSessionStart(now): string`
- Produces: `metricBuckets(stats, sessionStart, now)` with 1–24 elapsed buckets

- [ ] **Step 1: Write RED tests**

Opening at 14:02 yields only 14:00–14:05. At 14:11 it yields exactly three windows. Hide pre-session snapshots, update matching windows, and retain newest 24 after overflow.

- [ ] **Step 2: Run RED**

Run: `cd web; node --test src/lib/metricBuckets.test.ts`

- [ ] **Step 3: Implement bucket model**

Capture session start once on Dashboard mount. Generate from `max(sessionStart, activeEnd - 23 windows)` through active end; default missing elapsed values to zero.

- [ ] **Step 4: Implement fixed visual capacity**

D3's x-domain is 24 slot keys so bandwidth never changes. Render 24 neutral background slots; map elapsed buckets left-to-right. Only elapsed buckets receive labels, hit targets, focus, titles, and tooltips.

- [ ] **Step 5: Verify and commit**

Run frontend test/lint/build.

Commit: `feat: anchor metrics to dashboard session time`

---

### Task 8: Full Flink, Embedded UI, and Live Verification

**Files:**
- Modify generated: `app/web/dist/*`
- Modify source/tests only for failures proven during this task

- [ ] **Step 1: Run clean gates**

Run Go `go test ./... -race -v`, Node test/lint/build, and Java 11 `mvn -B clean verify`. Require zero failures/errors.

- [ ] **Step 2: Regenerate embedded assets**

Build `web/dist`, replace tracked `app/web/dist`, verify new hashes in `index.html`, and rerun Go tests.

- [ ] **Step 3: Rebuild clean Compose stack**

Run `docker compose down -v` and `docker compose up -d --build`. Poll health until app/Kafka/Flink are healthy and exactly 12 jobs are RUNNING.

- [ ] **Step 4: Live lifecycle proof**

Create two same-name sellers and shippers and prove distinct UUIDs. Complete checkout through delivery while proving: seller isolation; seller live checkout/picked/delivered statuses; shipper reload preserves countdown; wrong shipper gets 403; early delivery gets 409; owner succeeds after readiness; history appears only for owner.

- [ ] **Step 5: Live D3 proof**

Open a fresh dashboard: verify 24 neutral slots and one elapsed interactive slot; same-window values update without width changes; the next boundary adds an equal-width zero bar before its snapshot; hover/focus show only elapsed WIB ranges at desktop and 375px.

- [ ] **Step 6: Final review and commit**

Run `git diff --check`, inspect status/stat, ensure no generated artifacts outside tracked `app/web/dist`, and never stage `.superpowers/brainstorm/`.

Commit: `build: embed UUID lifecycle dashboard`
