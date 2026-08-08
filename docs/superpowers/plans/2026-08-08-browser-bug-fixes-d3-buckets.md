# Browser Bug Fixes and D3 Metric Buckets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix duplicate live events, accurately display Level 2 five-minute aggregates as D3 bucket charts, count total cart items, and isolate role sessions to a browser tab with protected routes.

**Architecture:** The backend changes Level 2 from overlapping sliding windows to aligned tumbling processing-time windows with a continuous five-second trigger. The browser normalizes backend snapshots into a fixed D3-ready sequence of 24 Jakarta-labelled buckets, filling absent buckets with zero and replacing the active bucket as new snapshots arrive. Pure helpers own all deduplication, bucket, cart-count, and session-role decisions; React components only bind those helpers to the UI.

**Tech Stack:** Java 11, Apache Flink 1.19, Go service/WebSocket, React 19, TypeScript, D3, Vite, Node built-in test runner, JUnit 5/MiniCluster.

## Global Constraints

- Preserve the existing Level 2 metric topics, `WindowStat` schema, and Jakarta-day aggregation semantics.
- `window_end` remains the authoritative UTC ISO timestamp. A bar labelled `07:00` represents `[07:00, 07:05)` and has `window_end` `07:05`.
- Window metrics use processing time and aligned, non-overlapping five-minute windows. A continuous trigger publishes the current bucket every five seconds without clearing it.
- Display the latest 24 buckets, including zero-valued buckets. The browser must advance the visible active bucket every five seconds even when no WebSocket event arrives.
- Format all chart labels and tooltips in `Asia/Jakarta`; do not rely on the workstation's timezone.
- Deduplicate only the bounded live-event feed by `event_id`; metric snapshots still replace by `(metric, scope, window_end)`.
- Store user token/name/role in `sessionStorage`, never in `localStorage`. `/dashboard` remains independently accessible through its dashboard token flow.
- Do not alter CEP behavior, its replay baseline, or daily metrics beyond retaining their display.

---

## Task 1: Add testable browser state helpers

**Files:**
- Create: `web/src/lib/eventFeed.ts`
- Create: `web/src/lib/eventFeed.test.ts`
- Create: `web/src/lib/cart.ts`
- Create: `web/src/lib/cart.test.ts`
- Create: `web/src/lib/session.ts`
- Create: `web/src/lib/session.test.ts`
- Modify: `web/package.json`

- [x] **Step 1: Write failing tests for bounded event deduplication.**

  Add tests for `appendUniqueEvent(events, event, maxEvents)` that verify a new `event_id` is prepended, a repeated `event_id` leaves the list unchanged, and the retained feed never exceeds 100 entries. Use event objects shaped like `EventEnvelope`, including a duplicate payload where practical so the comparison is explicitly by ID.

- [x] **Step 2: Write failing tests for total item quantities.**

  Test `cartItemCount(items)` with multiple product lines and repeated additions consolidated into a quantity, e.g. quantities `3` and `1` produce `4`, not `2`.

- [x] **Step 3: Write failing tests for role-session validation.**

  Keep browser-storage calls behind a small helper or injected `Storage`-like interface. Test that only a complete `(token, name, role)` record with the requested role is authorized; missing fields or a different role are rejected. Test both reading and clearing the three session keys.

- [x] **Step 4: Implement the helpers minimally.**

  In `eventFeed.ts`, export `MAX_LIVE_EVENTS = 100` and an immutable `appendUniqueEvent` that scans only the already bounded list before prepending. In `cart.ts`, export a reducer-based `cartItemCount`. In `session.ts`, centralize `readSession`, `writeSession`, `clearSession`, and `hasRequiredRole`, using `sessionStorage` and validating the `Role` union rather than casting arbitrary strings.

- [x] **Step 5: Run the new focused tests.**

  Run the new test files with Node's test runner and confirm the duplicate event, total item count, and wrong-role cases pass before wiring React to them.

## Task 2: Stop orphan WebSocket reconnects and apply event-feed deduplication

**Files:**
- Create: `web/src/lib/webSocketLifecycle.ts`
- Create: `web/src/lib/webSocketLifecycle.test.ts`
- Modify: `web/src/hooks/useWebSocket.ts`
- Modify: `web/src/context/EventContext.tsx`

- [ ] **Step 1: Write failing lifecycle tests.**

  Test a pure `shouldReconnect({ disposed, isCurrentSocket, hasToken })` decision helper. It must return false after effect cleanup, when a stale socket closes after being replaced, or when no token exists; it returns true only for the active socket in a live effect.

- [ ] **Step 2: Implement per-effect socket ownership.**

  Refactor `useWebSocket` so each effect owns a `disposed` flag and the socket it created. Clear its timer in cleanup, set `disposed = true` before closing, and schedule reconnect only when `shouldReconnect` accepts that closing socket. Do not allow an old `onclose` callback to overwrite connection state or create a second connection. Retain the existing one-second backoff and JSON parse handling.

- [ ] **Step 3: Use immutable `event_id` deduplication in the provider.**

  Replace the inline `setEvents` append logic in `EventContext.tsx` with `appendUniqueEvent`. Keep only the newest 100 unique events, which bounds the browser memory used for the feed.

- [ ] **Step 4: Run lifecycle and event tests.**

  Verify both pure helper suites and inspect the hook dependency arrangement for a single WebSocket per mounted consumer.

## Task 3: Build the five-minute bucket model before drawing charts

**Files:**
- Create: `web/src/lib/metricBuckets.ts`
- Create: `web/src/lib/metricBuckets.test.ts`
- Modify: `web/package.json`
- Modify: `web/package-lock.json`

- [ ] **Step 1: Add failing bucket-model tests for the approved example.**

  Write tests using window snapshots ending at `07:05` with value `2` and `07:15` with value `1`, evaluated at `07:10`. The normalized sequence must be:

  ```text
  07:00–07:05  2
  07:05–07:10  0
  07:10–07:15  1
  ```

  Also test that multiple snapshots for the same `(metric, scope, window_end)` select the latest value, the exact `07:10` boundary belongs to the `07:10–07:15` bucket, invalid timestamps are ignored, and the result has exactly 24 chronologically ordered buckets.

- [ ] **Step 2: Add failing Jakarta-format tests.**

  Test formatting a UTC `window_end` into a Jakarta start label and a Jakarta range label, including a UTC-to-WIB boundary. The test must pass under a non-Jakarta process timezone as well.

- [ ] **Step 3: Implement the pure bucket model.**

  Export `FIVE_MINUTES_MS`, `METRIC_BUCKET_COUNT`, `activeWindowEnd(now)`, `metricBuckets(stats, now)`, and explicit Jakarta label/range formatters. Derive a bucket start as `window_end - FIVE_MINUTES_MS`; generate all 24 expected window ends ending at the active aligned window end; use zero and empty details if no stat exists. Key source snapshots by `window_end` after Dashboard has already selected one metric and scope.

- [ ] **Step 4: Add D3 dependencies.**

  Install `d3` and its TypeScript declarations as project dependencies, updating the package manifest and lockfile without changing unrelated packages.

- [ ] **Step 5: Run the bucket tests.**

  Run the new test file and confirm it captures replacement, zero-fill, time alignment, and Jakarta formatting independently of D3 and React.

## Task 4: Replace CSS bars with an accessible D3 bucket chart

**Files:**
- Create: `web/src/components/MetricBarChart.tsx`
- Modify: `web/src/pages/Dashboard.tsx`
- Modify: `web/src/index.css`

- [ ] **Step 1: Implement `MetricBarChart` around the bucket model.**

  Accept `MetricBucket[]`, an accessible metric title, and the current value formatter. Use a responsive SVG with D3 band and linear scales: the x-axis shows local Jakarta bucket start labels; y-axis starts at zero; each bar has a `<title>` and keyboard/focus-friendly tooltip text containing the full `[start, end)` Jakarta range and exact formatted value. Use `ResizeObserver` to redraw to available card width and clean it up on unmount.

- [ ] **Step 2: Preserve the dashboard's snapshot upsert contract.**

  Keep `Dashboard.tsx` replacing a received window stat by `(metric, scope, window_end)`, capped to the recent retained history. Do not append repeat emissions as extra data points. Continue handling daily stats separately.

- [ ] **Step 3: Advance inactive buckets in the browser.**

  Add a five-second `now` state tick in Dashboard. For every window metric, pass its selected snapshots and `now` through `metricBuckets`; this makes an empty new bucket visible and zero-valued even if Flink emits nothing in that interval. Keep daily cards as their existing daily-cumulative display.

- [ ] **Step 4: Replace the inline flex-bar `MetricChart`.**

  Remove the old `.metric-chart`/`.metric-bar` rendering and use `MetricBarChart` for count, top-product count, and revenue window cards. Keep metric-card headline values, but label the five-minute value as the current aligned bucket. Update the Level 2 explanatory text to state: five-minute aligned windows update every five seconds; daily totals reset at Jakarta midnight.

- [ ] **Step 5: Add chart styles and manual rendering checks.**

  Add bounded SVG, axis, hover/focus, and tooltip CSS without disturbing CEP chart styles. Run the frontend build, then inspect the dashboard at desktop and narrow width to ensure labels remain legible and no chart leaks a resize observer.

## Task 5: Produce aligned Flink window snapshots every five seconds

**Files:**
- Create: `flink/src/main/java/com/flinkdemo/level2/MetricWindowing.java`
- Create: `flink/src/test/java/com/flinkdemo/level2/MetricWindowingTest.java`
- Modify: `flink/src/main/java/com/flinkdemo/level2/MetricJob.java`
- Modify: `flink/src/test/java/com/flinkdemo/level2/MetricFunctionsTest.java`
- Modify: `flink/src/test/java/com/flinkdemo/level2/MetricPipelineMiniClusterTest.java`

- [ ] **Step 1: Write failing unit tests for aligned-window behavior.**

  Put the production size and update interval in `MetricWindowing` (`Time.minutes(5)` and `Time.seconds(5)`), but test their observable window behavior rather than asserting literal constant values: `07:01` and `07:03` map to a window ending `07:05`; exact `07:10` maps to `07:15`. Extend the `CountWindowResult` test so the emitted `window_end` contract remains explicit.

- [ ] **Step 2: Write a bounded streaming integration test.**

  Add a deterministic test-stream/source fixture with a short test-only tumbling size and continuous trigger interval. It must verify repeated early firings have the same `window_end` while their aggregate grows, and that the next non-overlapping window has a new `window_end`. Keep the production five-minute constants untouched; the short values exist only to make the test fast.

- [ ] **Step 3: Implement reusable window construction.**

  In `MetricWindowing`, provide the shared `windowAll` configuration used by count and top-product metrics: `TumblingProcessingTimeWindows.of(WINDOW_SIZE)` followed by `ContinuousProcessingTimeTrigger.of(UPDATE_INTERVAL)`. Provide an overload/parameters suitable for the short integration test without duplicating production pipeline logic.

- [ ] **Step 4: Migrate both Level 2 window branches.**

  In `MetricJob.build`, replace both `SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(5))` calls with the shared tumbling configuration. Apply it identically to the aggregate and top-product process branches. Leave `DailyAggregateFunction` and Kafka source/sink behavior unchanged.

- [ ] **Step 5: Run focused Flink tests.**

  Run `MetricFunctionsTest`, `MetricWindowingTest`, and `MetricPipelineMiniClusterTest`. Confirm no sliding-window import remains and the tested repeated emissions share one `window_end` until the aligned boundary is crossed.

## Task 6: Make sessions tab-local and protect role routes

**Files:**
- Create: `web/src/components/RequireRole.tsx`
- Modify: `web/src/context/SessionContext.tsx`
- Modify: `web/src/App.tsx`
- Modify: `web/src/pages/Buyer.tsx`
- Modify: `web/src/pages/Seller.tsx`
- Modify: `web/src/pages/Shipper.tsx`

- [ ] **Step 1: Implement session storage through the tested helper.**

  Replace direct `localStorage` reads/writes/removals in `SessionContext` with `readSession`, `writeSession`, and `clearSession`. Initialize state from the validated session record so a stale or malformed browser value cannot masquerade as a user session.

- [ ] **Step 2: Add a declarative role guard.**

  Implement `RequireRole` using `useSession` and React Router's `<Navigate to="/" replace />`. Render children only when token, name, and exact role are valid. This prevents the protected page, its requests, and its WebSocket hook from mounting first.

- [ ] **Step 3: Wrap protected routes.**

  In `App.tsx`, guard `/buyer`, `/seller`, and `/shipper` with their matching role. Leave `/` and `/dashboard` public; dashboard continues to use its separate dashboard token rather than the user session.

- [ ] **Step 4: Simplify page-local checks and correct the cart label.**

  Remove redundant navigation checks from Buyer, Seller, and Shipper once routing owns authorization. In Buyer, use `cartItemCount(cart)` for the header text so four units across one or more product lines reads `Cart: 4 items`.

- [ ] **Step 5: Manually validate two-tab isolation.**

  In one browser tab establish a buyer session, then open `/shipper`, `/seller`, and an unauthenticated tab. Each role mismatch must immediately redirect to `/`; a fresh tab must not inherit the first tab's identity. Verify `/dashboard` still loads independently.

## Task 7: Full verification, live regression smoke, and review

**Files:**
- Modify only if verification exposes a defect; otherwise none.

- [ ] **Step 1: Run the complete frontend quality gate.**

  Run all Node test files (including existing Jakarta and CEP tests), `npm run lint`, and `npm run build` from `web/`.

- [ ] **Step 2: Run backend suites.**

  Run `go test ./... -race -v` for `app/`, then `mvn -B clean verify` for `flink/` using the established containerized Maven command if the local JDK/Maven setup is unavailable.

- [ ] **Step 3: Rebuild and perform the live smoke test.**

  Rebuild/redeploy the affected application and Level 2 jobs with the existing Docker Compose workflow. Publish or create events in one window and verify one live-feed row per `event_id`; wait for a five-second update and verify the active bar is replaced rather than duplicated; cross a five-minute boundary and verify the old value, an explicit zero gap when applicable, and a new bucket.

- [ ] **Step 4: Perform the approved browser acceptance checks.**

  Confirm D3 hover exposes each time range/value, event feed remains bounded after repeat messages, `Cart: N items` is the quantity total, route mismatches redirect before loading role UI, and a reload in the same tab retains the session while a new tab does not.

- [ ] **Step 5: Review the diff before handoff.**

  Compare all edits against the approved design, check that no placeholder chart or temporary timing constant remains, rerun the relevant failed-before tests, then record verification evidence in the handoff/PR description.
