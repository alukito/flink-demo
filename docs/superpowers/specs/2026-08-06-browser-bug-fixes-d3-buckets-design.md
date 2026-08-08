# Browser Bug Fixes and D3 Five-Minute Buckets — Design Spec

## Purpose

Correct four browser-visible defects discovered during Phase 4 testing while preserving the demo's Flink-first teaching value:

1. repeated raw events in the dashboard feed;
2. misleading overlapping-window bars in Level 2;
3. a cart header that counts product lines instead of units; and
4. user sessions leaking between browser tabs and role routes.

Level 2 will use D3.js to show aligned, non-overlapping five-minute buckets whose active value is refreshed by Flink every five seconds.

## Confirmed Root Causes

### Duplicate live events

Kafka inspection showed that sampled `product.listed` records had unique `event_id` values. The duplicates are introduced in the browser. During route changes or React Strict Mode cleanup, the WebSocket is closed, but its asynchronous `onclose` callback can still schedule a reconnect. These orphan connections retain callbacks into the shared event provider, so one Kafka event can be inserted multiple times.

### Repeated Level 2 values

The current Flink jobs use five-minute sliding processing-time windows with a five-second slide. A single event therefore belongs to every overlapping window for the following five minutes. The dashboard accurately renders those outputs, but the visualization does not match the desired aligned-bucket semantics.

### Incorrect cart item label

The cart header uses `cart.length`, which counts distinct product lines. Checkout correctly displays the quantity stored on each line, so four copies of one product appear as `1 item` in the header and `4x` at checkout.

### Cross-tab and wrong-role sessions

User token, name, and role are stored in `localStorage`, which is shared by every tab for the origin. Role pages check only for a name and token, not whether the stored role matches the route.

## Selected Approach

Use true Flink aligned five-minute windows with continuous five-second updates, then render their normalized timeline with D3.js.

This was selected over browser-side aggregation because Level 2 must remain a truthful demonstration of Flink stateful processing. Deriving fixed buckets from the existing overlapping-window results was rejected because additions and expirations make that transformation ambiguous.

## Level 2 Window Semantics

### Flink computation

- Replace five-minute sliding processing-time windows with aligned, non-overlapping five-minute tumbling processing-time windows.
- Attach a continuous processing-time trigger that fires every five seconds without purging the window state.
- Each early firing emits the current aggregate for the active five-minute window.
- Repeated emissions for the active window retain the same `window_end`; the dashboard treats `(metric, scope, window_end)` as the identity and replaces the earlier value.
- At the five-minute boundary, the completed bucket remains immutable and subsequent events belong to the new bucket.
- Daily aggregations and their Jakarta-midnight behavior remain unchanged.

Expected example:

| Bucket | Events | Rendered value |
| --- | --- | ---: |
| 07:00–07:05 | 07:01, 07:03 | 2 |
| 07:05–07:10 | none | 0 |
| 07:10–07:15 | 07:10 | 1 |

The Level 2 headline labeled `5 min` shows the current aligned five-minute bucket, not a trailing five-minute total.

### Missing and active buckets

Flink does not create a window when no event arrives. The frontend therefore builds a continuous aligned timeline ending at the current five-minute bucket and inserts zero-valued buckets where no Flink result exists. A five-second browser clock recomputes that timeline even when no WebSocket message arrives, so crossing a five-minute boundary creates the next zero bucket immediately. This also allows a newly opened dashboard to show an all-zero current timeline before the first event.

The dashboard retains and renders the latest 24 five-minute buckets per metric. Missing top-product buckets have value zero and no product name; a preceding product name must not be carried forward.

## D3 Visualization

- Add D3.js and its TypeScript declarations as explicit frontend dependencies.
- Introduce a focused chart component responsible for SVG rendering, time and value scales, axes, bars, responsive sizing, transitions, and hover tooltips.
- Introduce a pure timeline-normalization helper responsible for five-minute alignment, active-window replacement, 24-bucket retention, and zero insertion.
- Drive normalization with a five-second clock tick as well as incoming metric messages.
- D3 consumes only normalized bucket data and does not own application or WebSocket state.
- Existing Level 3 CEP visualizations are out of scope and remain unchanged.

Hovering a bar shows its five-minute range and formatted value. The x-axis uses Jakarta-local clock labels such as `07:00`, `07:05`, and `07:10`.

## Live Event Feed

### WebSocket lifecycle

The WebSocket hook distinguishes intentional disposal from an unexpected disconnect:

- cleanup marks the connection as disposed before closing it;
- pending reconnect timers are cleared;
- `onclose` schedules a retry only when the hook is still active and the closing socket is still the current socket; and
- unexpected disconnects retain the existing one-second retry behavior.

### Bounded event deduplication

The event provider already retains at most 100 event envelopes. Before prepending an event, it scans the retained array for an equal `event_id`. A duplicate is ignored; a new event is prepended and the result is sliced to 100 entries.

No unbounded ID set or additional cache is introduced. The maximum work is 100 ID comparisons per incoming event. IDs that have already aged out of the visible 100-event history may be accepted again, which is an intentional bounded-memory trade-off for this demo.

An increasing global ID is not used because current IDs are UUIDs, events originate from multiple Kafka topics, Kafka offsets are ordered only within a partition, and valid events may arrive out of order.

## Cart Quantity

The cart header computes its item count as the sum of all cart-line quantities. Four copies of one product therefore display `Cart: 4 items`. Price calculation and checkout payload construction continue using the same quantities and require no semantic change.

## Per-Tab Sessions and Route Protection

- Store user token, name, and role in `sessionStorage` instead of `localStorage`.
- Clearing or replacing a session affects only the current browser tab.
- Add a reusable role-route guard around `/buyer`, `/seller`, and `/shipper`.
- A route renders only when the current tab has a token, name, and role matching that route.
- Missing or mismatched sessions redirect to `/` before the role page opens API requests or a WebSocket.
- `/dashboard` remains public and continues to use its separate dashboard token.

Directly typing a role URL in a new tab therefore opens the landing page unless that tab already owns a matching role session.

## Error Handling

- Malformed WebSocket messages remain ignored and logged.
- An intentionally closed WebSocket never reconnects.
- Unexpected disconnects reconnect after one second.
- Invalid or unauthorized role state redirects to the landing page rather than rendering a partially functional role page.
- Empty metric history renders a zero-valued D3 timeline rather than stale values or a carried-forward product.
- D3 rendering failures must not alter the underlying metric state; pure normalization remains independently testable.

## Testing Strategy

Implementation follows test-driven development.

### Flink tests

- Verify five-minute processing-time alignment.
- Verify repeated five-second firings update the same `window_end` without purging accumulated state.
- Verify a later five-minute interval receives a distinct `window_end`.
- Keep daily aggregation tests unchanged and green.

### Frontend tests

- Normalize the approved `07:00=2`, `07:05=0`, `07:10=1` example.
- Replace repeated updates for the active bucket rather than appending bars.
- Retain exactly 24 aligned buckets and clear top-product detail for zero buckets.
- Ignore a duplicate `event_id` while retaining at most 100 events.
- Dispose a WebSocket without scheduling a reconnect and retain retries for unexpected closes.
- Sum cart quantities for the header.
- Keep sessions tab-scoped and redirect missing or mismatched roles.
- Verify D3 rendering inputs, labels, and hover text from normalized data.

### Full verification

- Run frontend unit tests, lint, and production build.
- Run the complete Maven verification suite.
- Run Go tests with the race detector.
- Rebuild the Compose images.
- Run the live Phase 4 smoke test and confirm 12 Flink jobs remain running.
- Manually verify the dashboard timeline, duplicate-free feed, cart label, and new-tab redirects in the browser.

## Scope Boundaries

- No database or server-side browser-session store is introduced.
- No global event sequence is added.
- Kafka topics and event envelope schemas remain unchanged.
- CEP pattern behavior and Level 3 charts remain unchanged.
- Daily metric semantics remain unchanged.
- The browser retains bounded in-memory histories suitable for the short demo, not durable analytics history.

## Acceptance Criteria

1. One Kafka event appears at most once in the retained dashboard feed for its `event_id`.
2. No WebSocket reconnect survives component disposal.
3. Level 2 displays D3 bars for aligned five-minute intervals and refreshes the active bar every five seconds.
4. Missing five-minute intervals render as explicit zero bars.
5. Events at 07:01 and 07:03 produce value 2 at 07:00; no events produce value 0 at 07:05; an event at 07:10 produces value 1 at 07:10.
6. Four units of one product display `Cart: 4 items`.
7. A new tab or wrong-role route redirects to `/` unless that tab has a matching role session.
8. All automated verification and the live smoke test pass.
