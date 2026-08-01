# Phase 4: Level 3 CEP Alerts

## Goal

Add five independent Apache Flink CEP jobs that detect cross-event commerce patterns using event time, publish immutable alert facts, and display useful eight-hour counts and measurements in the dashboard.

Phase 4 extends the existing Phase 3 Level 2 stateful metrics demo. It demonstrates Flink CEP reading from multiple Kafka streams while keeping the teaching demo small, inspectable, and deterministic.

## Approved decisions

- Implement all five patterns from the original stream-processing design.
- Run one Flink CEP job per pattern.
- Use event time from the envelope `timestamp`, with watermarks and bounded out-of-orderness appropriate for the local demo.
- Treat alerts as immutable facts. There is no detected/resolved/expired lifecycle and no downstream reaction workflow.
- Add a stable server-generated `cart_id` to cart events so abandoned-cart episodes can be correlated correctly.
- Use deterministic `alert_id` values for replay and defensive deduplication.
- Emit raw alert records from CEP. Go forwards and lightly caches them; React derives display counts.
- Retain only the last eight hours of alert history in memory. No persistent alert store is added.
- Keep the existing no-third-party-chart-dependency constraint. Existing native hover behavior for chart points remains required.

## Pattern definitions

Each pattern emits at most one alert for its natural match key.

| Pattern | CEP input streams | Match definition | Natural alert key | Detail |
|---|---|---|---|---|
| `abandoned_cart` | `cart.item.added`, `cart.checkout` | A cart episode has an item-added event followed by no checkout for two event-time minutes | `cart_id` | `cart_id`, buyer-safe display metadata, and cart/product summary as appropriate |
| `trending_product` | `cart.item.added` | The same product is added by at least three distinct buyers within a 60-second event-time window | `product_id + window_start` | `product_id`, product name, qualifying alert metadata |
| `slow_delivery` | `shipment.picked`, `shipment.delivered` | A picked order has no delivery within 60 event-time seconds | `order_id` | `order_id`, shipper/order metadata as appropriate |
| `order_surge` | `cart.checkout` | At least three distinct buyers complete checkout within a 30-second event-time window | `window_start` | qualifying checkout count and event-time metadata |
| `delivery_completed` | `cart.checkout`, `shipment.delivered` | A checkout is followed by delivery for the same order; report elapsed seconds | `order_id` | `order_id`, checkout time, delivery time, elapsed seconds |

The `trending_product` detector may use distinct buyers internally because that is the trigger definition, but the dashboard is product-focused and does not show a buyer column.

## Event and alert contracts

### Cart correlation

The Go buyer cart flow adds a server-generated `cart_id` to `cart.item.added` and `cart.checkout` payloads. The identifier remains stable for one cart episode and is separate from the per-seller `order_id` created at checkout.

### Alert envelope

All CEP jobs write to the existing `flink.cep.alerts` Kafka topic using one generalized JSON envelope:

```json
{
  "alert_id": "slow_delivery:<order_id>",
  "pattern": "slow_delivery",
  "detected_at": "2026-08-01T10:07:00Z",
  "detail": {
    "order_id": "uuid"
  }
}
```

`alert_id` is deterministic and unique for the pattern’s natural match key. `detected_at` is an absolute instant. Pattern-specific values are contained in `detail`; no pattern-specific top-level fields are introduced.

## Flink architecture

Each job has the same high-level pipeline:

1. Consume the pattern’s Kafka input topics with stable consumer groups.
2. Deserialize and validate the shared event envelope.
3. Assign event timestamps and watermarks.
4. Remove exact replay duplicates by `event_id` before CEP.
5. Union the required streams into a typed CEP input stream.
6. Apply a keyed CEP pattern using the natural correlation key.
7. Convert matches and timed-out partial matches into the immutable alert envelope.
8. Apply deterministic alert-ID protection at the output boundary for replay-safe delivery.
9. Serialize alerts to `flink.cep.alerts`.

Patterns that correlate multiple topics must visibly use a unioned stream in the implementation and tests. Single-topic patterns still use CEP so the phase demonstrates a consistent pattern API across all five jobs.

The jobs remain independently submitted so each pattern can be inspected, restarted, and demonstrated separately. The existing session-cluster deployment model is retained; Phase 4 adds the five CEP submissions without changing Level 1 or Level 2 behavior.

## Go and dashboard flow

The Go Kafka consumer forwards raw CEP alert bytes only to dashboard clients. It maintains a small process-local cache of alert records received within the last eight hours, pruning stale entries when alerts arrive or a dashboard reconnects. The cache is an optimization for demo reloads, not durable state; live delivery continues if the cache is empty.

Dashboard replay is deterministic by `detected_at` and `alert_id`. The Go layer and React layer both treat `alert_id` as an idempotency key so replayed records do not inflate counts.

The dashboard derives the following views from the raw immutable alert stream:

- Abandoned carts: count by ten-minute bucket across eight hours, rendered as a line or bar chart.
- Delivery delays: count by ten-minute bucket across eight hours, rendered as a line or bar chart. The UI uses “Delivery delays” rather than “Slow shipper.”
- Trending products: one row per product with product name and alert count across eight hours. No buyer column is shown.
- Order surge: a prominent “Detected”/not-detected status indicator, with alert count and latest detection time. It is a status display, not an interactive switch.
- Checkout-to-delivery: one point per completed order showing elapsed seconds, rendered as a line or compact bar/scatter-style chart.

Every chart point exposes its timestamp and value on hover. The existing native tooltip behavior is preserved unless a richer accessible tooltip is required by the implementation.

## Deduplication and failure semantics

CEP emits one match for a valid sequence during normal uninterrupted processing. The explicit protections address at-least-once Kafka and checkpoint recovery rather than changing pattern meaning:

- Input event deduplication uses `event_id` before CEP.
- Output alerts carry deterministic `alert_id` values.
- Go and React upsert by `alert_id` during live delivery and replay.

The phase does not add a durable database, Kafka compaction topic, or a complex post-match recovery state machine. The eight-hour cache is process-local and is lost when the app restarts, consistent with the existing teaching-demo limitation.

## Testing and verification

The implementation plan must include:

- Unit tests for each CEP pattern, including timeout/no-follow-up behavior.
- Event-time boundary tests for two-minute, 30-second, 60-second, and elapsed-time cases.
- Tests proving multi-topic union and correlation by `cart_id` or `order_id`.
- Tests for duplicate `event_id` input and deterministic `alert_id` output.
- Tests for eight-hour cache pruning, replay ordering, and dashboard idempotency.
- Frontend lint/build checks and chart hover-value verification.
- Compose smoke coverage that submits all five CEP jobs, creates representative events, and observes each alert type.
- Final live verification of alert counts, trending-product table, surge indicator, elapsed-time chart, dashboard reload replay, seven Level 2 jobs plus five Level 3 jobs, and one TaskManager.

Level 1 and Level 2 functionality must remain unchanged except for the explicitly shared cart-event `cart_id` extension and the new dashboard alert panel.

## Non-goals

- No alert reactions, notifications, or workflow automation.
- No alert resolution lifecycle.
- No historical storage beyond the process-local eight-hour cache.
- No third-party charting library.
- No replacement of existing Level 1/Level 2 pipelines.
- No production high-availability or durable Flink recovery guarantees.
