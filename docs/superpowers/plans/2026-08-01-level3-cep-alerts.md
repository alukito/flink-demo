# Phase 4: Level 3 CEP Alerts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (\`- [ ]\`) syntax for tracking.

**Goal:** Add five independent event-time Flink CEP jobs that emit immutable Level 3 alerts from multiple Kafka streams and render eight-hour counts and measurements in the dashboard.

**Architecture:** Extend the existing Java 11/Maven Flink module with a shared CEP foundation and one CepJob entry point selected by --pattern. Each pattern consumes its topic set, assigns event time, deduplicates input event IDs, applies a keyed CEP pattern, and writes deterministic alert envelopes to flink.cep.alerts. Go forwards and replays recent raw alerts; React derives the mixed dashboard views.

**Tech Stack:** Java 11, Maven, Apache Flink 1.19.3 DataStream + CEP, Kafka connector 3.3.0-1.19, Jackson 2.15.2, JUnit 5, Go, Kafka, React 19, TypeScript 6, Vite, Docker Compose.

## Global Constraints

- Implement abandoned_cart, trending_product, slow_delivery, order_surge, and delivery_completed.
- Run one independently submitted CEP job per pattern.
- Use event time from envelope timestamp with bounded-out-of-orderness watermarks.
- Add stable browser-generated cart_id to cart.item.added and cart.checkout; Go validates and propagates it unchanged.
- Emit immutable alerts to flink.cep.alerts with alert_id, pattern, detected_at, and detail.
- Deduplicate input event_id before CEP; deduplicate replayed alerts by alert_id in Go and React.
- Retain only the last eight hours of alerts in memory.
- Use ten-minute buckets for abandoned-cart and delivery-delay counts.
- Trending products show product and count only; no buyer column.
- Order surge is a status indicator, not a control.
- Preserve Level 1 and Level 2 behavior except for the shared cart ID and Level 3 panel.
- No third-party chart dependency; chart points expose hover values.
- Keep one TaskManager, set taskmanager.numberOfTaskSlots to 12 and taskmanager.memory.process.size to 2048m.
- Do not add durable alert storage, alert lifecycle, reactions, notifications, or production HA guarantees.

## File Structure

Create the Level 3 Java classes under flink/src/main/java/com/flinkdemo/level3 and their tests under flink/src/test/java/com/flinkdemo/level3. Modify the existing Go buyer handler, Kafka consumer, WebSocket hub, React EventContext, Buyer, Dashboard, API client, CSS, Makefile, Docker Compose, and submission script. Add scripts/phase4-smoke.sh and regenerate app/web/dist.

---

### Task 1: Stable Cart Correlation

**Files:** app/internal/buyer/handler.go, app/internal/buyer/handler_test.go, web/src/api/client.ts, web/src/pages/Buyer.tsx.

**Interfaces:** addToCart(token, cartID, productID, quantity) and checkout(token, cartID, items, shippingAddress) send cart_id. Go rejects an empty ID and copies it into both emitted payloads.

- [ ] Step 1: Add failing Go tests for cart ID preservation in cart.item.added and cart.checkout, plus HTTP 400 for an empty ID.
- [ ] Step 2: Run RED: cd app && go test ./internal/buyer -run 'CartID|cart_id' -v. Expected failure because the request and event fields do not exist.
- [ ] Step 3: Add CartID fields and validation in Go. In Buyer.tsx initialize one active cart with crypto.randomUUID(), pass it to add and checkout, and generate a new ID after successful checkout. Update the API helpers.
- [ ] Step 4: Run cd app && go test ./internal/buyer ./internal/event -v and cd web && npm run lint. Expect pass with only existing Fast Refresh warnings.
- [ ] Step 5: Commit with git add app/internal/buyer web/src/api/client.ts web/src/pages/Buyer.tsx && git commit -m "feat: add stable cart correlation IDs".

### Task 2: CEP Foundation

**Files:** flink/pom.xml; create level3/model/CepAlert.java, level3/serde/CepAlertSchema.java, level3/CepPattern.java, level3/CepJobSupport.java, level3/EventDeduplicator.java and their tests.

**Interfaces:** CepAlert(alertId, pattern, detectedAt, Map<String,Object> detail); CepJobSupport.source(brokers, pattern); CepJobSupport.eventTime(stream); CepJobSupport.alertSink(brokers); CepPattern.fromName(name); EventDeduplicator forwards the first event_id only.

- [ ] Step 1: Add the org.apache.flink:flink-cep dependency using the existing flink.version property. Write tests for defensive alert detail copies, JSON numeric detail values, exact five pattern names, and duplicate event_id suppression.
- [ ] Step 2: Run RED: mvn -f flink/pom.xml -Dtest='com.flinkdemo.level3.*Test' test.
- [ ] Step 3: Implement JSON model, serializer, topic/group mappings, event timestamp assignment using Instant.parse and five-second bounded watermarks, and bounded replay dedup state. Reject invalid timestamps.
- [ ] Step 4: Run the focused Maven tests; expect all pass and numeric JSON values remain numbers.
- [ ] Step 5: Commit with git add flink/pom.xml flink/src/main/java/com/flinkdemo/level3 flink/src/test/java/com/flinkdemo/level3 && git commit -m "feat: add Flink CEP alert foundation".

### Task 3: Abandoned Cart CEP

**Files:** create level3/pattern/AbandonedCartPattern.java and AbandonedCartPatternTest.java.

**Interface:** build(DataStream<EventEnvelope>) consumes cart.item.added and cart.checkout, keyed by payload.cart_id, and emits abandoned_cart:<cart_id> after two event-time minutes without checkout.

- [ ] Step 1: Test added-without-checkout timeout, checkout within two minutes, and duplicate input IDs.
- [ ] Step 2: Run RED: mvn -f flink/pom.xml -Dtest=AbandonedCartPatternTest test.
- [ ] Step 3: Implement an event-time keyed union with Pattern.begin("added"), a negative checkout condition, within(Time.minutes(2)), and a timeout callback that emits the immutable alert.
- [ ] Step 4: Run the focused test; expect one deterministic alert only for the abandoned cart.
- [ ] Step 5: Commit with git add flink/src/main/java/com/flinkdemo/level3/pattern/AbandonedCartPattern.java flink/src/test/java/com/flinkdemo/level3/pattern/AbandonedCartPatternTest.java && git commit -m "feat: detect abandoned carts with CEP".

### Task 4: Trending Product CEP

**Files:** create level3/pattern/TrendingProductPattern.java and TrendingProductPatternTest.java.

**Interface:** build(stream) keys by product_id and detects three distinct actor_id values within 60 event-time seconds; emits trending_product:<product_id>:<window_start>.

- [ ] Step 1: Test three distinct buyers, two buyers, repeated additions by one buyer, and a third buyer after the boundary.
- [ ] Step 2: Run RED: mvn -f flink/pom.xml -Dtest=TrendingProductPatternTest test.
- [ ] Step 3: Implement a keyed first-plus-two CEP pattern with an iterative condition that rejects a previously matched actor_id. Include product ID/name and qualifying count detail.
- [ ] Step 4: Run the focused test and verify overlapping combinations do not inflate one product/window alert ID.
- [ ] Step 5: Commit with git add flink/src/main/java/com/flinkdemo/level3/pattern/TrendingProductPattern.java flink/src/test/java/com/flinkdemo/level3/pattern/TrendingProductPatternTest.java && git commit -m "feat: detect trending products with CEP".

### Task 5: Slow Delivery CEP

**Files:** create level3/pattern/SlowDeliveryPattern.java and SlowDeliveryPatternTest.java.

**Interface:** build(stream) unions shipment.picked and shipment.delivered, keys by order_id, and emits slow_delivery:<order_id> after 60 event-time seconds without delivery.

- [ ] Step 1: Test timeout, picked-then-delivered, another order’s delivery, and duplicate picked IDs.
- [ ] Step 2: Run RED: mvn -f flink/pom.xml -Dtest=SlowDeliveryPatternTest test.
- [ ] Step 3: Implement Pattern.begin("picked").notFollowedBy("delivered").within(Time.seconds(60)) on the keyed event-time union.
- [ ] Step 4: Run the focused test; expect one alert per order.
- [ ] Step 5: Commit with git add flink/src/main/java/com/flinkdemo/level3/pattern/SlowDeliveryPattern.java flink/src/test/java/com/flinkdemo/level3/pattern/SlowDeliveryPatternTest.java && git commit -m "feat: detect delivery delays with CEP".

### Task 6: Order Surge CEP

**Files:** create level3/pattern/OrderSurgePattern.java and OrderSurgePatternTest.java.

**Interface:** build(stream) consumes cart.checkout, keys to one stable CEP partition, detects three distinct buyers within 30 event-time seconds, and emits order_surge:<window_start> with numeric checkout_count.

- [ ] Step 1: Test three distinct buyers, repeated buyer, fewer than three, and a later event-time bucket.
- [ ] Step 2: Run RED: mvn -f flink/pom.xml -Dtest=OrderSurgePatternTest test.
- [ ] Step 3: Implement an iterative distinct-buyer condition with a 30-second within bound and deterministic window-start ID.
- [ ] Step 4: Run the focused test; expect all cases to pass.
- [ ] Step 5: Commit with git add flink/src/main/java/com/flinkdemo/level3/pattern/OrderSurgePattern.java flink/src/test/java/com/flinkdemo/level3/pattern/OrderSurgePatternTest.java && git commit -m "feat: detect checkout surges with CEP".

### Task 7: Checkout-to-Delivery CEP

**Files:** create level3/pattern/DeliveryCompletedPattern.java and DeliveryCompletedPatternTest.java.

**Interface:** build(stream) unions cart.checkout and shipment.delivered, keys by order_id, and emits delivery_completed:<order_id> with numeric elapsed_seconds.

- [ ] Step 1: Test a 47-second completion, delivery without checkout, mismatched order IDs, negative elapsed time, and duplicate IDs.
- [ ] Step 2: Run RED: mvn -f flink/pom.xml -Dtest=DeliveryCompletedPatternTest test.
- [ ] Step 3: Implement an ordered checkout-to-delivered CEP pattern and compute Duration.between(checkoutInstant, deliveredInstant).getSeconds(). Do not add a business deadline.
- [ ] Step 4: Run the focused test and verify numeric elapsed detail.
- [ ] Step 5: Commit with git add flink/src/main/java/com/flinkdemo/level3/pattern/DeliveryCompletedPattern.java flink/src/test/java/com/flinkdemo/level3/pattern/DeliveryCompletedPatternTest.java && git commit -m "feat: measure checkout to delivery with CEP".

### Task 8: CEP Entrypoint and Deployment

**Files:** create level3/CepJob.java; modify level3/CepPattern.java, flink/submit-jobs.sh, docker-compose.yml, Makefile, and flink/Dockerfile.

**Interfaces:** CepJob.main accepts --pattern and --brokers and executes level3-<pattern>. The submitter launches seven Level 2 jobs and five Level 3 jobs.

- [ ] Step 1: Add failing tests for all five name mappings, unknown-name failure, and distinct level3 job names.
- [ ] Step 2: Run RED: mvn -f flink/pom.xml -Dtest='com.flinkdemo.level3.CepPattern*Test' test.
- [ ] Step 3: Implement CepJob with parallelism one, checkpointing, restart strategy, source, selected pattern, and alert sink. Add this exact submission loop:

~~~sh
for pattern in abandoned_cart trending_product slow_delivery order_surge delivery_completed; do
  /opt/flink/bin/flink run -d -m flink-jobmanager:8081 -c com.flinkdemo.level3.CepJob /opt/flink/usrlib/level2-jobs.jar --pattern "$pattern" --brokers kafka:9092
done
~~~

Set TaskManager slots to 12 and process memory to 2048m. Add flink-cep to the shaded JAR and invoke CepJob explicitly with -c.
- [ ] Step 4: Run mvn -f flink/pom.xml clean verify, docker compose config --quiet, and docker compose build flink-job-submit.
- [ ] Step 5: Commit with git add flink/src/main/java/com/flinkdemo/level3/CepJob.java flink/src/main/java/com/flinkdemo/level3/CepPattern.java flink/submit-jobs.sh docker-compose.yml Makefile flink/Dockerfile && git commit -m "feat: deploy five independent CEP jobs".

### Task 9: Go Alert Consumer and Replay Cache

**Files:** modify app/internal/kafkaclient/consumer.go, consumer_test.go, app/internal/ws/hub.go, and hub_test.go.

**Interfaces:** Broadcaster gains BroadcastCEPAlertRaw([]byte). Hub.alertCache stores copied alerts keyed by alert_id with detected_at; dashboard registration replays entries sorted by detected_at then alert_id.

- [ ] Step 1: Test topic subscription, dashboard-only delivery, byte-copy isolation, eight-hour pruning, deterministic replay, duplicate replacement, and unchanged Level 2 replay.
- [ ] Step 2: Run RED: cd app && go test ./internal/kafkaclient ./internal/ws -run 'Alert|Replay|CEP|Metric' -v.
- [ ] Step 3: Add flink.cep.alerts to Consumer.Start, route it to a separate raw-alert method, parse only ID/timestamp for cache indexing, prune with time.Now().UTC().Add(-8*time.Hour), and replay copied bytes with existing non-blocking sends.
- [ ] Step 4: Run cd app && go test ./internal/kafkaclient ./internal/ws -race -v.
- [ ] Step 5: Commit with git add app/internal/kafkaclient app/internal/ws && git commit -m "feat: replay recent CEP alerts to dashboards".

### Task 10: React CEP Views

**Files:** modify web/src/context/EventContext.tsx, web/src/pages/Dashboard.tsx, web/src/index.css, web/package.json, and Makefile; create web/src/lib/cepAlerts.ts and cepAlerts.test.ts; regenerate app/web/dist.

**Interfaces:** CepAlert is { alert_id, pattern, detected_at, detail }. Add isCepAlert, retainRecentAlerts, bucketAlertCounts, trendingProductCounts, latestOrderSurge, and deliveryDurations helpers.

- [ ] Step 1: Add Node tests for alert replacement, eight-hour boundaries, 48 ten-minute buckets, product/count aggregation, surge status, elapsed seconds, and sorting. Add npm script test:cep.
- [ ] Step 2: Run RED: cd web && npm run test:cep.
- [ ] Step 3: Extend DashboardMessage with CepAlert, replace duplicate alert IDs, ignore malformed timestamps, and derive bounded display state.
- [ ] Step 4: Add the Level 3 dashboard section: abandoned-cart chart, delivery-delay chart, product/count table, non-interactive surge badge, and checkout-to-delivery chart. Use title attributes containing timestamp/value on all chart points. Use label Delivery delays.
- [ ] Step 5: Run npm run test:cep, npm run lint, and npm run build from web. Expect pass with only known warnings.
- [ ] Step 6: Add cd web && npm run test:cep to Makefile verify before the Jakarta test.
- [ ] Step 7: Commit with git add web/src/context/EventContext.tsx web/src/lib/cepAlerts.ts web/src/lib/cepAlerts.test.ts web/src/pages/Dashboard.tsx web/src/index.css web/package.json Makefile app/web/dist && git commit -m "feat: render CEP alert dashboard views".

### Task 11: Compose Smoke Test and Final Verification

**Files:** create scripts/phase4-smoke.sh; modify README.md.

**Interfaces:** the smoke script confirms 12 RUNNING Flink jobs and observes all five alert types on flink.cep.alerts. README documents the five demo actions, 12-slot TaskManager, immutable counts, and eight-hour cache limitation.

- [ ] Step 1: Create an executable smoke script. It must create an abandoned cart, three-buyer trending product, three-buyer order surge, a completed checkout-to-delivery order, and a second picked order left undelivered until the slow-delivery timeout. Assert one deterministic alert ID per pattern and numeric count/elapsed detail.
- [ ] Step 2: Run:

~~~bash
docker compose down -v
PATH="/d/CodexTools/jq:$PATH" ./scripts/phase4-smoke.sh
~~~

Expected: Phase 4 smoke test passed: twelve jobs running and five CEP alert patterns observed. Print service logs on failure and leave the stack running on success.
- [ ] Step 3: Add README operator instructions and limitations.
- [ ] Step 4: Run portable mingw32-make verify. Expect Go tests, CEP/Jakarta tests, frontend lint/build, and Maven BUILD SUCCESS.
- [ ] Step 5: Verify dashboard charts/tooltips, product/count table, surge badge, elapsed points, reload replay without duplicate counts, 12 jobs, one 12-slot TaskManager, and zero browser/WebSocket errors.
- [ ] Step 6: Run docker compose config --quiet, confirm flink/target/level2-jobs.jar, confirm no web/package-lock.json diff, git diff --check, restore/remove only generated flink/target, and commit:

~~~bash
git add scripts/phase4-smoke.sh README.md
git commit -m "test: verify Phase 4 CEP demo flow"
~~~

- [ ] Step 7: Run git status --short and expect no tracked changes.

## Final Review Checklist

- Five independent CEP jobs run with event-time semantics.
- Multi-topic union is visible in abandoned-cart, slow-delivery, and checkout-to-delivery jobs.
- Stable cart_id correlates cart episodes.
- Input replay deduplication and deterministic alert IDs are tested.
- Alerts are immutable and count-oriented.
- Go replays only the last eight hours and remains process-local.
- Trending products are product/count focused with no buyer column.
- Charts expose values on hover.
- One TaskManager has 12 slots for seven Level 2 plus five Level 3 jobs.
- Local gates, Compose smoke, browser reload, and runtime job-count checks pass.
