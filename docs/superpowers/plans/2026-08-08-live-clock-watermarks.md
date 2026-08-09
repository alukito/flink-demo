# Live Clock Watermarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow Level 3 event-time CEP matches and timeouts to complete when no later business event arrives.

**Architecture:** Replace the event-driven bounded-out-of-orderness generator used after the Kafka source with a shared periodic generator whose watermark is the current UTC clock minus five seconds. Flink supplies the periodic callback; the generator contains no thread or scheduler. It emits only after seeing an event and never regresses.

**Tech Stack:** Java 11, Apache Flink 1.19.3 DataStream event-time API, JUnit 5, Maven, Docker Compose.

## Global Constraints

- Keep all five Level 3 jobs on event time.
- Allow five seconds of lateness for live Go events stamped from the current UTC clock.
- Use a one-second Flink auto-watermark interval.
- Do not depend on a later Kafka business event to advance CEP time.
- Preserve the existing eight-hour delivery-completion retention horizon.

---

### Task 1: Shared live-clock watermark generator

**Files:**
- Create: `flink/src/main/java/com/flinkdemo/level3/LiveClockWatermarkGenerator.java`
- Create: `flink/src/test/java/com/flinkdemo/level3/LiveClockWatermarkGeneratorTest.java`
- Modify: `flink/src/main/java/com/flinkdemo/level3/CepJobSupport.java`
- Modify: `flink/src/main/java/com/flinkdemo/level3/CepJob.java`
- Test: `flink/src/test/java/com/flinkdemo/level3/pattern/DeliveryCompletedPatternTest.java`

**Interfaces:**
- Consumes: Flink `WatermarkGenerator<EventEnvelope>`, an allowed-lateness `Duration`, and a serializable millisecond clock.
- Produces: `LiveClockWatermarkGenerator`, used by `CepJobSupport.eventTime(DataStream<EventEnvelope>)` through `WatermarkStrategy.forGenerator(...)`.

- [ ] **Step 1: Write failing generator tests**

  Add deterministic tests proving no watermark is emitted before the first event, a quiet stream advances from `currentTimeMillis() - 5_000`, and a backward clock cannot regress the watermark.

- [ ] **Step 2: Run the tests to verify RED**

  Run `mvn -B -Dtest=LiveClockWatermarkGeneratorTest test` in the Java 11 Maven container. Expect compilation failure because `LiveClockWatermarkGenerator` does not exist.

- [ ] **Step 3: Implement the minimal generator**

  Implement `onEvent` by recording that input has begun. Implement `onPeriodicEmit` by emitting `clock.currentTimeMillis() - allowedLatenessMillis` only when it exceeds the last emitted watermark. Keep the serializable clock interface package-private for deterministic tests.

- [ ] **Step 4: Wire the shared strategy**

  In `CepJobSupport.eventTime`, construct the strategy with `WatermarkStrategy.forGenerator(...)`, retain the existing event timestamp assigner, and remove the whole-stream idleness wrapper. In `CepJob`, set the auto-watermark interval to 1,000 milliseconds.

- [ ] **Step 5: Verify GREEN and regressions**

  Run the focused generator, support, and delivery-pattern tests, then `mvn -B clean verify`. Expect zero failures.

- [ ] **Step 6: Rebuild and verify the live lifecycle**

  Rebuild the Flink submission image, restart the Compose jobs without deleting volumes, perform a fresh checkout-to-delivery lifecycle, and verify that `flink.cep.alerts` receives `delivery_completed` about five to six seconds after delivery without another checkout or delivery event.
