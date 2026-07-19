# Phase 3: Level 2 — Stateful Flink Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add seven independently submitted Apache Flink metric jobs that publish five-minute sliding-window and UTC-daily aggregates to Kafka, forward them through Go to dashboard WebSockets, and render Level 2 cards and history charts.

**Architecture:** A Java 11/Maven module builds one shaded JAR whose `MetricJob` entry point is submitted seven times with a metric argument; every submission reads one Kafka input topic and writes the generalized JSON metric envelope to `flink.window.stats`. The existing Go process keeps typed routing for raw Level 1 events and adds a dashboard-only raw-JSON path for Flink output, while React discriminates the two envelope shapes and keeps bounded per-metric history. Docker Compose runs a Flink 1.19.3 session JobManager, one eight-slot TaskManager, and a one-shot submission image sized for a small VPS.

**Tech Stack:** Java 11, Maven 3.9, Apache Flink 1.19.3 DataStream API, Flink Kafka connector 3.3.0-1.19, Jackson 2.17.2, JUnit 5, Go 1.23, React 19, TypeScript 6, Docker Compose

## Global Constraints

- Preserve the six input topics and `flink.window.stats` output topic from the design spec.
- Use one logical Flink job per metric: `listings_count`, `cart_adds_count`, `tx_count`, `confirmed_orders`, `delivered_orders`, `top_product`, and `revenue`.
- Every window metric uses a five-minute processing-time sliding window with a five-second slide.
- `tx_count` and `delivered_orders` emit both `window` and `daily`; `revenue` emits only `daily`; the other metrics emit only `window`.
- Daily keys and boundaries use UTC calendar days derived from the event envelope timestamp.
- Monetary values are whole Indonesian Rupiah integers end-to-end; Java uses `long`, Go keeps integer JSON values, and React formats IDR with zero fractional digits.
- The output JSON shape is exactly `{metric, scope, window_end, value, detail}`; `value` is an integer and `detail` is always an object.
- `top_product.detail` is exactly `{product_id, name}` and its `value` counts cart-add events, not requested quantity.
- Kafka source groups are stable per metric, start from committed offsets, and fall back to earliest only when no committed offset exists.
- Kafka sink delivery is at-least-once with 30-second checkpoints; dashboard state tolerates duplicate envelopes by replacing equal `(metric, scope, window_end)` records.
- Keep Flink operator parallelism at one because every source topic currently has one partition and the demo runs on a small VPS.
- Do not add a frontend chart dependency; use semantic HTML/CSS bars.
- Keep Level 1 behavior and role filtering unchanged.
- No frontend unit-test framework is added; `npm run lint` and `npm run build` are the frontend gates.
- Official references: [Kafka connector 3.3.0-1.19](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/datastream/kafka/), [window semantics](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/), [Flink test utilities](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/configuration/testing/), [Maven shading](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/configuration/maven/), and [Docker session clusters](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/resource-providers/standalone/docker/).

---

## File Structure

```text
flink/
├── pom.xml
├── Dockerfile
├── submit-jobs.sh
└── src/
    ├── main/java/com/flinkdemo/level2/
    │   ├── MetricJob.java
    │   ├── MetricDefinition.java
    │   ├── KafkaIO.java
    │   ├── model/EventEnvelope.java
    │   ├── model/WindowStat.java
    │   ├── serde/EventEnvelopeSchema.java
    │   ├── serde/WindowStatSchema.java
    │   ├── function/CountAggregate.java
    │   ├── function/CountWindowResult.java
    │   ├── function/DailyAggregateFunction.java
    │   └── function/TopProductWindowFunction.java
    └── test/java/com/flinkdemo/level2/
        ├── JsonEnvelopeTest.java
        ├── MetricFunctionsTest.java
        └── MetricPipelineMiniClusterTest.java
app/internal/
├── buyer/handler.go
├── buyer/handler_test.go
├── kafkaclient/consumer.go
├── kafkaclient/consumer_test.go
├── product/store.go
├── order/store.go
├── ws/hub.go
└── ws/hub_test.go
web/src/
├── context/EventContext.tsx
├── hooks/useWebSocket.ts
├── pages/Dashboard.tsx
└── index.css
docker-compose.yml
Makefile
scripts/phase3-smoke.sh
```

---

### Task 1: Maven Module and Generalized JSON Contracts

**Files:**
- Create: `flink/pom.xml`
- Create: `flink/src/main/java/com/flinkdemo/level2/model/EventEnvelope.java`
- Create: `flink/src/main/java/com/flinkdemo/level2/model/WindowStat.java`
- Create: `flink/src/main/java/com/flinkdemo/level2/serde/EventEnvelopeSchema.java`
- Create: `flink/src/main/java/com/flinkdemo/level2/serde/WindowStatSchema.java`
- Test: `flink/src/test/java/com/flinkdemo/level2/JsonEnvelopeTest.java`

**Interfaces:**
- Produces: `EventEnvelope(String eventId, String eventType, String actorId, String actorRole, String timestamp, JsonNode payload)` and bean getters.
- Produces: `WindowStat(String metric, String scope, String windowEnd, long value, Map<String,String> detail)` and bean getters.
- Produces: `EventEnvelopeSchema.deserialize(byte[]): EventEnvelope` and `WindowStatSchema.serialize(WindowStat, KafkaSinkContext, Long): ProducerRecord<byte[],byte[]>`.

- [ ] **Step 1: Write the failing JSON contract test**

Create `flink/src/test/java/com/flinkdemo/level2/JsonEnvelopeTest.java`:

```java
package com.flinkdemo.level2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import com.flinkdemo.level2.serde.EventEnvelopeSchema;
import com.flinkdemo.level2.serde.WindowStatSchema;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonEnvelopeTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void deserializesInputEnvelopeWithoutChangingRupiahIntegers() throws Exception {
        String json = "{\"event_id\":\"e1\",\"event_type\":\"cart.checkout\",\"actor_id\":\"b1\",\"actor_role\":\"buyer\",\"timestamp\":\"2026-07-18T10:00:00Z\",\"payload\":{\"total_amount\":489000}}";
        EventEnvelope event = new EventEnvelopeSchema().deserialize(json.getBytes(StandardCharsets.UTF_8));
        assertEquals("cart.checkout", event.getEventType());
        assertEquals(489000L, event.getPayload().get("total_amount").longValueExact());
    }

    @Test
    void serializesGeneralizedMetricEnvelope() throws Exception {
        WindowStat stat = new WindowStat("top_product", "window", "2026-07-18T10:05:00Z", 4L, Map.of("product_id", "p1", "name", "Widget"));
        byte[] bytes = new WindowStatSchema("flink.window.stats").serialize(stat, null, null).value();
        var json = mapper.readTree(bytes);
        assertEquals("top_product", json.get("metric").textValue());
        assertEquals(4L, json.get("value").longValueExact());
        assertEquals("p1", json.get("detail").get("product_id").textValue());
    }
}
```

- [ ] **Step 2: Add the Maven build and verify the test fails**

Create `flink/pom.xml` with this exact content:

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.flinkdemo</groupId>
  <artifactId>level2-jobs</artifactId>
  <version>1.0.0</version>
  <properties>
    <maven.compiler.release>11</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <flink.version>1.19.3</flink.version>
    <junit.version>5.10.3</junit.version>
  </properties>
  <dependencies>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId><version>${flink.version}</version><scope>provided</scope></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-clients</artifactId><version>${flink.version}</version><scope>provided</scope></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-connector-kafka</artifactId><version>3.3.0-1.19</version></dependency>
    <dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId><version>2.17.2</version></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-test-utils</artifactId><version>${flink.version}</version><scope>test</scope></dependency>
    <dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><version>${junit.version}</version><scope>test</scope></dependency>
    <dependency><groupId>org.junit.vintage</groupId><artifactId>junit-vintage-engine</artifactId><version>${junit.version}</version><scope>test</scope></dependency>
    <dependency><groupId>junit</groupId><artifactId>junit</artifactId><version>4.13.2</version><scope>test</scope></dependency>
  </dependencies>
  <build>
    <finalName>level2-jobs</finalName>
    <plugins>
      <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
      <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-surefire-plugin</artifactId><version>3.2.5</version><configuration><useModulePath>false</useModulePath></configuration></plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId><artifactId>maven-shade-plugin</artifactId><version>3.6.0</version>
        <executions><execution><phase>package</phase><goals><goal>shade</goal></goals><configuration>
          <createDependencyReducedPom>false</createDependencyReducedPom>
          <filters><filter><artifact>*:*</artifact><excludes><exclude>META-INF/*.SF</exclude><exclude>META-INF/*.DSA</exclude><exclude>META-INF/*.RSA</exclude></excludes></filter></filters>
          <transformers><transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/><transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer"><mainClass>com.flinkdemo.level2.MetricJob</mainClass></transformer></transformers>
        </configuration></execution></executions>
      </plugin>
    </plugins>
  </build>
</project>
```

Run: `mvn -f flink/pom.xml -Dtest=JsonEnvelopeTest test`
Expected: FAIL during test compilation because the model and schema packages do not exist.

- [ ] **Step 3: Implement the input and output models**

Create `flink/src/main/java/com/flinkdemo/level2/model/EventEnvelope.java`:

```java
package com.flinkdemo.level2.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;

public class EventEnvelope implements Serializable {
    @JsonProperty("event_id") private String eventId;
    @JsonProperty("event_type") private String eventType;
    @JsonProperty("actor_id") private String actorId;
    @JsonProperty("actor_role") private String actorRole;
    private String timestamp;
    private JsonNode payload;

    public EventEnvelope() {}
    public EventEnvelope(String eventId, String eventType, String actorId, String actorRole, String timestamp, JsonNode payload) {
        this.eventId = eventId; this.eventType = eventType; this.actorId = actorId; this.actorRole = actorRole; this.timestamp = timestamp; this.payload = payload;
    }
    public String getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public String getActorId() { return actorId; }
    public String getActorRole() { return actorRole; }
    public String getTimestamp() { return timestamp; }
    public JsonNode getPayload() { return payload; }
}
```

Create `flink/src/main/java/com/flinkdemo/level2/model/WindowStat.java`:

```java
package com.flinkdemo.level2.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowStat implements Serializable {
    private String metric;
    private String scope;
    @JsonProperty("window_end") private String windowEnd;
    private long value;
    private Map<String, String> detail = new LinkedHashMap<>();

    public WindowStat() {}
    public WindowStat(String metric, String scope, String windowEnd, long value, Map<String, String> detail) {
        this.metric = metric; this.scope = scope; this.windowEnd = windowEnd; this.value = value; this.detail = new LinkedHashMap<>(detail);
    }
    public String getMetric() { return metric; }
    public String getScope() { return scope; }
    public String getWindowEnd() { return windowEnd; }
    public long getValue() { return value; }
    public Map<String, String> getDetail() { return detail; }
}
```

- [ ] **Step 4: Implement Kafka JSON schemas**

Create `flink/src/main/java/com/flinkdemo/level2/serde/EventEnvelopeSchema.java`:

```java
package com.flinkdemo.level2.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public final class EventEnvelopeSchema extends AbstractDeserializationSchema<EventEnvelope> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Override public EventEnvelope deserialize(byte[] message) throws IOException { return MAPPER.readValue(message, EventEnvelope.class); }
}
```

Create `flink/src/main/java/com/flinkdemo/level2/serde/WindowStatSchema.java`:

```java
package com.flinkdemo.level2.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.WindowStat;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class WindowStatSchema implements KafkaRecordSerializationSchema<WindowStat> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String topic;
    public WindowStatSchema(String topic) { this.topic = topic; }
    @Override public ProducerRecord<byte[], byte[]> serialize(WindowStat stat, KafkaSinkContext context, Long timestamp) {
        try {
            byte[] key = (stat.getMetric() + ":" + stat.getScope()).getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topic, key, MAPPER.writeValueAsBytes(stat));
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("cannot serialize window stat", error);
        }
    }
}
```

- [ ] **Step 5: Run the contract test**

Run: `mvn -f flink/pom.xml -Dtest=JsonEnvelopeTest test`
Expected: PASS with `Tests run: 2, Failures: 0, Errors: 0`.

- [ ] **Step 6: Commit**

```bash
git add flink/pom.xml flink/src/main/java/com/flinkdemo/level2/model flink/src/main/java/com/flinkdemo/level2/serde flink/src/test/java/com/flinkdemo/level2/JsonEnvelopeTest.java
git commit -m "feat: define Level 2 Flink metric envelopes"
```

---

### Task 2: Metric Definitions and Window/Daily Functions

**Files:**
- Create: `flink/src/main/java/com/flinkdemo/level2/MetricDefinition.java`
- Create: `flink/src/main/java/com/flinkdemo/level2/function/CountAggregate.java`
- Create: `flink/src/main/java/com/flinkdemo/level2/function/CountWindowResult.java`
- Create: `flink/src/main/java/com/flinkdemo/level2/function/DailyAggregateFunction.java`
- Create: `flink/src/main/java/com/flinkdemo/level2/function/TopProductWindowFunction.java`
- Test: `flink/src/test/java/com/flinkdemo/level2/MetricFunctionsTest.java`

**Interfaces:**
- Produces: `MetricDefinition.fromName(String)`, `sourceTopic()`, `hasWindow()`, `hasDaily()`, `isRevenue()`, `isTopProduct()`.
- Produces: `CountAggregate` returning event counts as `Long`.
- Produces: `CountWindowResult(String metric)` emitting `WindowStat` with an ISO-8601 UTC window end.
- Produces: `DailyAggregateFunction(String metric, boolean revenue)` keyed by UTC date and emitting a running total after every event.
- Produces: `TopProductWindowFunction` choosing highest event count, then lexicographically smallest product ID for deterministic ties.

- [ ] **Step 1: Write failing function tests**

Create `flink/src/test/java/com/flinkdemo/level2/MetricFunctionsTest.java`:

```java
package com.flinkdemo.level2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.function.CountAggregate;
import com.flinkdemo.level2.function.TopProductWindowFunction;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class MetricFunctionsTest {
    private final ObjectMapper mapper = new ObjectMapper();

    private EventEnvelope event(String id, String type, String timestamp, String payload) throws Exception {
        return new EventEnvelope(id, type, "actor", "buyer", timestamp, mapper.readTree(payload));
    }

    @Test void definitionsMapEveryMetricToOneTopic() {
        assertEquals("product.listed", MetricDefinition.fromName("listings_count").sourceTopic());
        assertEquals("cart.checkout", MetricDefinition.fromName("revenue").sourceTopic());
        assertThrows(IllegalArgumentException.class, () -> MetricDefinition.fromName("unknown"));
    }

    @Test void countAggregateCountsEventsRatherThanQuantity() throws Exception {
        CountAggregate aggregate = new CountAggregate();
        long accumulator = aggregate.add(event("e1", "cart.item.added", "2026-07-18T10:00:00Z", "{\"quantity\":9}"), aggregate.createAccumulator());
        assertEquals(1L, accumulator);
    }

    @Test void topProductUsesStableTieBreakAndRequiredDetailKeys() throws Exception {
        List<EventEnvelope> events = List.of(
            event("e1", "cart.item.added", "2026-07-18T10:00:00Z", "{\"product_id\":\"p2\",\"product_name\":\"B\"}"),
            event("e2", "cart.item.added", "2026-07-18T10:00:01Z", "{\"product_id\":\"p1\",\"product_name\":\"A\"}"));
        List<WindowStat> output = new ArrayList<>();
        new TopProductWindowFunction().process(new TimeWindow(0L, 5000L), events, collector(output));
        assertEquals("p1", output.get(0).getDetail().get("product_id"));
        assertEquals("A", output.get(0).getDetail().get("name"));
        assertEquals(1L, output.get(0).getValue());
    }

    private static <T> Collector<T> collector(List<T> values) {
        return new Collector<>() { public void collect(T value) { values.add(value); } public void close() {} };
    }
}
```

Run: `mvn -f flink/pom.xml -Dtest=MetricFunctionsTest test`
Expected: FAIL during test compilation because the metric definition and functions do not exist.

- [ ] **Step 2: Implement metric definitions**

Create `flink/src/main/java/com/flinkdemo/level2/MetricDefinition.java`:

```java
package com.flinkdemo.level2;

public enum MetricDefinition {
    LISTINGS_COUNT("listings_count", "product.listed", true, false, false, false),
    CART_ADDS_COUNT("cart_adds_count", "cart.item.added", true, false, false, false),
    TX_COUNT("tx_count", "cart.checkout", true, true, false, false),
    CONFIRMED_ORDERS("confirmed_orders", "order.confirmed", true, false, false, false),
    DELIVERED_ORDERS("delivered_orders", "shipment.delivered", true, true, false, false),
    TOP_PRODUCT("top_product", "cart.item.added", true, false, false, true),
    REVENUE("revenue", "cart.checkout", false, true, true, false);

    private final String metric; private final String sourceTopic; private final boolean window; private final boolean daily; private final boolean revenue; private final boolean topProduct;
    MetricDefinition(String metric, String sourceTopic, boolean window, boolean daily, boolean revenue, boolean topProduct) {
        this.metric = metric; this.sourceTopic = sourceTopic; this.window = window; this.daily = daily; this.revenue = revenue; this.topProduct = topProduct;
    }
    public String metric() { return metric; }
    public String sourceTopic() { return sourceTopic; }
    public boolean hasWindow() { return window; }
    public boolean hasDaily() { return daily; }
    public boolean isRevenue() { return revenue; }
    public boolean isTopProduct() { return topProduct; }
    public static MetricDefinition fromName(String name) {
        for (MetricDefinition value : values()) if (value.metric.equals(name)) return value;
        throw new IllegalArgumentException("unsupported metric: " + name);
    }
}
```

- [ ] **Step 3: Implement count and top-product windows**

Create `flink/src/main/java/com/flinkdemo/level2/function/CountAggregate.java`:

```java
package com.flinkdemo.level2.function;

import com.flinkdemo.level2.model.EventEnvelope;
import org.apache.flink.api.common.functions.AggregateFunction;

public final class CountAggregate implements AggregateFunction<EventEnvelope, Long, Long> {
    public Long createAccumulator() { return 0L; }
    public Long add(EventEnvelope value, Long accumulator) { return accumulator + 1L; }
    public Long getResult(Long accumulator) { return accumulator; }
    public Long merge(Long left, Long right) { return left + right; }
}
```

Create `flink/src/main/java/com/flinkdemo/level2/function/CountWindowResult.java`:

```java
package com.flinkdemo.level2.function;

import com.flinkdemo.level2.model.WindowStat;
import java.time.Instant;
import java.util.Collections;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class CountWindowResult extends ProcessAllWindowFunction<Long, WindowStat, TimeWindow> {
    private final String metric;
    public CountWindowResult(String metric) { this.metric = metric; }
    @Override public void process(Context context, Iterable<Long> values, Collector<WindowStat> out) {
        out.collect(new WindowStat(metric, "window", Instant.ofEpochMilli(context.window().getEnd()).toString(), values.iterator().next(), Collections.emptyMap()));
    }
}
```

Create `flink/src/main/java/com/flinkdemo/level2/function/TopProductWindowFunction.java`:

```java
package com.flinkdemo.level2.function;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class TopProductWindowFunction extends ProcessAllWindowFunction<EventEnvelope, WindowStat, TimeWindow> {
    @Override public void process(Context context, Iterable<EventEnvelope> events, Collector<WindowStat> out) {
        Map<String, Long> counts = new HashMap<>();
        Map<String, String> names = new HashMap<>();
        for (EventEnvelope event : events) {
            String id = event.getPayload().path("product_id").asText();
            String name = event.getPayload().path("product_name").asText();
            if (!id.isBlank()) { counts.merge(id, 1L, Long::sum); names.put(id, name); }
        }
        String winner = counts.keySet().stream().sorted((a, b) -> {
            int countOrder = Long.compare(counts.get(b), counts.get(a));
            return countOrder != 0 ? countOrder : a.compareTo(b);
        }).findFirst().orElse(null);
        if (winner != null) out.collect(new WindowStat("top_product", "window", Instant.ofEpochMilli(context.window().getEnd()).toString(), counts.get(winner), Map.of("product_id", winner, "name", names.get(winner))));
    }
}
```

- [ ] **Step 4: Implement daily cumulative state**

Create `flink/src/main/java/com/flinkdemo/level2/function/DailyAggregateFunction.java`:

```java
package com.flinkdemo.level2.function;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class DailyAggregateFunction extends KeyedProcessFunction<String, EventEnvelope, WindowStat> {
    private final String metric; private final boolean revenue; private transient ValueState<Long> total;
    public DailyAggregateFunction(String metric, boolean revenue) { this.metric = metric; this.revenue = revenue; }
    @Override public void open(Configuration parameters) { total = getRuntimeContext().getState(new ValueStateDescriptor<>("daily-total", Long.class, 0L)); }
    @Override public void processElement(EventEnvelope event, Context context, Collector<WindowStat> out) throws Exception {
        long increment = revenue ? event.getPayload().path("total_amount").longValue() : 1L;
        long next = total.value() + increment;
        total.update(next);
        String windowEnd = LocalDate.parse(context.getCurrentKey()).plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toString();
        out.collect(new WindowStat(metric, "daily", windowEnd, next, Collections.emptyMap()));
    }
}
```

- [ ] **Step 5: Run function tests**

Run: `mvn -f flink/pom.xml -Dtest=MetricFunctionsTest test`
Expected: PASS with `Tests run: 3, Failures: 0, Errors: 0`.

- [ ] **Step 6: Commit**

```bash
git add flink/src/main/java/com/flinkdemo/level2/MetricDefinition.java flink/src/main/java/com/flinkdemo/level2/function flink/src/test/java/com/flinkdemo/level2/MetricFunctionsTest.java
git commit -m "feat: add Level 2 window and daily aggregators"
```

---

### Task 3: Kafka Wiring and Seven Independently Submitted Jobs

**Files:**
- Create: `flink/src/main/java/com/flinkdemo/level2/KafkaIO.java`
- Create: `flink/src/main/java/com/flinkdemo/level2/MetricJob.java`
- Test: `flink/src/test/java/com/flinkdemo/level2/MetricPipelineMiniClusterTest.java`

**Interfaces:**
- Produces: `KafkaIO.source(String brokers, MetricDefinition): KafkaSource<EventEnvelope>`.
- Produces: `KafkaIO.sink(String brokers): KafkaSink<WindowStat>`.
- Produces: `MetricJob.build(DataStream<EventEnvelope>, MetricDefinition): DataStream<WindowStat>` for MiniCluster testing and production assembly.
- CLI: `MetricJob --metric <name> --brokers <host:port>`.

- [ ] **Step 1: Write a failing MiniCluster pipeline test**

Create `flink/src/test/java/com/flinkdemo/level2/MetricPipelineMiniClusterTest.java`:

```java
package com.flinkdemo.level2;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

class MetricPipelineMiniClusterTest {
    @ClassRule public static final MiniClusterWithClientResource CLUSTER = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder().setNumberTaskManagers(1).setNumberSlotsPerTaskManager(2).build());

    @Test void dailyRevenuePipelineRunsInMiniClusterAndKeepsRupiahInteger() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EventEnvelope first = new EventEnvelope("e1", "cart.checkout", "b1", "buyer", "2026-07-18T10:00:00Z", mapper.readTree("{\"total_amount\":150000}"));
        EventEnvelope second = new EventEnvelope("e2", "cart.checkout", "b2", "buyer", "2026-07-18T10:00:01Z", mapper.readTree("{\"total_amount\":339000}"));
        DataStream<EventEnvelope> input = env.fromCollection(List.of(first, second));
        List<Long> values = MetricJob.build(input, MetricDefinition.REVENUE).executeAndCollect(2).stream().map(stat -> stat.getValue()).collect(Collectors.toList());
        assertTrue(values.contains(489000L));
    }
}
```

Run: `mvn -f flink/pom.xml -Dtest=MetricPipelineMiniClusterTest test`
Expected: FAIL during compilation because `MetricJob.build` does not exist.

- [ ] **Step 2: Implement Kafka source and sink factories**

Create `flink/src/main/java/com/flinkdemo/level2/KafkaIO.java`:

```java
package com.flinkdemo.level2;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import com.flinkdemo.level2.serde.EventEnvelopeSchema;
import com.flinkdemo.level2.serde.WindowStatSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class KafkaIO {
    private KafkaIO() {}
    public static KafkaSource<EventEnvelope> source(String brokers, MetricDefinition definition) {
        return KafkaSource.<EventEnvelope>builder().setBootstrapServers(brokers).setTopics(definition.sourceTopic()).setGroupId("flink-level2-" + definition.metric()).setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)).setValueOnlyDeserializer(new EventEnvelopeSchema()).build();
    }
    public static KafkaSink<WindowStat> sink(String brokers) {
        return KafkaSink.<WindowStat>builder().setBootstrapServers(brokers).setRecordSerializer(new WindowStatSchema("flink.window.stats")).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
    }
}
```

- [ ] **Step 3: Implement the common entry point and exact per-metric graph**

Create `flink/src/main/java/com/flinkdemo/level2/MetricJob.java`:

```java
package com.flinkdemo.level2;

import com.flinkdemo.level2.function.CountAggregate;
import com.flinkdemo.level2.function.CountWindowResult;
import com.flinkdemo.level2.function.DailyAggregateFunction;
import com.flinkdemo.level2.function.TopProductWindowFunction;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public final class MetricJob {
    private MetricJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        MetricDefinition definition = MetricDefinition.fromName(parameters.getRequired("metric"));
        String brokers = parameters.get("brokers", "kafka:9092");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(Duration.ofSeconds(30).toMillis());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        DataStream<EventEnvelope> source = env.fromSource(KafkaIO.source(brokers, definition), WatermarkStrategy.noWatermarks(), definition.metric() + "-source");
        build(source, definition).sinkTo(KafkaIO.sink(brokers)).name(definition.metric() + "-kafka-sink");
        env.execute("level2-" + definition.metric());
    }

    public static DataStream<WindowStat> build(DataStream<EventEnvelope> source, MetricDefinition definition) {
        DataStream<WindowStat> output = null;
        if (definition.hasWindow()) {
            output = definition.isTopProduct()
                ? source.windowAll(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(5))).process(new TopProductWindowFunction()).name("top-product-window")
                : source.windowAll(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(5))).aggregate(new CountAggregate(), new CountWindowResult(definition.metric())).name(definition.metric() + "-window");
        }
        if (definition.hasDaily()) {
            DataStream<WindowStat> daily = source.keyBy(event -> Instant.parse(event.getTimestamp()).atZone(ZoneOffset.UTC).toLocalDate().toString()).process(new DailyAggregateFunction(definition.metric(), definition.isRevenue())).name(definition.metric() + "-daily");
            output = output == null ? daily : output.union(daily);
        }
        if (output == null) throw new IllegalStateException("metric has no pipeline: " + definition.metric());
        return output;
    }
}
```

- [ ] **Step 4: Run all Java tests and package the shaded JAR**

Run: `mvn -f flink/pom.xml clean verify`
Expected: BUILD SUCCESS; all six tests pass; `flink/target/level2-jobs.jar` exists.

Run: `jar tf flink/target/level2-jobs.jar | grep -E 'MetricJob|KafkaSource'`
Expected: output includes `com/flinkdemo/level2/MetricJob.class` and Kafka connector classes, proving the connector is shaded while Flink core remains provided.

- [ ] **Step 5: Commit**

```bash
git add flink/src/main/java/com/flinkdemo/level2/KafkaIO.java flink/src/main/java/com/flinkdemo/level2/MetricJob.java flink/src/test/java/com/flinkdemo/level2/MetricPipelineMiniClusterTest.java
git commit -m "feat: wire independently submitted Flink metric jobs"
```

---

### Task 4: Supply Top-Product Names and Correct Rupiah Domain Documentation

**Files:**
- Modify: `app/internal/buyer/handler.go:64-69`
- Modify: `app/internal/buyer/handler_test.go`
- Modify: `app/internal/product/store.go:9-15`
- Modify: `app/internal/order/store.go:26-41`

**Interfaces:**
- Produces: `cartItemPayload(p *product.Product, quantity int) map[string]any`.
- Changes `cart.item.added.payload` to include `product_name: string` while preserving all existing keys.
- Clarifies that `Product.Price`, `OrderItem.UnitPrice`, and `Order.TotalAmount` are whole Rupiah integers; JSON types do not change.

- [ ] **Step 1: Add a failing payload-construction test**

Append to `app/internal/buyer/handler_test.go`:

```go
func TestCartItemPayloadIncludesProductIdentity(t *testing.T) {
	p := &product.Product{ID: "p1", Name: "Widget", SellerID: "seller1"}
	payload := cartItemPayload(p, 2)
	assert.Equal(t, "p1", payload["product_id"])
	assert.Equal(t, "Widget", payload["product_name"])
	assert.Equal(t, "seller1", payload["seller_id"])
	assert.Equal(t, 2, payload["quantity"])
}
```

Run: `cd app && go test ./internal/buyer -run TestCartItemPayloadIncludesProductIdentity -v`
Expected: FAIL during compilation because `cartItemPayload` is undefined.

- [ ] **Step 2: Add the tested payload helper and use it**

Add to `app/internal/buyer/handler.go` below `addToCartRequest`:

```go
func cartItemPayload(p *product.Product, quantity int) map[string]any {
	return map[string]any{
		"product_id":   p.ID,
		"product_name": p.Name,
		"seller_id":    p.SellerID,
		"quantity":     quantity,
	}
}
```

Replace the `cart.item.added` construction with:

```go
ev := event.NewEvent("cart.item.added", claims.Name, "buyer", cartItemPayload(p, req.Quantity))
```

Replace the three misleading field comments with these exact declarations:

```go
Price    int `json:"price"`
```

```go
UnitPrice int `json:"unit_price"`
```

```go
TotalAmount int `json:"total_amount"`
```

The UI already labels and formats these integer values as Rupiah, so no conversion or schema migration is permitted.

- [ ] **Step 3: Run Go tests**

Run: `cd app && go test ./...`
Expected: PASS for every Go package.

- [ ] **Step 4: Commit**

```bash
git add app/internal/buyer/handler.go app/internal/buyer/handler_test.go app/internal/product/store.go app/internal/order/store.go
git commit -m "fix: preserve product names and Rupiah values in metrics"
```

---

### Task 5: Go Output-Topic Consumer and Dashboard-Only WebSocket Forwarding

**Files:**
- Modify: `app/internal/kafkaclient/consumer.go`
- Create: `app/internal/kafkaclient/consumer_test.go`
- Modify: `app/internal/ws/hub.go`
- Modify: `app/internal/ws/hub_test.go`

**Interfaces:**
- Extends `Broadcaster` with `BroadcastRaw(data []byte)`.
- Produces: `Consumer.forward(topic string, value []byte) error`, allowing deterministic unit tests without Kafka.
- Produces: `Hub.BroadcastRaw(data []byte)`; raw output is sent only to clients whose role is `dashboard`.
- `Consumer.Start` consumes all six input topics plus `flink.window.stats`; `flink.cep.alerts` remains Phase 4 scope.

- [ ] **Step 1: Write failing consumer forwarding tests**

Create `app/internal/kafkaclient/consumer_test.go`:

```go
package kafkaclient

import (
	"encoding/json"
	"testing"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingBroadcaster struct {
	events []event.EventEnvelope
	raw    [][]byte
}

func (r *recordingBroadcaster) Broadcast(ev event.EventEnvelope) { r.events = append(r.events, ev) }
func (r *recordingBroadcaster) BroadcastRaw(data []byte) { r.raw = append(r.raw, append([]byte(nil), data...)) }

func TestConsumerForwardsInputAsTypedEventAndFlinkOutputAsRawJSON(t *testing.T) {
	recorder := &recordingBroadcaster{}
	consumer := NewConsumer("localhost:9092", recorder)
	input, _ := json.Marshal(event.NewEvent("product.listed", "seller", "seller", map[string]any{"product_id": "p1"}))
	require.NoError(t, consumer.forward("product.listed", input))
	require.NoError(t, consumer.forward("flink.window.stats", []byte(`{"metric":"tx_count","scope":"window","window_end":"2026-07-18T10:05:00Z","value":7,"detail":{}}`)))
	assert.Len(t, recorder.events, 1)
	assert.Len(t, recorder.raw, 1)
	assert.JSONEq(t, `{"metric":"tx_count","scope":"window","window_end":"2026-07-18T10:05:00Z","value":7,"detail":{}}`, string(recorder.raw[0]))
}

func TestConsumerRejectsMalformedJSON(t *testing.T) {
	consumer := NewConsumer("localhost:9092", &recordingBroadcaster{})
	assert.Error(t, consumer.forward("flink.window.stats", []byte(`not-json`)))
}
```

Run: `cd app && go test ./internal/kafkaclient -run TestConsumer -v`
Expected: FAIL because `BroadcastRaw` and `forward` do not exist.

- [ ] **Step 2: Refactor the consumer around the tested forwarding seam**

Replace `app/internal/kafkaclient/consumer.go` with:

```go
package kafkaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/segmentio/kafka-go"
)

type Broadcaster interface {
	Broadcast(ev event.EventEnvelope)
	BroadcastRaw(data []byte)
}

type Consumer struct { addr string; broadcaster Broadcaster }

func NewConsumer(addr string, hub Broadcaster) *Consumer { return &Consumer{addr: addr, broadcaster: hub} }

func (c *Consumer) Start(ctx context.Context) error {
	topics := []string{"product.listed", "cart.item.added", "cart.checkout", "order.confirmed", "shipment.picked", "shipment.delivered", "flink.window.stats"}
	for _, topic := range topics { go c.consumeTopic(ctx, topic) }
	<-ctx.Done()
	return ctx.Err()
}

func (c *Consumer) consumeTopic(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{c.addr}, Topic: topic, GroupID: "ws-hub-" + topic})
	defer reader.Close()
	slog.Info("kafka consumer started", "topic", topic)
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil { return }
			slog.Error("failed to read kafka message", "topic", topic, "error", err)
			continue
		}
		if err := c.forward(topic, msg.Value); err != nil { slog.Error("failed to forward kafka message", "topic", topic, "error", err) }
	}
}

func (c *Consumer) forward(topic string, value []byte) error {
	if !json.Valid(value) { return fmt.Errorf("invalid JSON on %s", topic) }
	if topic == "flink.window.stats" {
		c.broadcaster.BroadcastRaw(value)
		slog.Debug("flink result consumed", "topic", topic)
		return nil
	}
	var ev event.EventEnvelope
	if err := json.Unmarshal(value, &ev); err != nil { return fmt.Errorf("decode event on %s: %w", topic, err) }
	c.broadcaster.Broadcast(ev)
	slog.Debug("kafka event consumed", "topic", topic, "event_type", ev.EventType, "event_id", ev.EventID)
	return nil
}
```

- [ ] **Step 3: Add a dashboard-only raw channel to the hub**

In `app/internal/ws/hub.go`, add `raw chan []byte` beside `broadcast`, initialize it with `make(chan []byte, 100)`, and add this exact `Run` case:

```go
case data := <-h.raw:
	h.mu.RLock()
	for client := range h.clients {
		if client.Role != "dashboard" { continue }
		message := append([]byte(nil), data...)
		select {
		case client.send <- message:
		default:
		}
	}
	h.mu.RUnlock()
```

Add this method:

```go
func (h *Hub) BroadcastRaw(data []byte) {
	message := append([]byte(nil), data...)
	select {
	case h.raw <- message:
	default:
		slog.Warn("raw broadcast channel full, dropping event")
	}
}
```

- [ ] **Step 4: Test dashboard-only raw forwarding**

Append to `app/internal/ws/hub_test.go`:

```go
func TestBroadcastRawOnlyQueuesForDashboard(t *testing.T) {
	hub := NewHub()
	go hub.Run()
	defer hub.Close()
	dashboard := &Client{Name: "dash", Role: "dashboard", send: make(chan []byte, 1)}
	buyer := &Client{Name: "buyer", Role: "buyer", send: make(chan []byte, 1)}
	hub.Register <- dashboard
	hub.Register <- buyer
	hub.BroadcastRaw([]byte(`{"metric":"tx_count","scope":"window","window_end":"2026-07-18T10:05:00Z","value":7,"detail":{}}`))
	assert.Eventually(t, func() bool { return len(dashboard.send) == 1 }, time.Second, 10*time.Millisecond)
	assert.Empty(t, buyer.send)
}
```

Add `"time"` to that test file's imports.

Run: `cd app && go test ./internal/kafkaclient ./internal/ws -v`
Expected: PASS, including malformed JSON and dashboard-only assertions.

- [ ] **Step 5: Run the complete Go suite and commit**

Run: `cd app && go test ./...`
Expected: PASS for every package.

```bash
git add app/internal/kafkaclient/consumer.go app/internal/kafkaclient/consumer_test.go app/internal/ws/hub.go app/internal/ws/hub_test.go
git commit -m "feat: forward Flink metrics to dashboard WebSockets"
```

---

### Task 6: Typed React Messages, Level 2 Cards, and CSS History Bars

**Files:**
- Modify: `web/src/context/EventContext.tsx:3-10`
- Modify: `web/src/hooks/useWebSocket.ts:3-40`
- Replace: `web/src/pages/Dashboard.tsx`
- Modify: `web/src/index.css`

**Interfaces:**
- Produces: `WindowStat`, `DashboardMessage`, `isWindowStat(value): value is WindowStat`.
- Changes hook signature to `useWebSocket<T>(onEvent: (event: T) => void, overrideToken?: string | null)`.
- Dashboard retains at most 24 unique window points per metric and the latest unique daily value.

- [ ] **Step 1: Add the message union and generic WebSocket hook**

Append below `EventEnvelope` in `web/src/context/EventContext.tsx`:

```ts
export type MetricName = 'listings_count' | 'cart_adds_count' | 'tx_count' | 'confirmed_orders' | 'delivered_orders' | 'top_product' | 'revenue';
export interface WindowStat {
  metric: MetricName;
  scope: 'window' | 'daily';
  window_end: string;
  value: number;
  detail: Record<string, string>;
}
export type DashboardMessage = EventEnvelope | WindowStat;
export function isWindowStat(value: DashboardMessage): value is WindowStat {
  return 'metric' in value && 'scope' in value && 'window_end' in value;
}
```

Replace the hook declaration and parsed type in `web/src/hooks/useWebSocket.ts` with:

```ts
export function useWebSocket<T = EventEnvelope>(onEvent: (event: T) => void, overrideToken?: string | null) {
```

```ts
const event: T = JSON.parse(e.data) as T;
```

Run: `cd web && npm run build`
Expected: PASS; existing role pages infer `EventEnvelope` from their `addEvent` callbacks.

- [ ] **Step 2: Replace the dashboard with bounded Level 1 and Level 2 state**

Replace `web/src/pages/Dashboard.tsx` with:

```tsx
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useWebSocket } from '../hooks/useWebSocket';
import { useEvents, isWindowStat, type DashboardMessage, type EventEnvelope, type MetricName, type WindowStat } from '../context/EventContext';
import { createSession } from '../api/client';

const METRICS: Array<{ name: MetricName; label: string; window: boolean; daily: boolean; rupiah?: boolean }> = [
  { name: 'listings_count', label: 'Listings', window: true, daily: false },
  { name: 'cart_adds_count', label: 'Cart adds', window: true, daily: false },
  { name: 'tx_count', label: 'Checkouts', window: true, daily: true },
  { name: 'confirmed_orders', label: 'Confirmed', window: true, daily: false },
  { name: 'delivered_orders', label: 'Delivered', window: true, daily: true },
  { name: 'top_product', label: 'Top product', window: true, daily: false },
  { name: 'revenue', label: 'Revenue', window: false, daily: true, rupiah: true },
];

function formatValue(value: number | undefined, rupiah = false): string {
  if (value === undefined) return '—';
  return rupiah ? new Intl.NumberFormat('id-ID', { style: 'currency', currency: 'IDR', maximumFractionDigits: 0 }).format(value) : value.toLocaleString('id-ID');
}

function EventRow({ event }: { event: EventEnvelope }) {
  return <div className="event-row"><span>{new Date(event.timestamp).toLocaleTimeString()}</span><strong>{event.event_type}</strong><span>{event.actor_id}</span><span>{JSON.stringify(event.payload)}</span></div>;
}

function MetricChart({ points, rupiah }: { points: WindowStat[]; rupiah?: boolean }) {
  const max = Math.max(1, ...points.map((point) => point.value));
  return <div className="metric-chart" aria-label="Sliding window history">
    {points.length === 0 ? <span className="empty-chart">Waiting for a window…</span> : points.map((point) => <div key={point.window_end} className="metric-bar" title={`${new Date(point.window_end).toLocaleTimeString()} — ${formatValue(point.value, rupiah)}`} style={{ height: `${Math.max(8, point.value / max * 100)}%` }} />)}
  </div>;
}

export default function Dashboard() {
  const { events, addEvent, clearEvents } = useEvents();
  const [dashToken, setDashToken] = useState<string | null>(() => localStorage.getItem('dash_token'));
  const [stats, setStats] = useState<WindowStat[]>([]);
  const onMessage = useCallback((message: DashboardMessage) => {
    if (!isWindowStat(message)) { addEvent(message); return; }
    setStats((previous) => {
      const unique = previous.filter((item) => !(item.metric === message.metric && item.scope === message.scope && item.window_end === message.window_end));
      const next = [...unique, message];
      const windows = next.filter((item) => item.scope === 'window').sort((a, b) => a.window_end.localeCompare(b.window_end));
      const retainedWindows = METRICS.flatMap(({ name }) => windows.filter((item) => item.metric === name).slice(-24));
      const retainedDaily = METRICS.flatMap(({ name }) => next.filter((item) => item.metric === name && item.scope === 'daily').sort((a, b) => b.window_end.localeCompare(a.window_end)).slice(0, 1));
      return [...retainedWindows, ...retainedDaily];
    });
  }, [addEvent]);
  const { connected } = useWebSocket<DashboardMessage>(onMessage, dashToken);

  useEffect(() => {
    if (dashToken) return;
    createSession(`dashboard-${Math.random().toString(36).slice(2, 8)}`, 'dashboard').then((response) => { localStorage.setItem('dash_token', response.token); setDashToken(response.token); }).catch((error) => console.error('[dashboard] failed to create session', error));
  }, [dashToken]);

  const grouped = useMemo(() => Object.fromEntries(METRICS.map(({ name }) => [name, stats.filter((item) => item.metric === name)])) as Record<MetricName, WindowStat[]>, [stats]);

  return <main className="dashboard">
    <header className="dashboard-header"><h1>Stream Processing Dashboard</h1><div><span className={connected ? 'connection connected' : 'connection'}>{connected ? 'Connected' : dashToken ? 'Reconnecting…' : 'Connecting…'}</span><button onClick={() => { clearEvents(); setStats([]); }}>Clear</button></div></header>
    <section><h2>Level 1 — Live Event Feed</h2><p>Raw Kafka events, forwarded without stateful processing.</p><div className="event-feed">{events.length === 0 ? <div className="empty">Waiting for events…</div> : events.map((event) => <EventRow key={event.event_id} event={event} />)}</div></section>
    <section><h2>Level 2 — Stateful Aggregations</h2><p>Five-minute windows slide every five seconds; daily totals reset at UTC midnight.</p><div className="metric-grid">{METRICS.map((metric) => {
      const values = grouped[metric.name];
      const windows = values.filter((item) => item.scope === 'window');
      const latestWindow = windows.at(-1);
      const daily = values.find((item) => item.scope === 'daily');
      const topName = metric.name === 'top_product' ? latestWindow?.detail.name : undefined;
      return <article className="metric-card" key={metric.name}><h3>{metric.label}</h3><div className="metric-values"><div><span>5 min</span><strong>{formatValue(latestWindow?.value, metric.rupiah)}</strong></div><div><span>Today</span><strong>{formatValue(daily?.value, metric.rupiah)}</strong></div></div>{topName && <p className="metric-detail">{topName}</p>}{metric.window ? <MetricChart points={windows} rupiah={metric.rupiah} /> : <div className="metric-chart"><span className="empty-chart">Daily cumulative</span></div>}</article>;
    })}</div></section>
  </main>;
}
```

- [ ] **Step 3: Add responsive chart/card styles without a library**

Append to `web/src/index.css`:

```css
.dashboard { padding: 20px; max-width: 1500px; margin: 0 auto; }
.dashboard section { margin-bottom: 24px; }
.dashboard section > p { color: #6b7280; margin: 4px 0 12px; }
.dashboard-header, .dashboard-header > div, .metric-values { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.dashboard-header { margin-bottom: 20px; }
.connection { padding: 4px 12px; border-radius: 12px; background: #fee2e2; color: #dc2626; font-size: 12px; }
.connection.connected { background: #d1fae5; color: #059669; }
.dashboard-header button { padding: 6px 16px; }
.event-feed { background: white; border: 1px solid #e5e7eb; border-radius: 8px; max-height: 280px; overflow: auto; }
.event-row { display: grid; grid-template-columns: 90px 170px 110px 1fr; gap: 12px; padding: 8px 12px; border-bottom: 1px solid #e5e7eb; font: 13px monospace; }
.event-row span:first-child { color: #9ca3af; }
.empty { padding: 36px; text-align: center; color: #9ca3af; }
.metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }
.metric-card { min-width: 0; padding: 16px; background: white; border: 1px solid #e5e7eb; border-radius: 8px; }
.metric-values { margin: 12px 0; }
.metric-values div { display: flex; flex-direction: column; }
.metric-values span { color: #6b7280; font-size: 12px; }
.metric-values strong { font-size: 22px; }
.metric-detail { height: 20px; color: #7c3aed; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.metric-chart { height: 90px; display: flex; align-items: flex-end; gap: 3px; padding-top: 10px; border-bottom: 1px solid #d1d5db; }
.metric-bar { flex: 1; min-width: 3px; max-width: 14px; border-radius: 3px 3px 0 0; background: linear-gradient(#60a5fa, #2563eb); }
.empty-chart { margin: auto; color: #9ca3af; font-size: 12px; }
@media (max-width: 700px) { .event-row { grid-template-columns: 80px 1fr; } .event-row span:nth-child(n+3) { display: none; } }
```

- [ ] **Step 4: Run frontend quality gates**

Run: `cd web && npm run lint`
Expected: exits 0 with no lint errors.

Run: `cd web && npm run build`
Expected: TypeScript and Vite exit 0 and regenerate `app/web/dist`.

- [ ] **Step 5: Commit source and generated embedded assets**

```bash
git add web/src/context/EventContext.tsx web/src/hooks/useWebSocket.ts web/src/pages/Dashboard.tsx web/src/index.css app/web/dist
git commit -m "feat: render Level 2 metric cards and history charts"
```

---

### Task 7: Flink Session Cluster and One-Shot Submission Container

**Files:**
- Create: `flink/Dockerfile`
- Create: `flink/submit-jobs.sh`
- Modify: `docker-compose.yml`
- Modify: `Makefile`

**Interfaces:**
- Image `flink-job-submit` contains `/opt/flink/usrlib/level2-jobs.jar` and submits seven detached jobs through the Flink CLI, which uses the JobManager REST endpoint.
- JobManager REST/UI remains host port `8081`; app remains host port `15300`.
- TaskManager has eight slots, 1536 MiB process memory; JobManager has 768 MiB process memory.

- [ ] **Step 1: Create the shaded-JAR submission image**

Create `flink/Dockerfile`:

```dockerfile
FROM maven:3.9.9-eclipse-temurin-11 AS builder
WORKDIR /build
COPY flink/pom.xml ./pom.xml
RUN mvn -B dependency:go-offline
COPY flink/src ./src
RUN mvn -B clean verify

FROM flink:1.19.3-scala_2.12-java11
COPY --from=builder /build/target/level2-jobs.jar /opt/flink/usrlib/level2-jobs.jar
COPY flink/submit-jobs.sh /opt/flink/submit-jobs.sh
ENTRYPOINT ["/opt/flink/submit-jobs.sh"]
```

Create `flink/submit-jobs.sh`:

```sh
#!/bin/sh
set -eu
until curl --fail --silent http://flink-jobmanager:8081/overview >/dev/null; do sleep 2; done
for metric in listings_count cart_adds_count tx_count confirmed_orders delivered_orders top_product revenue; do
  /opt/flink/bin/flink run -d -m flink-jobmanager:8081 -c com.flinkdemo.level2.MetricJob /opt/flink/usrlib/level2-jobs.jar --metric "$metric" --brokers kafka:9092
done
```

Run: `chmod +x flink/submit-jobs.sh`
Expected: file mode becomes executable.

- [ ] **Step 2: Extend Compose with a resource-conscious session cluster**

Append these services under `services:` in `docker-compose.yml`:

```yaml
  flink-jobmanager:
    image: flink:1.19.3-scala_2.12-java11
    command: jobmanager
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        jobmanager.memory.process.size: 768m
        restart-strategy.type: fixed-delay
        restart-strategy.fixed-delay.attempts: 3
        restart-strategy.fixed-delay.delay: 10s
    ports:
      - "8081:8081"
    healthcheck:
      test: ["CMD-SHELL", "curl --fail --silent http://localhost:8081/overview >/dev/null"]
      interval: 5s
      timeout: 3s
      retries: 20

  flink-taskmanager:
    image: flink:1.19.3-scala_2.12-java11
    command: taskmanager
    depends_on:
      flink-jobmanager:
        condition: service_healthy
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 8
        taskmanager.memory.process.size: 1536m

  flink-job-submit:
    build:
      context: .
      dockerfile: flink/Dockerfile
    depends_on:
      kafka:
        condition: service_healthy
      flink-jobmanager:
        condition: service_healthy
      flink-taskmanager:
        condition: service_started
    restart: "no"
```

Keep the existing Zookeeper, Kafka, and app blocks byte-for-byte except dependencies already shown. Validate indentation rather than replacing the existing services.

- [ ] **Step 3: Add Maven commands to the Makefile**

Change `.PHONY` to include `build-flink test-flink verify`, then add:

```make
build-flink:
	mvn -f flink/pom.xml clean package

test-flink:
	mvn -f flink/pom.xml test

verify:
	cd app && go test ./...
	cd web && npm run lint
	cd web && npm run build
	mvn -f flink/pom.xml clean verify
```

Run: `docker compose config --quiet`
Expected: exits 0 with no Compose schema error.

Run: `mvn -f flink/pom.xml clean verify`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Build and inspect the submission image**

Run: `docker compose build flink-job-submit`
Expected: image builds successfully and Maven reports BUILD SUCCESS in the builder stage.

Run: `docker compose run --rm --entrypoint sh flink-job-submit -c 'test -x /opt/flink/submit-jobs.sh && test -f /opt/flink/usrlib/level2-jobs.jar'`
Expected: exits 0.

- [ ] **Step 5: Commit**

```bash
git add flink/Dockerfile flink/submit-jobs.sh docker-compose.yml Makefile
git commit -m "feat: deploy Level 2 jobs to a Flink session cluster"
```

---

### Task 8: End-to-End Compose Smoke Test

**Files:**
- Create: `scripts/phase3-smoke.sh`

**Interfaces:**
- Script starts the stack, confirms exactly seven running Flink jobs, performs list → add → checkout through the REST API, and verifies `tx_count`, `revenue`, and `top_product` envelopes on `flink.window.stats`.
- Script leaves the stack running on success for inspection and prints diagnostics before failing.

- [ ] **Step 1: Create the executable smoke script**

Create `scripts/phase3-smoke.sh`:

```sh
#!/bin/sh
set -eu
base=http://localhost:15300
suffix=$(date +%s)
fail() { docker compose logs --no-color flink-jobmanager flink-taskmanager flink-job-submit app; exit 1; }
docker compose up -d --build || fail
for attempt in $(seq 1 60); do
  if curl --fail --silent "$base/api/health" >/dev/null && [ "$(curl --silent http://localhost:8081/jobs/overview | jq '[.jobs[] | select(.state == "RUNNING")] | length')" -eq 7 ]; then break; fi
  [ "$attempt" -eq 60 ] && fail
  sleep 2
done
seller=$(curl --fail --silent -H 'Content-Type: application/json' -d "{\"name\":\"seller-$suffix\",\"role\":\"seller\"}" "$base/api/session" | jq -r .token)
buyer=$(curl --fail --silent -H 'Content-Type: application/json' -d "{\"name\":\"buyer-$suffix\",\"role\":\"buyer\"}" "$base/api/session" | jq -r .token)
product=$(curl --fail --silent -H "Authorization: Bearer $seller" -H 'Content-Type: application/json' -d '{"name":"Widget","price":489000,"quantity":10}' "$base/api/seller/products")
product_id=$(printf '%s' "$product" | jq -r .id)
curl --fail --silent -H "Authorization: Bearer $buyer" -H 'Content-Type: application/json' -d "{\"product_id\":\"$product_id\",\"quantity\":1}" "$base/api/buyer/cart/items" >/dev/null
curl --fail --silent -H "Authorization: Bearer $buyer" -H 'Content-Type: application/json' -d "{\"items\":[{\"product_id\":\"$product_id\",\"quantity\":1}],\"shipping_address\":\"Jakarta\"}" "$base/api/buyer/cart/checkout" >/dev/null
output=$(docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic flink.window.stats --from-beginning --timeout-ms 20000 2>/dev/null || true)
printf '%s\n' "$output" | jq -e -s 'any(.[]; .metric == "tx_count" and .scope == "window" and .value >= 1)' >/dev/null || fail
printf '%s\n' "$output" | jq -e -s 'any(.[]; .metric == "revenue" and .scope == "daily" and .value >= 489000)' >/dev/null || fail
printf '%s\n' "$output" | jq -e -s --arg id "$product_id" 'any(.[]; .metric == "top_product" and .scope == "window" and .detail.product_id == $id and .detail.name == "Widget")' >/dev/null || fail
printf '%s\n' 'Phase 3 smoke test passed: seven jobs running and Level 2 metrics observed.'
```

Run: `chmod +x scripts/phase3-smoke.sh`
Expected: file mode becomes executable.

- [ ] **Step 2: Run the smoke test**

Prerequisite check: `command -v docker && command -v curl && command -v jq`
Expected: prints paths for all three commands.

Run: `./scripts/phase3-smoke.sh`
Expected: `Phase 3 smoke test passed: seven jobs running and Level 2 metrics observed.`

Run: `curl --silent http://localhost:8081/jobs/overview | jq -r '.jobs[] | [.name,.state] | @tsv' | sort`
Expected: seven lines named `level2-<metric>`, each in `RUNNING` state.

- [ ] **Step 3: Commit**

```bash
git add scripts/phase3-smoke.sh
git commit -m "test: cover Level 2 metrics through Docker Compose"
```

---

### Task 9: Final Verification and Operator-Facing Checks

**Files:**
- Modify only files required to fix a failing check; do not broaden scope.

**Interfaces:**
- Produces a reproducible green `make verify` and valid Compose model.
- Confirms no chart package was added and no non-integer currency conversion exists.

- [ ] **Step 1: Run all local quality gates**

Run: `make verify`
Expected: Go tests PASS, oxlint exits 0, TypeScript/Vite build succeeds, and Maven reports BUILD SUCCESS.

- [ ] **Step 2: Validate packaging and dependency constraints**

Run: `docker compose config --quiet && test -f flink/target/level2-jobs.jar`
Expected: exits 0.

Run: `git diff -- web/package.json web/package-lock.json`
Expected: no output, proving no chart library was added.

Run: `grep -R "\/ 100\|\* 100" app web/src flink/src/main || true`
Expected: no output, proving Rupiah values are not converted to or from cents.

- [ ] **Step 3: Manually inspect the live teaching view**

Open `http://localhost:15300/dashboard` after the smoke test and verify these exact outcomes:

```text
Level 1 raw events still appear.
Level 2 shows seven metric cards.
Window-only metrics show an em dash for Today.
Revenue shows an em dash for 5 min and an IDR daily value with no decimal fraction.
Checkouts and delivered orders can show both 5 min and Today values.
Top product shows Widget and a history bar.
Flink Web UI shows seven RUNNING jobs and one TaskManager.
```

Expected: every line is true; browser console has no parsing or WebSocket errors.

- [ ] **Step 4: Confirm the implementation commits are clean**

Run: `git status --short`
Expected: no tracked source changes remain after the task commits; local-only untracked files that predated execution are left untouched.

---

## Self-Review Record

- **Spec coverage:** Tasks 1–3 cover the generalized envelope, seven one-topic logical jobs, five-minute/five-second windows, daily `tx_count`, `revenue`, and `delivered_orders`, Kafka source/sink, shaded JAR, and Flink tests. Tasks 4–6 cover product detail, integer Rupiah values, Go output consumption, dashboard-only WebSocket delivery, cards, daily values, and bounded bar history. Tasks 7–9 cover the session cluster, one-shot submission, small-VPS sizing, Compose startup, integration test, and all quality gates.
- **Deliberate interpretation:** Daily boundaries are UTC because event timestamps are UTC and the spec does not name a business timezone. `cart_adds_count` and `top_product` count add events, matching the metric names and example value semantics; quantity remains available in the input but is not summed.
- **Type consistency:** Java output `long` serializes as a JSON integer; Go forwards bytes without decoding numbers; TypeScript receives `number`; IDR formatting has zero fractional digits. `detail` is `Map<String,String>` / `Record<string,string>` throughout.
- **Scope consistency:** Level 3 consumption is not started, frontend tests are not introduced, no database or chart dependency is added, and existing Level 1 filtering remains typed and unchanged.
- **Operational trade-off:** Processing-time windows produce a result only for windows containing events; the dashboard intentionally keeps an empty state instead of synthesizing zeros. Seven one-slot jobs require eight TaskManager slots, leaving one spare for restart overlap.
