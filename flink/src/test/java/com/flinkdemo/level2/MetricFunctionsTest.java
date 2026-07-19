package com.flinkdemo.level2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.function.CountAggregate;
import com.flinkdemo.level2.function.CountWindowResult;
import com.flinkdemo.level2.function.DailyAggregateFunction;
import com.flinkdemo.level2.function.TopProductWindowFunction;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.CloseableIterator;
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

    @Test void definitionsKeepAllMetricFlags() {
        assertMetricFlags("listings_count", "product.listed", true, false, false, false);
        assertMetricFlags("cart_adds_count", "cart.item.added", true, false, false, false);
        assertMetricFlags("tx_count", "cart.checkout", true, true, false, false);
        assertMetricFlags("confirmed_orders", "order.confirmed", true, false, false, false);
        assertMetricFlags("delivered_orders", "shipment.delivered", true, true, false, false);
        assertMetricFlags("top_product", "cart.item.added", true, false, false, true);
        assertMetricFlags("revenue", "cart.checkout", false, true, true, false);
    }

    private void assertMetricFlags(String name, String topic, boolean window, boolean daily, boolean revenue, boolean topProduct) {
        MetricDefinition definition = MetricDefinition.fromName(name);
        assertEquals(topic, definition.sourceTopic(), "Topic mismatch for " + name);
        assertEquals(window, definition.hasWindow(), "Window flag mismatch for " + name);
        assertEquals(daily, definition.hasDaily(), "Daily flag mismatch for " + name);
        assertEquals(revenue, definition.isRevenue(), "Revenue flag mismatch for " + name);
        assertEquals(topProduct, definition.isTopProduct(), "TopProduct flag mismatch for " + name);
    }

    @Test void countAggregateCountsEventsRatherThanQuantity() throws Exception {
        CountAggregate aggregate = new CountAggregate();
        long accumulator = aggregate.add(event("e1", "cart.item.added", "2026-07-18T10:00:00Z", "{\"quantity\":9}"), aggregate.createAccumulator());
        assertEquals(1L, accumulator);
    }

    @Test void countWindowResultEmitsAnIsoUtcWindowEnd() {
        long start = Instant.parse("2026-07-18T10:00:00Z").toEpochMilli();
        TimeWindow window = new TimeWindow(start, start + 300_000L);
        List<WindowStat> output = new ArrayList<>();

        new CountWindowResult("tx_count").process(window, List.of(5L), collector(output));

        assertEquals(1, output.size());
        WindowStat stat = output.get(0);
        assertEquals("tx_count", stat.getMetric());
        assertEquals("window", stat.getScope());
        assertEquals("2026-07-18T10:05:00Z", stat.getWindowEnd());
        assertEquals(5L, stat.getValue());
        assertEquals(0, stat.getDetail().size());
    }

    @Test void dailyAggregateKeepsIndependentUtcDayCounts() throws Exception {
        List<WindowStat> output = runDaily("tx_count", false, List.of(
            event("e1", "cart.checkout", "2026-07-18T10:00:00Z", "{}"),
            event("e2", "cart.checkout", "2026-07-18T11:00:00Z", "{}"),
            event("e3", "cart.checkout", "2026-07-19T09:00:00Z", "{}")));

        assertEquals(List.of(1L, 2L, 1L), output.stream().map(WindowStat::getValue).collect(Collectors.toList()));
        assertEquals(List.of("2026-07-19T00:00:00Z", "2026-07-19T00:00:00Z", "2026-07-20T00:00:00Z"), output.stream().map(WindowStat::getWindowEnd).collect(Collectors.toList()));
    }

    @Test void dailyAggregateSumsWholeRupiahAmounts() throws Exception {
        List<WindowStat> output = runDaily("revenue", true, List.of(
            event("e1", "cart.checkout", "2026-07-18T10:00:00Z", "{\"total_amount\":489000}"),
            event("e2", "cart.checkout", "2026-07-18T11:00:00Z", "{\"total_amount\":1000}"),
            event("e3", "cart.checkout", "2026-07-19T09:00:00Z", "{\"total_amount\":50000}")));

        assertEquals(List.of(489000L, 490000L, 50000L), output.stream().map(WindowStat::getValue).collect(Collectors.toList()));
    }

    private List<WindowStat> runDaily(String metric, boolean revenue, List<EventEnvelope> events) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        List<WindowStat> output = new ArrayList<>();
        try (CloseableIterator<WindowStat> iterator = environment
            .fromData(events)
            .keyBy(event -> LocalDate.parse(event.getTimestamp().substring(0, 10)).toString())
            .process(new DailyAggregateFunction(metric, revenue))
            .executeAndCollect("daily-aggregate-test")) {
            while (iterator.hasNext()) output.add(iterator.next());
        }
        return output;
    }

    @Test void topProductUsesStableTieBreakAndRequiredDetailKeys() throws Exception {
        List<EventEnvelope> events = List.of(
            event("e1", "cart.item.added", "2026-07-18T10:00:00Z", "{\"product_id\":\"p2\",\"product_name\":\"B\"}"),
            event("e2", "cart.item.added", "2026-07-18T10:00:01Z", "{\"product_id\":\"p1\",\"product_name\":\"A\"}"));
        List<WindowStat> output = new ArrayList<>();
        new TopProductWindowFunction().process(new TimeWindow(0L, 5000L), events, collector(output));
        assertEquals("p1", output.get(0).getDetail().get("product_id"));
        assertEquals("A", output.get(0).getDetail().get("name"));
        assertEquals(Map.of("product_id", "p1", "name", "A"), output.get(0).getDetail());
        assertEquals(1L, output.get(0).getValue());
    }

    private static <T> Collector<T> collector(List<T> values) {
        return new Collector<>() {
            @Override public void collect(T value) { values.add(value); }
            @Override public void close() {}
        };
    }
}
