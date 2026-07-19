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
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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

    @Test void testMetricDefinitionFlags() {
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

    @Test void countWindowResultOutput() throws Exception {
        String metricName = "test_metric";
        Long expectedValue = 5L;
        String expectedWindowEnd = "2026-07-18T10:05:00Z"; // Assuming window ends 5 seconds after start

        List<Long> values = List.of(expectedValue);
        List<WindowStat> output = new ArrayList<>();
        
        ProcessAllWindowFunction<Long, WindowStat, TimeWindow> function = new CountWindowResult(metricName);
        TimeWindow window = new TimeWindow(0L, 5000L); // 0 to 5 seconds

        function.process(new ProcessAllWindowFunction.Context() {
            @Override public TimeWindow window() { return window; }
            @Override public org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context.WindowTimerEvent getEvent() { return null; }
        }, values, collector(output));

        assertEquals(1, output.size());
        WindowStat stat = output.get(0);
        assertEquals(metricName, stat.getMetric());
        assertEquals("window", stat.getScope());
        assertEquals(expectedWindowEnd, stat.getWindowEnd());
        assertEquals(expectedValue, stat.getValue());
        assertEquals(Collections.emptyMap(), stat.getDetail());
    }

    @Test void dailyAggregateFunctionDailyTotals() throws Exception {
        DailyAggregateFunction function = new DailyAggregateFunction("tx_count", false);
        
        // Mock context for KeyedProcessFunction
        var mockContext = new MockKeyedProcessFunctionContext("2026-07-18");
        function.open(mockContext.getRuntimeContext()); // Initialize state

        // Event 1: Day 1, count 1
        function.processElement(event("e1", "cart.checkout", "2026-07-18T10:00:00Z", "{}"), mockContext, mockContext.getCollector());
        // Expect output for Day 1, with total 1. WindowEnd should be start of Day 2 UTC.
        assertEquals("2026-07-19T00:00:00Z", mockContext.getCollector().getOutput().get(0).getWindowEnd());
        assertEquals(1L, mockContext.getCollector().getOutput().get(0).getValue());
        
        // Event 2: Day 1 again, count 1 more
        function.processElement(event("e2", "cart.checkout", "2026-07-18T11:00:00Z", "{}"), mockContext, mockContext.getCollector());
        // Expect output for Day 1, with total 2.
        assertEquals(2L, mockContext.getCollector().getOutput().get(1).getValue());

        // Event 3: Day 2, count 1
        mockContext.setCurrentKey("2026-07-19"); // Simulate key change for new day
        function.processElement(event("e3", "cart.checkout", "2026-07-19T09:00:00Z", "{}"), mockContext, mockContext.getCollector());
        // Expect output for Day 2, with total 1. WindowEnd should be start of Day 3 UTC.
        assertEquals("2026-07-20T00:00:00Z", mockContext.getCollector().getOutput().get(2).getWindowEnd());
        assertEquals(1L, mockContext.getCollector().getOutput().get(1).getValue()); // Re-check previous value persists
        assertEquals(1L, mockContext.getCollector().getOutput().get(2).getValue());
    }

     @Test void dailyAggregateFunctionRevenue() throws Exception {
        DailyAggregateFunction function = new DailyAggregateFunction("revenue", true);
        var mockContext = new MockKeyedProcessFunctionContext("2026-07-18");
        function.open(mockContext.getRuntimeContext());

        // Event 1: Day 1, revenue 489,000
        function.processElement(event("e1", "cart.checkout", "2026-07-18T10:00:00Z", "{\"total_amount\":489000}"), mockContext, mockContext.getCollector());
        assertEquals(489000L, mockContext.getCollector().getOutput().get(0).getValue());

        // Event 2: Day 1, revenue 1000
        function.processElement(event("e2", "cart.checkout", "2026-07-18T11:00:00Z", "{\"total_amount\":1000}"), mockContext, mockContext.getCollector());
        assertEquals(490000L, mockContext.getCollector().getOutput().get(1).getValue());

        // Event 3: Day 2, revenue 50000
        mockContext.setCurrentKey("2026-07-19");
        function.processElement(event("e3", "cart.checkout", "2026-07-19T09:00:00Z", "{\"total_amount\":50000}"), mockContext, mockContext.getCollector());
        assertEquals(50000L, mockContext.getCollector().getOutput().get(2).getValue());
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
    
    // Mock classes for testing KeyedProcessFunction and ProcessAllWindowFunction
    static class MockKeyedProcessFunctionContext implements org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context {
        private String currentKey;
        private final List<WindowStat> collectedOutput = new ArrayList<>();
        private final java.util.Map<String, ValueState<Long>> state = new java.util.HashMap<>();

        public MockKeyedProcessFunctionContext(String key) { this.currentKey = key; }

        public List<WindowStat> getOutput() { return collectedOutput; }
        public void setCurrentKey(String key) { this.currentKey = key; }
        public Collector<WindowStat> getCollector() { return collectedOutput::add; }

        @Override public String getCurrentKey() { return currentKey; }
        @Override public long getProcessingTime() { return 0; } // Not used in this test
        @Override public org.apache.flink.streaming.api.functions.KeyedProcessFunction.Context.WindowTimerEvent getEvent() { return null; } // Not used in this test
        
        @Override public <T> ValueState<T> getPartitionedState(ValueStateDescriptor<T> stateDescriptor) {
             // Simulate state management: use HashMap to store state per key
             @SuppressWarnings("unchecked") ValueState<T> valueState = (ValueState<T>) state.computeIfAbsent(stateDescriptor.getName(), k -> new MockValueState<>(stateDescriptor.getDefaultValue()));
             return valueState;
        }
        
        // Mock ValueState implementation
        static class MockValueState<T> implements ValueState<T> {
            private T value;

            MockValueState(T defaultValue) { this.value = defaultValue; }

            @Override public T value() { return value; }
            @Override public void update(T value) { this.value = value; }
            @Override public void clear() { this.value = null; } // Or set to default value
        }
    }

    private static <T> Collector<T> collector(List<T> values) {
        return new Collector<>() { public void collect(T value) { values.add(value); } public void close() {} };
    }
}
