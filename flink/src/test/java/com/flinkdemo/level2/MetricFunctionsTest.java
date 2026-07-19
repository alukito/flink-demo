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
