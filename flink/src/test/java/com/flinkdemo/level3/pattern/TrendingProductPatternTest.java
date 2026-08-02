package com.flinkdemo.level3.pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level3.model.CepAlert;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class TrendingProductPatternTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void emitsProductFocusedAlertForThreeDistinctBuyersWithinOneMinute() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "buyer-1", "2026-08-01T10:00:00Z"),
            event("added-2", "buyer-2", "2026-08-01T10:00:20Z"),
            event("added-3", "buyer-3", "2026-08-01T10:00:40Z")));

        assertEquals(1, alerts.size());
        CepAlert alert = alerts.get(0);
        assertEquals("trending_product:product-1:2026-08-01T10:00:00Z", alert.getAlertId());
        assertEquals("trending_product", alert.getPattern());
        assertEquals("product-1", alert.getDetail().get("product_id"));
        assertEquals("Demo Product", alert.getDetail().get("product_name"));
        assertEquals(3, alert.getDetail().get("qualifying_count"));
        assertEquals(false, alert.getDetail().containsKey("buyer"));
        assertEquals(false, alert.getDetail().containsKey("buyers"));
    }

    @Test
    void doesNotEmitForOnlyTwoDistinctBuyers() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "buyer-1", "2026-08-01T10:00:00Z"),
            event("added-2", "buyer-2", "2026-08-01T10:00:20Z")));

        assertEquals(List.of(), alerts);
    }

    @Test
    void doesNotCountRepeatedAdditionsByOneBuyerTowardTheThreshold() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "buyer-1", "2026-08-01T10:00:00Z"),
            event("added-2", "buyer-1", "2026-08-01T10:00:10Z"),
            event("added-3", "buyer-2", "2026-08-01T10:00:20Z")));

        assertEquals(List.of(), alerts);
    }

    @Test
    void deduplicatesRepeatedInputEventIdsBeforeCountingBuyers() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "buyer-1", "2026-08-01T10:00:00Z"),
            event("added-1", "buyer-1", "2026-08-01T10:00:00Z"),
            event("added-2", "buyer-2", "2026-08-01T10:00:20Z")));

        assertEquals(List.of(), alerts);
    }

    @Test
    void doesNotCombineABuyerAfterTheOneMinuteBoundary() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "buyer-1", "2026-08-01T10:00:00Z"),
            event("added-2", "buyer-2", "2026-08-01T10:00:30Z"),
            event("added-3", "buyer-3", "2026-08-01T10:01:01Z")));

        assertEquals(List.of(), alerts);
    }

    @Test
    void doesNotMatchWhenTheThirdBuyerArrivesAtExactlyTheSixtySecondBoundary() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "buyer-1", "2026-08-01T10:00:00Z"),
            event("added-2", "buyer-2", "2026-08-01T10:00:30Z"),
            event("added-3", "buyer-3", "2026-08-01T10:01:00Z")));

        assertEquals(List.of(), alerts);
    }

    @Test
    void emitsOnlyOneAlertForOverlappingMatchesWithTheSameProductWindow() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "buyer-1", "2026-08-01T10:00:00Z"),
            event("added-2", "buyer-2", "2026-08-01T10:00:10Z"),
            event("added-3", "buyer-3", "2026-08-01T10:00:20Z"),
            event("added-4", "buyer-4", "2026-08-01T10:00:30Z")));

        assertEquals(1, alerts.stream()
            .filter(alert -> alert.getAlertId().equals("trending_product:product-1:2026-08-01T10:00:00Z"))
            .count());
    }

    private List<CepAlert> run(List<EventEnvelope> events) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<CepAlert> alerts = new ArrayList<>();
        try (CloseableIterator<CepAlert> iterator = TrendingProductPattern.build(env.fromCollection(events))
            .executeAndCollect()) {
            while (iterator.hasNext()) {
                alerts.add(iterator.next());
            }
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
        return alerts;
    }

    private EventEnvelope event(String eventId, String actorId, String timestamp) throws Exception {
        return new EventEnvelope(
            eventId,
            "cart.item.added",
            actorId,
            "buyer",
            timestamp,
            mapper.readTree("{\"product_id\":\"product-1\",\"product_name\":\"Demo Product\"}"));
    }
}
