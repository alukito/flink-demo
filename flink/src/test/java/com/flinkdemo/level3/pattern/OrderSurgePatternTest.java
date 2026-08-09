package com.flinkdemo.level3.pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level3.model.CepAlert;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class OrderSurgePatternTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void emitsSurgeAlertForThreeDistinctBuyersWithinThirtySeconds() throws Exception {
        List<CepAlert> alerts = run(List.of(
            checkout("checkout-1", "buyer-1", "2026-08-01T10:00:00Z"),
            checkout("checkout-2", "buyer-2", "2026-08-01T10:00:10Z"),
            checkout("checkout-3", "buyer-3", "2026-08-01T10:00:29Z")));

        assertEquals(1, alerts.size());
        CepAlert alert = alerts.get(0);
        assertEquals("order_surge:2026-08-01T10:00:00Z", alert.getAlertId());
        assertEquals("order_surge", alert.getPattern());
        assertEquals("2026-08-01T10:00:29Z", alert.getDetectedAt());
        assertEquals(true, alert.getDetail().get("surge"));
        assertEquals(3, alert.getDetail().get("checkout_count"));
        assertEquals(false, alert.getDetail().containsKey("buyer"));
        assertEquals(false, alert.getDetail().containsKey("buyers"));
    }

    @Test
    void doesNotEmitForFewerThanThreeDistinctBuyers() throws Exception {
        assertEquals(List.of(), run(List.of(
            checkout("checkout-1", "buyer-1", "2026-08-01T10:00:00Z"),
            checkout("checkout-2", "buyer-2", "2026-08-01T10:00:20Z"))));
    }

    @Test
    void doesNotCountRepeatedBuyerTowardSurgeThreshold() throws Exception {
        assertEquals(List.of(), run(List.of(
            checkout("checkout-1", "buyer-1", "2026-08-01T10:00:00Z"),
            checkout("checkout-2", "buyer-1", "2026-08-01T10:00:10Z"),
            checkout("checkout-3", "buyer-2", "2026-08-01T10:00:20Z"))));
    }

    @Test
    void doesNotCombineBuyersOutsideThirtySecondBoundary() throws Exception {
        assertEquals(List.of(), run(List.of(
            checkout("checkout-1", "buyer-1", "2026-08-01T10:00:00Z"),
            checkout("checkout-2", "buyer-2", "2026-08-01T10:00:10Z"),
            checkout("checkout-3", "buyer-3", "2026-08-01T10:00:31Z"))));
    }

    @Test
    void emitsANewSurgeForThreeBuyersInALaterThirtySecondWindow() throws Exception {
        List<CepAlert> alerts = run(List.of(
            checkout("checkout-1", "buyer-1", "2026-08-01T10:00:00Z"),
            checkout("checkout-2", "buyer-2", "2026-08-01T10:00:31Z"),
            checkout("checkout-3", "buyer-3", "2026-08-01T10:00:32Z"),
            checkout("checkout-4", "buyer-4", "2026-08-01T10:00:33Z")));

        assertEquals(1, alerts.size());
        assertEquals("order_surge:2026-08-01T10:00:31Z", alerts.get(0).getAlertId());
    }

    @Test
    void doesNotMatchWhenThirdBuyerArrivesAtExactlyThirtySecondBoundary() throws Exception {
        assertEquals(List.of(), run(List.of(
            checkout("checkout-1", "buyer-1", "2026-08-01T10:00:00Z"),
            checkout("checkout-2", "buyer-2", "2026-08-01T10:00:10Z"),
            checkout("checkout-3", "buyer-3", "2026-08-01T10:00:30Z"))));
    }

    @Test
    void suppressesDuplicateInputEventIdsBeforeCountingBuyers() throws Exception {
        assertEquals(List.of(), run(List.of(
            checkout("checkout-1", "buyer-1", "2026-08-01T10:00:00Z"),
            checkout("checkout-1", "buyer-2", "2026-08-01T10:00:10Z"),
            checkout("checkout-2", "buyer-3", "2026-08-01T10:00:20Z"))));
    }

    private List<CepAlert> run(List<EventEnvelope> events) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<CepAlert> alerts = new ArrayList<>();
        try (CloseableIterator<CepAlert> iterator = OrderSurgePattern.build(env.fromCollection(events))
            .executeAndCollect()) {
            while (iterator.hasNext()) {
                alerts.add(iterator.next());
            }
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
        return alerts;
    }

    private EventEnvelope checkout(String eventId, String buyerId, String timestamp) throws Exception {
        return new EventEnvelope(
            eventId,
            "cart.checkout",
            buyerId,
            "Buyer",
            "buyer",
            timestamp,
            mapper.readTree("{}"));
    }
}
