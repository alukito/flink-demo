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

class DeliveryCompletedPatternTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void emitsElapsedSecondsForCheckoutFollowedByDelivery() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("checkout-1", "cart.checkout", "order-1", "2026-08-01T10:00:00Z"),
            event("delivery-1", "shipment.delivered", "order-1", "2026-08-01T10:00:47Z")));

        assertEquals(1, alerts.size());
        CepAlert alert = alerts.get(0);
        assertEquals("delivery_completed:order-1", alert.getAlertId());
        assertEquals("delivery_completed", alert.getPattern());
        assertEquals("2026-08-01T10:00:47Z", alert.getDetectedAt());
        assertEquals("order-1", alert.getDetail().get("order_id"));
        assertEquals("2026-08-01T10:00:00Z", alert.getDetail().get("checkout_at"));
        assertEquals("2026-08-01T10:00:47Z", alert.getDetail().get("delivered_at"));
        assertEquals(47L, alert.getDetail().get("elapsed_seconds"));
    }

    @Test
    void emitsNumericElapsedSecondsForDeliveryWithinTheOperationalRetentionHorizon() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("checkout-1", "cart.checkout", "order-1", "2026-08-01T10:00:00Z"),
            event("delivery-1", "shipment.delivered", "order-1", "2026-08-01T17:59:59Z")));

        assertEquals(1, alerts.size());
        assertEquals(28_799L, alerts.get(0).getDetail().get("elapsed_seconds"));
    }

    @Test
    void discardsCheckoutAtTheOperationalRetentionHorizonWithoutEmittingATimeoutAlert() throws Exception {
        assertEquals(List.of(), run(List.of(
            event("checkout-1", "cart.checkout", "order-1", "2026-08-01T10:00:00Z"),
            event("delivery-1", "shipment.delivered", "order-1", "2026-08-01T18:00:00Z"))));
    }

    @Test
    void ignoresDeliveryWithoutCheckout() throws Exception {
        assertEquals(List.of(), run(List.of(
            event("delivery-1", "shipment.delivered", "order-1", "2026-08-01T10:00:47Z"))));
    }

    @Test
    void isolatesDifferentOrderIds() throws Exception {
        assertEquals(List.of(), run(List.of(
            event("checkout-1", "cart.checkout", "order-1", "2026-08-01T10:00:00Z"),
            event("delivery-2", "shipment.delivered", "order-2", "2026-08-01T10:00:47Z"))));
    }

    @Test
    void ignoresDeliveryWithEarlierEventTimeThanCheckout() throws Exception {
        assertEquals(List.of(), run(List.of(
            event("checkout-1", "cart.checkout", "order-1", "2026-08-01T10:01:00Z"),
            event("delivery-1", "shipment.delivered", "order-1", "2026-08-01T10:00:59Z"))));
    }

    @Test
    void suppressesDuplicateInputEventIdsBeforeCep() throws Exception {
        assertEquals(List.of(), run(List.of(
            event("checkout-1", "cart.checkout", "order-1", "2026-08-01T10:00:00Z"),
            event("checkout-1", "shipment.delivered", "order-1", "2026-08-01T10:00:47Z"))));
    }

    private List<CepAlert> run(List<EventEnvelope> events) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<CepAlert> alerts = new ArrayList<>();
        try (CloseableIterator<CepAlert> iterator = DeliveryCompletedPattern.build(env.fromCollection(events))
            .executeAndCollect()) {
            while (iterator.hasNext()) {
                alerts.add(iterator.next());
            }
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
        return alerts;
    }

    private EventEnvelope event(String eventId, String eventType, String orderId, String timestamp) throws Exception {
        return new EventEnvelope(
            eventId,
            eventType,
            "seller-1",
            "seller",
            timestamp,
            mapper.readTree("{\"order_id\":\"" + orderId + "\"}"));
    }
}
