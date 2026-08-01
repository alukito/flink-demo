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

class AbandonedCartPatternTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void emitsOneAlertWhenAnAddedCartHasNoCheckoutForTwoMinutes() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "cart.item.added", "cart-1", "2026-08-01T10:00:00Z")));

        assertEquals(1, alerts.size());
        CepAlert alert = alerts.get(0);
        assertEquals("abandoned_cart:cart-1", alert.getAlertId());
        assertEquals("abandoned_cart", alert.getPattern());
        assertEquals("cart-1", alert.getDetail().get("cart_id"));
        assertEquals("2026-08-01T10:02:00Z", alert.getDetectedAt());
    }

    @Test
    void doesNotEmitWhenCheckoutArrivesWithinTwoMinutes() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "cart.item.added", "cart-1", "2026-08-01T10:00:00Z"),
            event("checkout-1", "cart.checkout", "cart-1", "2026-08-01T10:01:59Z")));

        assertEquals(List.of(), alerts);
    }

    @Test
    void doesNotEmitWhenCheckoutArrivesAtTheTwoMinuteBoundary() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "cart.item.added", "cart-1", "2026-08-01T10:00:00Z"),
            event("checkout-1", "cart.checkout", "cart-1", "2026-08-01T10:02:00Z")));

        assertEquals(List.of(), alerts);
    }

    @Test
    void ignoresRepeatedInputEventIdsBeforeMatching() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("added-1", "cart.item.added", "cart-1", "2026-08-01T10:00:00Z"),
            event("added-1", "cart.item.added", "cart-1", "2026-08-01T10:00:00Z")));

        assertEquals(List.of("abandoned_cart:cart-1"), alerts.stream()
            .map(CepAlert::getAlertId)
            .collect(Collectors.toList()));
    }

    private List<CepAlert> run(List<EventEnvelope> events) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<CepAlert> alerts = new ArrayList<>();
        try (CloseableIterator<CepAlert> iterator = AbandonedCartPattern.build(env.fromCollection(events))
            .executeAndCollect()) {
            while (iterator.hasNext()) {
                alerts.add(iterator.next());
            }
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
        return alerts;
    }

    private EventEnvelope event(String eventId, String eventType, String cartId, String timestamp) throws Exception {
        return new EventEnvelope(
            eventId,
            eventType,
            "buyer-1",
            "buyer",
            timestamp,
            mapper.readTree("{\"cart_id\":\"" + cartId + "\"}"));
    }
}
