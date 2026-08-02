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

class SlowDeliveryPatternTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void emitsAlertWhenPickedOrderIsNotDeliveredWithinOneMinute() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("picked-1", "shipment.picked", "order-1", "2026-08-01T10:00:00Z")));

        assertEquals(1, alerts.size());
        CepAlert alert = alerts.get(0);
        assertEquals("slow_delivery:order-1", alert.getAlertId());
        assertEquals("slow_delivery", alert.getPattern());
        assertEquals("order-1", alert.getDetail().get("order_id"));
        assertEquals("2026-08-01T10:01:00Z", alert.getDetectedAt());
    }

    @Test
    void doesNotEmitWhenDeliveryArrivesWithinOneMinute() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("picked-1", "shipment.picked", "order-1", "2026-08-01T10:00:00Z"),
            event("delivered-1", "shipment.delivered", "order-1", "2026-08-01T10:00:59Z")));

        assertEquals(List.of(), alerts);
    }

    @Test
    void doesNotEmitWhenDeliveryArrivesAtTheOneMinuteBoundary() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("picked-1", "shipment.picked", "order-1", "2026-08-01T10:00:00Z"),
            event("delivered-1", "shipment.delivered", "order-1", "2026-08-01T10:01:00Z")));

        assertEquals(List.of(), alerts);
    }

    @Test
    void deliveryForAnotherOrderDoesNotCancelThePickedOrderTimeout() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("picked-1", "shipment.picked", "order-1", "2026-08-01T10:00:00Z"),
            event("delivered-2", "shipment.delivered", "order-2", "2026-08-01T10:00:30Z")));

        assertEquals(List.of("slow_delivery:order-1"), alerts.stream()
            .map(CepAlert::getAlertId)
            .collect(Collectors.toList()));
    }

    @Test
    void ignoresDuplicateEventIdBeforeDeliveryCanCancelPickupTimeout() throws Exception {
        List<CepAlert> alerts = run(List.of(
            event("picked-1", "shipment.picked", "order-1", "2026-08-01T10:00:00Z"),
            event("picked-1", "shipment.delivered", "order-1", "2026-08-01T10:00:30Z")));

        assertEquals(List.of("slow_delivery:order-1"), alerts.stream()
            .map(CepAlert::getAlertId)
            .collect(Collectors.toList()));
    }

    private List<CepAlert> run(List<EventEnvelope> events) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<CepAlert> alerts = new ArrayList<>();
        try (CloseableIterator<CepAlert> iterator = SlowDeliveryPattern.build(env.fromCollection(events))
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
