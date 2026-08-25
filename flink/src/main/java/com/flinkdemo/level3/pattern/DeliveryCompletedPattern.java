package com.flinkdemo.level3.pattern;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level3.AlertDeduplicator;
import com.flinkdemo.level3.CepJobSupport;
import com.flinkdemo.level3.EventDeduplicator;
import com.flinkdemo.level3.model.CepAlert;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Measures event-time checkout-to-delivery duration for an order.
 *
 * <p>Unmatched checkouts are retained for eight hours to match the approved demo/cache horizon.
 * That operational cleanup horizon is not a slow-delivery threshold and emits no timeout alert.
 */
public final class DeliveryCompletedPattern {
    private static final String ALERT_PATTERN = "delivery_completed";

    private DeliveryCompletedPattern() {}

    public static DataStream<CepAlert> build(DataStream<EventEnvelope> input) {
        DataStream<EventEnvelope> deduplicated = CepJobSupport.eventTime(input)
            .keyBy(EventEnvelope::getEventId)
            .process(new EventDeduplicator());
        DataStream<EventEnvelope> orderEvents = deduplicated
            .filter(DeliveryCompletedPattern::isCheckout)
            .union(deduplicated.filter(DeliveryCompletedPattern::isDelivery));
        KeyedStream<EventEnvelope, String> byOrder = orderEvents
            .filter(DeliveryCompletedPattern::hasOrderId)
            .keyBy(DeliveryCompletedPattern::orderId);

        Pattern<EventEnvelope, EventEnvelope> pattern = Pattern.<EventEnvelope>begin("checkout")
            .where(new SimpleCondition<EventEnvelope>() {
                @Override
                public boolean filter(EventEnvelope event) {
                    return isCheckout(event);
                }
            })
            .followedBy("delivered")
            .where(new DeliveryAfterCheckoutCondition())
            .within(Time.hours(8));

        PatternStream<EventEnvelope> matches = CEP.pattern(byOrder, pattern);
        return matches
            .select(new PatternSelectFunction<EventEnvelope, CepAlert>() {
                @Override
                public CepAlert select(Map<String, List<EventEnvelope>> match) {
                    return alert(match);
                }
            })
            .keyBy(CepAlert::getAlertId)
            .process(new AlertDeduplicator("checkout-delivery-alert-emitted"));
    }

    private static boolean isCheckout(EventEnvelope event) {
        return event != null && "cart.checkout".equals(event.getEventType());
    }

    private static boolean isDelivery(EventEnvelope event) {
        return event != null && "shipment.delivered".equals(event.getEventType());
    }

    private static boolean hasOrderId(EventEnvelope event) {
        return !orderId(event).isBlank();
    }

    private static String orderId(EventEnvelope event) {
        return event == null || event.getPayload() == null ? "" : event.getPayload().path("order_id").asText("");
    }

    private static CepAlert alert(Map<String, List<EventEnvelope>> match) {
        EventEnvelope checkout = event(match, "checkout");
        EventEnvelope delivery = event(match, "delivered");
        Instant checkoutAt = Instant.ofEpochMilli(CepJobSupport.eventTimestamp(checkout));
        Instant deliveredAt = Instant.ofEpochMilli(CepJobSupport.eventTimestamp(delivery));
        long elapsedSeconds = Duration.between(checkoutAt, deliveredAt).getSeconds();
        String orderId = orderId(checkout);
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("order_id", orderId);
        detail.put("checkout_at", checkoutAt.toString());
        detail.put("delivered_at", deliveredAt.toString());
        detail.put("elapsed_seconds", elapsedSeconds);
        for (String field : List.of("shipper_id", "shipper_name")) {
            if (delivery.getPayload() != null) {
                String value = delivery.getPayload().path(field).asText("");
                if (!value.isBlank()) detail.put(field, value);
            }
        }
        return new CepAlert(
            ALERT_PATTERN + ":" + orderId,
            ALERT_PATTERN,
            deliveredAt.toString(),
            detail);
    }

    private static EventEnvelope event(Map<String, List<EventEnvelope>> match, String name) {
        return match.get(name).get(0);
    }

    private static final class DeliveryAfterCheckoutCondition extends IterativeCondition<EventEnvelope> {
        @Override
        public boolean filter(EventEnvelope delivery, Context<EventEnvelope> context) throws Exception {
            if (!isDelivery(delivery)) {
                return false;
            }
            for (EventEnvelope checkout : context.getEventsForPattern("checkout")) {
                return CepJobSupport.eventTimestamp(delivery) >= CepJobSupport.eventTimestamp(checkout);
            }
            return false;
        }
    }
}
