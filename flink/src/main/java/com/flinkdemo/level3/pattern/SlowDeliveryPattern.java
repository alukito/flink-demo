package com.flinkdemo.level3.pattern;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level3.AlertDeduplicator;
import com.flinkdemo.level3.CepJobSupport;
import com.flinkdemo.level3.EventDeduplicator;
import com.flinkdemo.level3.model.CepAlert;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/** Detects orders that remain undelivered for one event-time minute after pickup. */
public final class SlowDeliveryPattern {
    private static final String ALERT_PATTERN = "slow_delivery";
    private static final long SLOW_AFTER_MILLIS = Time.seconds(60).toMilliseconds();

    private SlowDeliveryPattern() {}

    public static DataStream<CepAlert> build(DataStream<EventEnvelope> input) {
        DataStream<EventEnvelope> deduplicated = CepJobSupport.eventTime(input)
            .keyBy(EventEnvelope::getEventId)
            .process(new EventDeduplicator());
        DataStream<EventEnvelope> shipmentEvents = deduplicated
            .filter(event -> "shipment.picked".equals(event.getEventType()))
            .union(deduplicated.filter(event -> "shipment.delivered".equals(event.getEventType())));
        KeyedStream<EventEnvelope, String> byOrder = shipmentEvents
            .filter(SlowDeliveryPattern::isShipmentEventWithOrderId)
            .keyBy(SlowDeliveryPattern::orderId);

        Pattern<EventEnvelope, EventEnvelope> pattern = Pattern.<EventEnvelope>begin("picked")
            .where(new SimpleCondition<EventEnvelope>() {
                @Override
                public boolean filter(EventEnvelope event) {
                    return "shipment.picked".equals(event.getEventType());
                }
            })
            .notFollowedBy("delivered")
            .where(new SimpleCondition<EventEnvelope>() {
                @Override
                public boolean filter(EventEnvelope event) {
                    return "shipment.delivered".equals(event.getEventType());
                }
            })
            .within(Time.milliseconds(SLOW_AFTER_MILLIS + 1));

        PatternStream<EventEnvelope> matches = CEP.pattern(byOrder, pattern);
        OutputTag<CepAlert> timeoutTag = new OutputTag<CepAlert>("slow-delivery-timeouts") {};
        SingleOutputStreamOperator<CepAlert> completed = matches.select(
            timeoutTag,
            new PatternTimeoutFunction<EventEnvelope, CepAlert>() {
                @Override
                public CepAlert timeout(Map<String, List<EventEnvelope>> match, long timeoutTimestamp) {
                    EventEnvelope picked = pickedEvent(match);
                    return alert(match, CepJobSupport.eventTimestamp(picked) + SLOW_AFTER_MILLIS);
                }
            },
            new PatternSelectFunction<EventEnvelope, CepAlert>() {
                @Override
                public CepAlert select(Map<String, List<EventEnvelope>> match) {
                    EventEnvelope picked = pickedEvent(match);
                    return alert(match, CepJobSupport.eventTimestamp(picked) + SLOW_AFTER_MILLIS);
                }
            });

        return completed.union(completed.getSideOutput(timeoutTag))
            .keyBy(CepAlert::getAlertId)
            .process(new AlertDeduplicator("slow-delivery-alert-emitted"));
    }

    private static boolean isShipmentEventWithOrderId(EventEnvelope event) {
        return ("shipment.picked".equals(event.getEventType()) || "shipment.delivered".equals(event.getEventType()))
            && !orderId(event).isBlank();
    }

    private static String orderId(EventEnvelope event) {
        return event.getPayload() == null ? "" : event.getPayload().path("order_id").asText("");
    }

    private static CepAlert alert(Map<String, List<EventEnvelope>> match, long detectedAtMillis) {
        EventEnvelope picked = pickedEvent(match);
        String orderId = orderId(picked);
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("order_id", orderId);
        for (String field : List.of("buyer_id", "buyer_name", "seller_id", "seller_name", "shipper_id", "shipper_name")) {
            if (picked.getPayload() != null) {
                String value = picked.getPayload().path(field).asText("");
                if (!value.isBlank()) detail.put(field, value);
            }
        }
        return new CepAlert(
            ALERT_PATTERN + ":" + orderId,
            ALERT_PATTERN,
            Instant.ofEpochMilli(detectedAtMillis).toString(),
            detail);
    }

    private static EventEnvelope pickedEvent(Map<String, List<EventEnvelope>> match) {
        return match.get("picked").get(0);
    }
}
