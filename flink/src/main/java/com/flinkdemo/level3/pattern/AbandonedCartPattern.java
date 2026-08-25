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

/** Detects cart episodes that do not complete checkout within two event-time minutes. */
public final class AbandonedCartPattern {
    private static final String ALERT_PATTERN = "abandoned_cart";
    private static final long ABANDONED_AFTER_MILLIS = Time.minutes(2).toMilliseconds();

    private AbandonedCartPattern() {}

    public static DataStream<CepAlert> build(DataStream<EventEnvelope> input) {
        DataStream<EventEnvelope> deduplicated = CepJobSupport.eventTime(input)
            .keyBy(EventEnvelope::getEventId)
            .process(new EventDeduplicator());
        DataStream<EventEnvelope> cartEvents = deduplicated
            .filter(event -> "cart.item.added".equals(event.getEventType()))
            .union(deduplicated.filter(event -> "cart.checkout".equals(event.getEventType())));
        KeyedStream<EventEnvelope, String> byCart = cartEvents
            .filter(AbandonedCartPattern::isCartEventWithId)
            .keyBy(AbandonedCartPattern::cartId);

        Pattern<EventEnvelope, EventEnvelope> pattern = Pattern.<EventEnvelope>begin("added")
            .where(new SimpleCondition<EventEnvelope>() {
                @Override
                public boolean filter(EventEnvelope event) {
                    return "cart.item.added".equals(event.getEventType());
                }
            })
            .notFollowedBy("checkout")
            .where(new SimpleCondition<EventEnvelope>() {
                @Override
                public boolean filter(EventEnvelope event) {
                    return "cart.checkout".equals(event.getEventType());
                }
            })
            .within(Time.milliseconds(ABANDONED_AFTER_MILLIS + 1));

        PatternStream<EventEnvelope> matches = CEP.pattern(byCart, pattern);
        OutputTag<CepAlert> timeoutTag = new OutputTag<CepAlert>("abandoned-cart-timeouts") {};
        SingleOutputStreamOperator<CepAlert> completed = matches.select(
            timeoutTag,
            new PatternTimeoutFunction<EventEnvelope, CepAlert>() {
                @Override
                public CepAlert timeout(Map<String, List<EventEnvelope>> match, long timeoutTimestamp) {
                    EventEnvelope added = addedEvent(match);
                    return alert(match, CepJobSupport.eventTimestamp(added) + ABANDONED_AFTER_MILLIS);
                }
            },
            new PatternSelectFunction<EventEnvelope, CepAlert>() {
                @Override
                public CepAlert select(Map<String, List<EventEnvelope>> match) {
                    EventEnvelope added = addedEvent(match);
                    return alert(match, CepJobSupport.eventTimestamp(added) + ABANDONED_AFTER_MILLIS);
                }
            });

        return completed.union(completed.getSideOutput(timeoutTag))
            .keyBy(CepAlert::getAlertId)
            .process(new AlertDeduplicator("abandoned-cart-alert-emitted"));
    }

    private static boolean isCartEventWithId(EventEnvelope event) {
        return ("cart.item.added".equals(event.getEventType()) || "cart.checkout".equals(event.getEventType()))
            && !cartId(event).isBlank();
    }

    private static String cartId(EventEnvelope event) {
        return event.getPayload() == null ? "" : event.getPayload().path("cart_id").asText("");
    }

    private static CepAlert alert(Map<String, List<EventEnvelope>> match, long detectedAtMillis) {
        EventEnvelope added = addedEvent(match);
        String cartId = cartId(added);
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("cart_id", cartId);
        putIfPresent(detail, "buyer_id", added.getActorId());
        putIfPresent(detail, "buyer_name", added.getActorName());
        putPayloadText(detail, added, "seller_id");
        putPayloadText(detail, added, "seller_name");
        return new CepAlert(
            ALERT_PATTERN + ":" + cartId,
            ALERT_PATTERN,
            Instant.ofEpochMilli(detectedAtMillis).toString(),
            detail);
    }

    private static void putPayloadText(Map<String, Object> detail, EventEnvelope event, String field) {
        if (event.getPayload() != null) {
            putIfPresent(detail, field, event.getPayload().path(field).asText(""));
        }
    }

    private static void putIfPresent(Map<String, Object> detail, String field, String value) {
        if (value != null && !value.isBlank()) detail.put(field, value);
    }

    private static EventEnvelope addedEvent(Map<String, List<EventEnvelope>> match) {
        return match.get("added").get(0);
    }
}
