package com.flinkdemo.level3.pattern;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level3.CepJobSupport;
import com.flinkdemo.level3.EventDeduplicator;
import com.flinkdemo.level3.model.CepAlert;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Detects cart episodes that do not complete checkout within two event-time minutes. */
public final class AbandonedCartPattern {
    private static final String ALERT_PATTERN = "abandoned_cart";

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
            .within(Time.minutes(2));

        PatternStream<EventEnvelope> matches = CEP.pattern(byCart, pattern);
        OutputTag<CepAlert> timeoutTag = new OutputTag<CepAlert>("abandoned-cart-timeouts") {};
        SingleOutputStreamOperator<CepAlert> completed = matches.select(
            timeoutTag,
            new PatternTimeoutFunction<EventEnvelope, CepAlert>() {
                @Override
                public CepAlert timeout(Map<String, List<EventEnvelope>> match, long timeoutTimestamp) {
                    return alert(match, timeoutTimestamp);
                }
            },
            new PatternSelectFunction<EventEnvelope, CepAlert>() {
                @Override
                public CepAlert select(Map<String, List<EventEnvelope>> match) {
                    EventEnvelope added = addedEvent(match);
                    return alert(match, CepJobSupport.eventTimestamp(added) + Time.minutes(2).toMilliseconds());
                }
            });

        return completed.getSideOutput(timeoutTag)
            .keyBy(CepAlert::getAlertId)
            .process(new FirstAlertOnly());
    }

    private static boolean isCartEventWithId(EventEnvelope event) {
        return ("cart.item.added".equals(event.getEventType()) || "cart.checkout".equals(event.getEventType()))
            && !cartId(event).isBlank();
    }

    private static String cartId(EventEnvelope event) {
        return event.getPayload() == null ? "" : event.getPayload().path("cart_id").asText("");
    }

    private static CepAlert alert(Map<String, List<EventEnvelope>> match, long detectedAtMillis) {
        String cartId = cartId(addedEvent(match));
        return new CepAlert(
            ALERT_PATTERN + ":" + cartId,
            ALERT_PATTERN,
            Instant.ofEpochMilli(detectedAtMillis).toString(),
            Map.of("cart_id", cartId));
    }

    private static EventEnvelope addedEvent(Map<String, List<EventEnvelope>> match) {
        return match.get("added").get(0);
    }

    private static final class FirstAlertOnly extends KeyedProcessFunction<String, CepAlert, CepAlert> {
        private transient ValueState<Boolean> emitted;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            emitted = getRuntimeContext().getState(new ValueStateDescriptor<>("abandoned-cart-alert-emitted", Types.BOOLEAN));
        }

        @Override
        public void processElement(CepAlert alert, Context context, Collector<CepAlert> out) throws Exception {
            if (!Boolean.TRUE.equals(emitted.value())) {
                emitted.update(true);
                out.collect(alert);
            }
        }
    }
}
