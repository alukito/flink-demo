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

/** Detects orders that remain undelivered for one event-time minute after pickup. */
public final class SlowDeliveryPattern {
    private static final String ALERT_PATTERN = "slow_delivery";

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
            .within(Time.seconds(60));

        PatternStream<EventEnvelope> matches = CEP.pattern(byOrder, pattern);
        OutputTag<CepAlert> timeoutTag = new OutputTag<CepAlert>("slow-delivery-timeouts") {};
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
                    EventEnvelope picked = pickedEvent(match);
                    return alert(match, CepJobSupport.eventTimestamp(picked) + Time.seconds(60).toMilliseconds());
                }
            });

        return completed.getSideOutput(timeoutTag)
            .keyBy(CepAlert::getAlertId)
            .process(new FirstAlertOnly());
    }

    private static boolean isShipmentEventWithOrderId(EventEnvelope event) {
        return ("shipment.picked".equals(event.getEventType()) || "shipment.delivered".equals(event.getEventType()))
            && !orderId(event).isBlank();
    }

    private static String orderId(EventEnvelope event) {
        return event.getPayload() == null ? "" : event.getPayload().path("order_id").asText("");
    }

    private static CepAlert alert(Map<String, List<EventEnvelope>> match, long detectedAtMillis) {
        String orderId = orderId(pickedEvent(match));
        return new CepAlert(
            ALERT_PATTERN + ":" + orderId,
            ALERT_PATTERN,
            Instant.ofEpochMilli(detectedAtMillis).toString(),
            Map.of("order_id", orderId));
    }

    private static EventEnvelope pickedEvent(Map<String, List<EventEnvelope>> match) {
        return match.get("picked").get(0);
    }

    private static final class FirstAlertOnly extends KeyedProcessFunction<String, CepAlert, CepAlert> {
        private transient ValueState<Boolean> emitted;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            emitted = getRuntimeContext().getState(new ValueStateDescriptor<>("slow-delivery-alert-emitted", Types.BOOLEAN));
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
