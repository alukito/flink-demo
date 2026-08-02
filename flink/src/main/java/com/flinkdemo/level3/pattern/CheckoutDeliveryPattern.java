package com.flinkdemo.level3.pattern;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level3.CepJobSupport;
import com.flinkdemo.level3.EventDeduplicator;
import com.flinkdemo.level3.model.CepAlert;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Measures the event-time elapsed duration from checkout to delivery for an order. */
public final class CheckoutDeliveryPattern {
    private static final String ALERT_PATTERN = "delivery_completed";

    private CheckoutDeliveryPattern() {}

    public static DataStream<CepAlert> build(DataStream<EventEnvelope> input) {
        DataStream<EventEnvelope> deduplicated = CepJobSupport.eventTime(input)
            .keyBy(EventEnvelope::getEventId)
            .process(new EventDeduplicator());
        DataStream<EventEnvelope> orderEvents = deduplicated
            .filter(CheckoutDeliveryPattern::isCheckout)
            .union(deduplicated.filter(CheckoutDeliveryPattern::isDelivery));
        KeyedStream<EventEnvelope, String> byOrder = orderEvents
            .filter(CheckoutDeliveryPattern::hasOrderId)
            .keyBy(CheckoutDeliveryPattern::orderId);

        Pattern<EventEnvelope, EventEnvelope> pattern = Pattern.<EventEnvelope>begin("checkout")
            .where(new SimpleCondition<EventEnvelope>() {
                @Override
                public boolean filter(EventEnvelope event) {
                    return isCheckout(event);
                }
            })
            .followedBy("delivered")
            .where(new DeliveryAfterCheckoutCondition());

        PatternStream<EventEnvelope> matches = CEP.pattern(byOrder, pattern);
        return matches
            .select(new PatternSelectFunction<EventEnvelope, CepAlert>() {
                @Override
                public CepAlert select(Map<String, List<EventEnvelope>> match) {
                    return alert(match);
                }
            })
            .keyBy(CepAlert::getAlertId)
            .process(new FirstAlertOnly());
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
        return new CepAlert(
            "checkout_delivery:" + orderId + ":" + checkoutAt,
            ALERT_PATTERN,
            deliveredAt.toString(),
            Map.of(
                "order_id", orderId,
                "checkout_at", checkoutAt.toString(),
                "delivered_at", deliveredAt.toString(),
                "elapsed_seconds", elapsedSeconds));
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

    private static final class FirstAlertOnly extends KeyedProcessFunction<String, CepAlert, CepAlert> {
        private transient ValueState<Boolean> emitted;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
                "checkout-delivery-alert-emitted", Types.BOOLEAN);
            descriptor.enableTimeToLive(
                StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(8))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build());
            emitted = getRuntimeContext().getState(descriptor);
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
