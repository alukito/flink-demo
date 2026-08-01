package com.flinkdemo.level3.pattern;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level3.CepJobSupport;
import com.flinkdemo.level3.EventDeduplicator;
import com.flinkdemo.level3.model.CepAlert;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/** Detects a product being added by three distinct buyers in one event-time minute. */
public final class TrendingProductPattern {
    private static final String ALERT_PATTERN = "trending_product";

    private TrendingProductPattern() {}

    public static DataStream<CepAlert> build(DataStream<EventEnvelope> input) {
        DataStream<EventEnvelope> deduplicated = CepJobSupport.eventTime(input)
            .keyBy(EventEnvelope::getEventId)
            .process(new EventDeduplicator());
        KeyedStream<EventEnvelope, String> byProduct = deduplicated
            .filter(TrendingProductPattern::isProductAdded)
            .keyBy(TrendingProductPattern::productId);

        Pattern<EventEnvelope, EventEnvelope> pattern = Pattern.<EventEnvelope>begin("first")
            .where(new SimpleCondition<EventEnvelope>() {
                @Override
                public boolean filter(EventEnvelope event) {
                    return isProductAdded(event);
                }
            })
            .followedByAny("second")
            .where(new DistinctActorCondition("first"))
            .followedByAny("third")
            .where(new DistinctActorCondition("first", "second"))
            .within(Time.seconds(60));

        PatternStream<EventEnvelope> matches = CEP.pattern(byProduct, pattern);
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

    private static boolean isProductAdded(EventEnvelope event) {
        return event != null
            && "cart.item.added".equals(event.getEventType())
            && !productId(event).isBlank()
            && !actorId(event).isBlank();
    }

    private static CepAlert alert(Map<String, List<EventEnvelope>> match) {
        EventEnvelope first = event(match, "first");
        EventEnvelope third = event(match, "third");
        String productId = productId(first);
        String windowStart = Instant.ofEpochMilli(CepJobSupport.eventTimestamp(first)).toString();
        return new CepAlert(
            ALERT_PATTERN + ":" + productId + ":" + windowStart,
            ALERT_PATTERN,
            Instant.ofEpochMilli(CepJobSupport.eventTimestamp(third)).toString(),
            Map.of(
                "product_id", productId,
                "product_name", productName(first),
                "qualifying_count", 3));
    }

    private static EventEnvelope event(Map<String, List<EventEnvelope>> match, String name) {
        return match.get(name).get(0);
    }

    private static String productId(EventEnvelope event) {
        return event.getPayload() == null ? "" : event.getPayload().path("product_id").asText("");
    }

    private static String productName(EventEnvelope event) {
        return event.getPayload() == null ? "" : event.getPayload().path("product_name").asText("");
    }

    private static String actorId(EventEnvelope event) {
        return event.getActorId() == null ? "" : event.getActorId();
    }

    private static final class DistinctActorCondition extends IterativeCondition<EventEnvelope> {
        private final String[] previousNames;

        private DistinctActorCondition(String... previousNames) {
            this.previousNames = previousNames;
        }

        @Override
        public boolean filter(EventEnvelope event, Context<EventEnvelope> context) throws Exception {
            if (!isProductAdded(event)) {
                return false;
            }
            Set<String> previousActors = new HashSet<>();
            for (String previousName : previousNames) {
                for (EventEnvelope previous : context.getEventsForPattern(previousName)) {
                    previousActors.add(actorId(previous));
                }
            }
            return !previousActors.contains(actorId(event));
        }
    }

    private static final class FirstAlertOnly extends KeyedProcessFunction<String, CepAlert, CepAlert> {
        private transient ValueState<Boolean> emitted;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            emitted = getRuntimeContext().getState(new ValueStateDescriptor<>("trending-product-alert-emitted", Types.BOOLEAN));
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
