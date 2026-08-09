package com.flinkdemo.level3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class EventDeduplicatorTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void forwardsOnlyTheFirstOccurrenceOfAnEventId() throws Exception {
        EventEnvelope first = event("event-1");
        EventEnvelope duplicate = event("event-1");
        EventEnvelope second = event("event-2");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> eventIds = env.fromCollection(List.of(first, duplicate, second))
            .keyBy(EventEnvelope::getEventId)
            .process(new EventDeduplicator())
            .executeAndCollect(2)
            .stream()
            .map(EventEnvelope::getEventId)
            .collect(Collectors.toList());

        assertEquals(List.of("event-1", "event-2"), eventIds);
    }

    private EventEnvelope event(String eventId) throws Exception {
        return new EventEnvelope(
            eventId, "cart.checkout", "buyer-1", "Buyer", "buyer", "2026-08-01T10:00:00Z", mapper.readTree("{}"));
    }
}
