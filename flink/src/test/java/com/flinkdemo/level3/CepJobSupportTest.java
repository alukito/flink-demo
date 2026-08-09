package com.flinkdemo.level3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class CepJobSupportTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void parsesEnvelopeTimestampForEventTime() throws Exception {
        assertEquals(1785578400000L, CepJobSupport.eventTimestamp(event("2026-08-01T10:00:00Z")));
    }

    @Test
    void dropsInvalidEventTimeTimestampsWhilePreservingValidEvents() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<EventEnvelope> collected = CepJobSupport.eventTime(
            env.fromCollection(List.of(event("not-a-timestamp"), event("2026-08-01T10:00:00Z"))))
            .executeAndCollect(1);

        assertEquals("2026-08-01T10:00:00Z", collected.get(0).getTimestamp());
    }

    private EventEnvelope event(String timestamp) throws Exception {
        return new EventEnvelope("event", "cart.checkout", "buyer", "Buyer", "buyer", timestamp, mapper.readTree("{}"));
    }
}
