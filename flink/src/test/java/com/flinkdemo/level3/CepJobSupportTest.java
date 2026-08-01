package com.flinkdemo.level3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import org.junit.jupiter.api.Test;

class CepJobSupportTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void parsesEnvelopeTimestampForEventTime() throws Exception {
        assertEquals(1785578400000L, CepJobSupport.eventTimestamp(event("2026-08-01T10:00:00Z")));
    }

    @Test
    void rejectsInvalidEventTimeTimestamps() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> CepJobSupport.eventTimestamp(event("not-a-timestamp")));
    }

    private EventEnvelope event(String timestamp) throws Exception {
        return new EventEnvelope("event", "cart.checkout", "buyer", "buyer", timestamp, mapper.readTree("{}"));
    }
}
