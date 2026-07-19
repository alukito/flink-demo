package com.flinkdemo.level2.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public final class EventEnvelopeSchema extends AbstractDeserializationSchema<EventEnvelope> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Override public EventEnvelope deserialize(byte[] message) throws IOException { return MAPPER.readValue(message, EventEnvelope.class); }
}
