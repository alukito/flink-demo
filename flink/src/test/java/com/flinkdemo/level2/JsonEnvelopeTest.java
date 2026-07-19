package com.flinkdemo.level2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import com.flinkdemo.level2.serde.EventEnvelopeSchema;
import com.flinkdemo.level2.serde.WindowStatSchema;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonEnvelopeTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void deserializesInputEnvelopeWithoutChangingRupiahIntegers() throws Exception {
        String json = "{\"event_id\":\"e1\",\"event_type\":\"cart.checkout\",\"actor_id\":\"b1\",\"actor_role\":\"buyer\",\"timestamp\":\"2026-07-18T10:00:00Z\",\"payload\":{\"total_amount\":489000}}";
        EventEnvelope event = new EventEnvelopeSchema().deserialize(json.getBytes(StandardCharsets.UTF_8));
        assertEquals("cart.checkout", event.getEventType());
        assertEquals(489000L, event.getPayload().get("total_amount").longValue());
    }

    @Test
    void serializesGeneralizedMetricEnvelope() throws Exception {
        WindowStat stat = new WindowStat("top_product", "window", "2026-07-18T10:05:00Z", 4L, Map.of("product_id", "p1", "name", "Widget"));
        byte[] bytes = new WindowStatSchema("flink.window.stats").serialize(stat, null, null).value();
        var json = mapper.readTree(bytes);
        assertEquals("top_product", json.get("metric").textValue());
        assertEquals(4L, json.get("value").longValue());
        assertEquals("p1", json.get("detail").get("product_id").textValue());
    }
}
