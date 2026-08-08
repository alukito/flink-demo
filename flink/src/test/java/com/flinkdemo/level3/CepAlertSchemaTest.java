package com.flinkdemo.level3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkdemo.level3.model.CepAlert;
import com.flinkdemo.level3.serde.CepAlertSchema;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CepAlertSchemaTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void serializesNumericDetailValuesAsJsonNumbers() throws Exception {
        CepAlert alert = new CepAlert(
            "order_surge:1", "order_surge", "2026-08-01T10:00:00Z", Map.of("checkout_count", 3L));

        byte[] bytes = new CepAlertSchema("flink.cep.alerts").serialize(alert, null, null).value();
        var json = mapper.readTree(bytes);

        assertTrue(json.get("detail").get("checkout_count").isNumber());
        assertEquals(3L, json.get("detail").get("checkout_count").longValue());
    }
}
