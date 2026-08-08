package com.flinkdemo.level3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.flinkdemo.level3.model.CepAlert;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CepAlertTest {
    @Test
    void copiesDetailDefensively() {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("checkout_count", 3L);
        CepAlert alert = new CepAlert("order_surge:1", "order_surge", "2026-08-01T10:00:00Z", detail);

        detail.put("checkout_count", 99L);

        assertEquals(3L, alert.getDetail().get("checkout_count"));
        assertThrows(UnsupportedOperationException.class, () -> alert.getDetail().put("other", "value"));
    }
}
