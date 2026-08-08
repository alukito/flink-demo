package com.flinkdemo.level3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class CepPatternTest {
    @Test
    void exposesExactlyTheFiveApprovedPatternNames() {
        assertEquals(
            List.of("abandoned_cart", "trending_product", "slow_delivery", "order_surge", "delivery_completed"),
            java.util.Arrays.stream(CepPattern.values()).map(CepPattern::pattern).collect(Collectors.toList()));
    }

    @Test
    void resolvesNamesAndRejectsUnknownPatterns() {
        assertEquals(CepPattern.ORDER_SURGE, CepPattern.fromName("order_surge"));
        assertThrows(IllegalArgumentException.class, () -> CepPattern.fromName("unknown"));
    }

    @Test
    void mapsEachPatternToItsKafkaTopicsAndConsumerGroup() {
        assertEquals(List.of("cart.item.added", "cart.checkout"), CepPattern.ABANDONED_CART.sourceTopics());
        assertEquals(List.of("cart.item.added"), CepPattern.TRENDING_PRODUCT.sourceTopics());
        assertEquals(List.of("shipment.picked", "shipment.delivered"), CepPattern.SLOW_DELIVERY.sourceTopics());
        assertEquals(List.of("cart.checkout"), CepPattern.ORDER_SURGE.sourceTopics());
        assertEquals(List.of("cart.checkout", "shipment.delivered"), CepPattern.DELIVERY_COMPLETED.sourceTopics());
        assertEquals("flink-level3-order_surge", CepPattern.ORDER_SURGE.consumerGroup());
    }

    @Test
    void givesEveryPatternItsOwnLevel3JobName() {
        assertEquals("level3-abandoned_cart", CepPattern.ABANDONED_CART.jobName());
        assertEquals("level3-trending_product", CepPattern.TRENDING_PRODUCT.jobName());
        assertEquals("level3-slow_delivery", CepPattern.SLOW_DELIVERY.jobName());
        assertEquals("level3-order_surge", CepPattern.ORDER_SURGE.jobName());
        assertEquals("level3-delivery_completed", CepPattern.DELIVERY_COMPLETED.jobName());
    }
}
