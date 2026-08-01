package com.flinkdemo.level3;

import java.util.List;

public enum CepPattern {
    ABANDONED_CART("abandoned_cart", List.of("cart.item.added", "cart.checkout")),
    TRENDING_PRODUCT("trending_product", List.of("cart.item.added")),
    SLOW_DELIVERY("slow_delivery", List.of("shipment.picked", "shipment.delivered")),
    ORDER_SURGE("order_surge", List.of("cart.checkout")),
    DELIVERY_COMPLETED("delivery_completed", List.of("cart.checkout", "shipment.delivered"));

    private final String pattern;
    private final List<String> sourceTopics;

    CepPattern(String pattern, List<String> sourceTopics) {
        this.pattern = pattern;
        this.sourceTopics = sourceTopics;
    }

    public String pattern() { return pattern; }
    public List<String> sourceTopics() { return sourceTopics; }
    public String consumerGroup() { return "flink-level3-" + pattern; }

    public static CepPattern fromName(String name) {
        for (CepPattern value : values()) {
            if (value.pattern.equals(name)) return value;
        }
        throw new IllegalArgumentException("unsupported CEP pattern: " + name);
    }
}
