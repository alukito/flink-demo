package com.flinkdemo.level2;

public enum MetricDefinition {
    LISTINGS_COUNT("listings_count", "product.listed", true, false, false, false),
    CART_ADDS_COUNT("cart_adds_count", "cart.item.added", true, false, false, false),
    TX_COUNT("tx_count", "cart.checkout", true, true, false, false),
    CONFIRMED_ORDERS("confirmed_orders", "order.confirmed", true, false, false, false),
    DELIVERED_ORDERS("delivered_orders", "shipment.delivered", true, true, false, false),
    TOP_PRODUCT("top_product", "cart.item.added", true, false, false, true),
    REVENUE("revenue", "cart.checkout", false, true, true, false);

    private final String metric; private final String sourceTopic; private final boolean window; private final boolean daily; private final boolean revenue; private final boolean topProduct;
    MetricDefinition(String metric, String sourceTopic, boolean window, boolean daily, boolean revenue, boolean topProduct) {
        this.metric = metric; this.sourceTopic = sourceTopic; this.window = window; this.daily = daily; this.revenue = revenue; this.topProduct = topProduct;
    }
    public String metric() { return metric; }
    public String sourceTopic() { return sourceTopic; }
    public boolean hasWindow() { return window; }
    public boolean hasDaily() { return daily; }
    public boolean isRevenue() { return revenue; }
    public boolean isTopProduct() { return topProduct; }
    public static MetricDefinition fromName(String name) {
        for (MetricDefinition value : values()) if (value.metric.equals(name)) return value;
        throw new IllegalArgumentException("unsupported metric: " + name);
    }
}
