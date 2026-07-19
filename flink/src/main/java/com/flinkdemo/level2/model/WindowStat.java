package com.flinkdemo.level2.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowStat implements Serializable {
    private String metric;
    private String scope;
    @JsonProperty("window_end") private String windowEnd;
    private long value;
    private Map<String, String> detail = new LinkedHashMap<>();

    public WindowStat() {}
    public WindowStat(String metric, String scope, String windowEnd, long value, Map<String, String> detail) {
        this.metric = metric; this.scope = scope; this.windowEnd = windowEnd; this.value = value; this.detail = new LinkedHashMap<>(detail);
    }
    public String getMetric() { return metric; }
    public String getScope() { return scope; }
    public String getWindowEnd() { return windowEnd; }
    public long getValue() { return value; }
    public Map<String, String> getDetail() { return detail; }
}
