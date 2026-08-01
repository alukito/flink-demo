package com.flinkdemo.level3.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class CepAlert implements Serializable {
    @JsonProperty("alert_id") private final String alertId;
    private final String pattern;
    @JsonProperty("detected_at") private final String detectedAt;
    private final Map<String, Object> detail;

    public CepAlert(String alertId, String pattern, String detectedAt, Map<String, Object> detail) {
        this.alertId = alertId;
        this.pattern = pattern;
        this.detectedAt = detectedAt;
        this.detail = Collections.unmodifiableMap(new LinkedHashMap<>(detail));
    }

    public String getAlertId() { return alertId; }
    public String getPattern() { return pattern; }
    public String getDetectedAt() { return detectedAt; }
    public Map<String, Object> getDetail() { return detail; }
}
