package com.flinkdemo.level2.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;

public class EventEnvelope implements Serializable {
    @JsonProperty("event_id") private String eventId;
    @JsonProperty("event_type") private String eventType;
    @JsonProperty("actor_id") private String actorId;
    @JsonProperty("actor_role") private String actorRole;
    private String timestamp;
    private JsonNode payload;

    public EventEnvelope() {}
    public EventEnvelope(String eventId, String eventType, String actorId, String actorRole, String timestamp, JsonNode payload) {
        this.eventId = eventId; this.eventType = eventType; this.actorId = actorId; this.actorRole = actorRole; this.timestamp = timestamp; this.payload = payload;
    }
    public String getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public String getActorId() { return actorId; }
    public String getActorRole() { return actorRole; }
    public String getTimestamp() { return timestamp; }
    public JsonNode getPayload() { return payload; }
}
