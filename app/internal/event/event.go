package event

import (
	"time"

	"github.com/google/uuid"
)

// EventEnvelope is the standard wrapper for all events on Kafka topics.
type EventEnvelope struct {
	EventID   string         `json:"event_id"`
	EventType string         `json:"event_type"`
	ActorID   string         `json:"actor_id"`
	ActorName string         `json:"actor_name"`
	ActorRole string         `json:"actor_role"`
	Timestamp string         `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

// NewEvent creates a new EventEnvelope with a generated UUID and current timestamp.
func NewEvent(eventType, actorID, actorName, actorRole string, payload map[string]any) EventEnvelope {
	return EventEnvelope{
		EventID:   uuid.New().String(),
		EventType: eventType,
		ActorID:   actorID,
		ActorName: actorName,
		ActorRole: actorRole,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}
}
