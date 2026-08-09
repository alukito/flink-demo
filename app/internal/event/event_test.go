package event

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEventCreatesEnvelope(t *testing.T) {
	payload := map[string]any{"product_id": "abc", "name": "Widget"}
	ev := NewEvent("product.listed", "seller1", "Alex", "seller", payload)

	assert.NotEmpty(t, ev.EventID)
	assert.Equal(t, "product.listed", ev.EventType)
	assert.Equal(t, "seller1", ev.ActorID)
	assert.Equal(t, "Alex", ev.ActorName)
	assert.Equal(t, "seller", ev.ActorRole)
	assert.NotEmpty(t, ev.Timestamp)
	assert.Equal(t, payload, ev.Payload)
}

func TestNewEventGeneratesUniqueIDs(t *testing.T) {
	ev1 := NewEvent("test", "a", "Alex", "buyer", nil)
	ev2 := NewEvent("test", "a", "Alex", "buyer", nil)
	require.NotEqual(t, ev1.EventID, ev2.EventID, "each event should have a unique ID")
}

func TestEventEnvelopeJSONRoundtrip(t *testing.T) {
	ev := NewEvent("cart.checkout", "buyer1", "Alex", "buyer", map[string]any{
		"order_id":  "ord-123",
		"seller_id": "seller1",
		"total":     float64(1500),
	})
	data, err := json.Marshal(ev)
	require.NoError(t, err)

	var decoded EventEnvelope
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, ev.EventID, decoded.EventID)
	assert.Equal(t, ev.EventType, decoded.EventType)
	assert.Equal(t, ev.ActorID, decoded.ActorID)
	assert.Equal(t, ev.ActorName, decoded.ActorName)
	assert.Equal(t, "ord-123", decoded.Payload["order_id"])
}
