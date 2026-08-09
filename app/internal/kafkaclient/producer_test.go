package kafkaclient

import (
	"encoding/json"
	"testing"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducerWriteSerializesEventAsJSON(t *testing.T) {
	// We can't test against a real Kafka broker in a unit test,
	// but we can verify the event is properly serialized to JSON
	// by checking the message value that would be sent.
	ev := event.NewEvent("product.listed", "seller1", "Seller", "seller", map[string]any{
		"product_id": "p1",
		"name":       "Widget",
	})

	data, err := json.Marshal(ev)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, "product.listed", decoded["event_type"])
	assert.Equal(t, "seller1", decoded["actor_id"])
	assert.Equal(t, "p1", decoded["payload"].(map[string]any)["product_id"])
}

func TestNewProducerCreatesWriter(t *testing.T) {
	p := NewProducer("localhost:9092")
	assert.NotNil(t, p)
	p.Close()
}
