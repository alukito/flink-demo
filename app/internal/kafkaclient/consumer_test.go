package kafkaclient

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingBroadcaster struct {
	events []event.EventEnvelope
	raw    [][]byte
	alerts [][]byte
}

func (r *recordingBroadcaster) Broadcast(ev event.EventEnvelope) { r.events = append(r.events, ev) }
func (r *recordingBroadcaster) BroadcastRaw(data []byte) {
	r.raw = append(r.raw, append([]byte(nil), data...))
}
func (r *recordingBroadcaster) BroadcastCEPAlertRaw(data []byte) {
	r.alerts = append(r.alerts, append([]byte(nil), data...))
}

func TestConsumerForwardsInputAsTypedEventAndFlinkOutputAsRawJSON(t *testing.T) {
	recorder := &recordingBroadcaster{}
	consumer := NewConsumer("localhost:9092", recorder)
	input, _ := json.Marshal(event.NewEvent("product.listed", "seller", "Seller", "seller", map[string]any{"product_id": "p1"}))
	raw := []byte("{\n \"metric\":\"tx_count\", \"scope\":\"window\", \"window_end\":\"2026-07-18T10:05:00Z\", \"value\":7, \"detail\":{}\n}")
	alert := []byte(`{"alert_id":"slow_delivery:o1","pattern":"slow_delivery","detected_at":"2026-08-01T10:07:00Z","detail":{"order_id":"o1"}}`)

	require.NoError(t, consumer.forward("product.listed", input))
	require.NoError(t, consumer.forward("flink.window.stats", raw))
	require.NoError(t, consumer.forward("flink.cep.alerts", alert))

	assert.Len(t, recorder.events, 1)
	assert.Len(t, recorder.raw, 1)
	assert.True(t, bytes.Equal(raw, recorder.raw[0]))
	assert.Len(t, recorder.alerts, 1)
	assert.True(t, bytes.Equal(alert, recorder.alerts[0]))
}

func TestConsumerSubscribesToCEPAlerts(t *testing.T) {
	assert.Contains(t, consumerTopics, "flink.cep.alerts")
}

func TestConsumerRejectsMalformedJSON(t *testing.T) {
	consumer := NewConsumer("localhost:9092", &recordingBroadcaster{})

	assert.Error(t, consumer.forward("flink.window.stats", []byte(`not-json`)))
	assert.Error(t, consumer.forward("flink.cep.alerts", []byte(`not-json`)))
}
