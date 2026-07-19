package kafkaclient

import (
	"encoding/json"
	"testing"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingBroadcaster struct {
	events []event.EventEnvelope
	raw    [][]byte
}

func (r *recordingBroadcaster) Broadcast(ev event.EventEnvelope) { r.events = append(r.events, ev) }
func (r *recordingBroadcaster) BroadcastRaw(data []byte) {
	r.raw = append(r.raw, append([]byte(nil), data...))
}

func TestConsumerForwardsInputAsTypedEventAndFlinkOutputAsRawJSON(t *testing.T) {
	recorder := &recordingBroadcaster{}
	consumer := NewConsumer("localhost:9092", recorder)
	input, _ := json.Marshal(event.NewEvent("product.listed", "seller", "seller", map[string]any{"product_id": "p1"}))

	require.NoError(t, consumer.forward("product.listed", input))
	require.NoError(t, consumer.forward("flink.window.stats", []byte(`{"metric":"tx_count","scope":"window","window_end":"2026-07-18T10:05:00Z","value":7,"detail":{}}`)))

	assert.Len(t, recorder.events, 1)
	assert.Len(t, recorder.raw, 1)
	assert.JSONEq(t, `{"metric":"tx_count","scope":"window","window_end":"2026-07-18T10:05:00Z","value":7,"detail":{}}`, string(recorder.raw[0]))
}

func TestConsumerRejectsMalformedJSON(t *testing.T) {
	consumer := NewConsumer("localhost:9092", &recordingBroadcaster{})

	assert.Error(t, consumer.forward("flink.window.stats", []byte(`not-json`)))
}
