package kafkaclient

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/segmentio/kafka-go"
)

// Broadcaster is the interface for the WebSocket hub's broadcast method.
type Broadcaster interface {
	Broadcast(ev event.EventEnvelope)
}

// Consumer reads Kafka topics and forwards events to the WebSocket hub.
type Consumer struct {
	addr        string
	broadcaster Broadcaster
}

// NewConsumer creates a Kafka consumer that forwards events to the given broadcaster.
func NewConsumer(addr string, hub Broadcaster) *Consumer {
	return &Consumer{addr: addr, broadcaster: hub}
}

// Start begins consuming all input Kafka topics. Blocks until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	topics := []string{
		"product.listed",
		"cart.item.added",
		"cart.checkout",
		"order.confirmed",
		"shipment.picked",
		"shipment.delivered",
	}

	for _, topic := range topics {
		go c.consumeTopic(ctx, topic)
	}

	<-ctx.Done()
	return ctx.Err()
}

func (c *Consumer) consumeTopic(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{c.addr},
		Topic:   topic,
		GroupID: "ws-hub-" + topic,
	})
	defer reader.Close()

	slog.Info("kafka consumer started", "topic", topic)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("failed to read kafka message", "topic", topic, "error", err)
			continue
		}

		var ev event.EventEnvelope
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			slog.Error("failed to unmarshal kafka event", "topic", topic, "error", err)
			continue
		}

		slog.Debug("kafka event consumed", "topic", topic, "event_type", ev.EventType, "event_id", ev.EventID)
		c.broadcaster.Broadcast(ev)
	}
}
