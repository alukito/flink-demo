package kafkaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/segmentio/kafka-go"
)

// Broadcaster is the interface for the WebSocket hub's broadcast method.
type Broadcaster interface {
	Broadcast(ev event.EventEnvelope)
	BroadcastRaw(data []byte)
	BroadcastCEPAlertRaw(data []byte)
}

var consumerTopics = []string{
	"product.listed",
	"cart.item.added",
	"cart.checkout",
	"order.confirmed",
	"shipment.picked",
	"shipment.delivered",
	"flink.window.stats",
	"flink.cep.alerts",
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
	for _, topic := range consumerTopics {
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

		if err := c.forward(topic, msg.Value); err != nil {
			slog.Error("failed to forward kafka message", "topic", topic, "error", err)
		}
	}
}

func (c *Consumer) forward(topic string, value []byte) error {
	if !json.Valid(value) {
		return fmt.Errorf("invalid JSON on %s", topic)
	}
	if topic == "flink.window.stats" {
		c.broadcaster.BroadcastRaw(value)
		slog.Debug("flink result consumed", "topic", topic)
		return nil
	}
	if topic == "flink.cep.alerts" {
		c.broadcaster.BroadcastCEPAlertRaw(value)
		slog.Debug("flink CEP alert consumed", "topic", topic)
		return nil
	}

	var ev event.EventEnvelope
	if err := json.Unmarshal(value, &ev); err != nil {
		return fmt.Errorf("decode event on %s: %w", topic, err)
	}
	c.broadcaster.Broadcast(ev)
	slog.Debug("kafka event consumed", "topic", topic, "event_type", ev.EventType, "event_id", ev.EventID)
	return nil
}
