package kafkaclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/kuang/flink-demo/internal/event"
	"github.com/segmentio/kafka-go"
)

// Producer writes events to Kafka topics.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer creates a Kafka producer for the given broker address.
// Uses Async mode so writes return immediately without waiting for
// broker acknowledgments — the Go server's in-memory state is the
// source of truth, and Kafka events are fire-and-forget notifications.
func NewProducer(addr string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(addr),
			Async:        true,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireNone,
		},
	}
}

// Write serializes an event and writes it to the specified Kafka topic.
// In Async mode, this returns immediately without waiting for broker
// acknowledgment. Errors are logged but not returned to the caller.
func (p *Producer) Write(ctx context.Context, topic string, ev event.EventEnvelope) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(ev.EventID),
		Value: data,
	})
	if err != nil {
		slog.Error("failed to write to kafka",
			"topic", topic,
			"event_type", ev.EventType,
			"event_id", ev.EventID,
			"error", err,
		)
		return err
	}

	slog.Debug("kafka event produced",
		"topic", topic,
		"event_type", ev.EventType,
		"event_id", ev.EventID,
	)
	return nil
}

// Close closes the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
