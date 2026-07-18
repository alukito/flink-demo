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
func NewProducer(addr string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(addr),
			Async:        false,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
		},
	}
}

// Write serializes an event and writes it to the specified Kafka topic.
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
		return fmt.Errorf("failed to write to kafka: %w", err)
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
