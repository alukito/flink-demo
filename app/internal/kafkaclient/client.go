package kafkaclient

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// Client wraps a Kafka admin connection for topic management.
type Client struct {
	addr string
}

// NewClient creates a Kafka client for the given broker address.
func NewClient(addr string) *Client {
	return &Client{addr: addr}
}

// CreateTopics creates all required Kafka topics if they don't exist.
// Retries up to 10 times with 2-second intervals until Kafka is reachable.
func (c *Client) CreateTopics(ctx context.Context) error {
	conn, err := c.dialWithRetry(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	topics := RequiredTopics()
	for _, topic := range topics {
		err := conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
		if err != nil {
			slog.Warn("failed to create topic (may already exist)", "topic", topic, "error", err)
			continue
		}
		slog.Info("created kafka topic", "topic", topic)
	}

	return nil
}

func (c *Client) dialWithRetry(ctx context.Context) (*kafka.Conn, error) {
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		conn, err := kafka.DialContext(ctx, "tcp", c.addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		slog.Warn("kafka connection attempt failed", "attempt", attempt+1, "error", err)
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("kafka unreachable after retries: %w", lastErr)
}

// Ping checks if the Kafka broker is reachable.
func (c *Client) Ping(ctx context.Context) error {
	d := net.Dialer{Timeout: 3 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

// Close releases any resources held by the client.
// The current implementation dials per-operation, so this is a no-op,
// but it satisfies the lifecycle interface expected by callers.
func (c *Client) Close() error {
	return nil
}
