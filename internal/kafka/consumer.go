package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Handler processes a single Kafka message.
// Return nil  → message is committed (success).
// Return err  → message is committed anyway, but the handler is expected to have
//
//	persisted retry/DLQ state before returning so it isn't silently lost.
//	(We commit to avoid the consumer getting stuck on a poison pill.)
type Handler func(ctx context.Context, msg kafka.Message) error

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: time.Second,
			StartOffset:    kafka.FirstOffset,
			// Give the broker enough time before declaring the consumer dead.
			SessionTimeout:   30 * time.Second,
			HeartbeatInterval: 3 * time.Second,
		}),
	}
}

// Run blocks and delivers messages to handler until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // normal shutdown
			}
			return fmt.Errorf("kafka consumer: fetch: %w", err)
		}

		if err := handler(ctx, msg); err != nil {
			log.Printf("[consumer] handler error (topic=%s partition=%d offset=%d): %v",
				msg.Topic, msg.Partition, msg.Offset, err)
		}

		// Always commit — handler is responsible for retry/DLQ persistence.
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[consumer] commit error: %v", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
