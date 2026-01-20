// Package kafka provides Kafka implementation for user activity tracking
package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// ActivityProducer is a specialized producer for user activity events
type ActivityProducer struct {
	writer *kafkago.Writer
}

// UserActivityEvent represents a user activity event
type UserActivityEvent struct {
	EventID   string                 `json:"event_id"`
	EventType string                 `json:"event_type"`
	UserID    *string                `json:"user_id,omitempty"`
	SessionID string                 `json:"session_id"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// NewActivityProducer creates a specialized producer for user activity
func NewActivityProducer(brokers []string) *ActivityProducer {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        "user-activity",
		Balancer:     &kafkago.Hash{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
	}

	return &ActivityProducer{writer: writer}
}

func (p *ActivityProducer) PublishActivity(ctx context.Context, event *UserActivityEvent) error {
	// Auto-generate event ID if not provided
	if event.EventID == "" {
		event.EventID = uuid.New().String()
	}

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafkago.Message{
		Key:   []byte(event.SessionID),
		Value: value,
		Time:  event.Timestamp,
	}

	return p.writer.WriteMessages(ctx, msg)
}

func (p *ActivityProducer) Close() error {
	return p.writer.Close()
}
