package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type ActivityProducer struct {
	writer *kafka.Writer
}

func NewActivityProducer(brokers []string) *ActivityProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "user-activity",
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true, // Non-blocking writes
	}

	log.WithFields(log.Fields{
		"brokers": brokers,
		"topic":   "user-activity",
	}).Info("Kafka activity producer initialized")

	return &ActivityProducer{writer: writer}
}

type UserActivityEvent struct {
	EventID   string                 `json:"event_id"`
	EventType string                 `json:"event_type"`
	UserID    *string                `json:"user_id,omitempty"`
	SessionID string                 `json:"session_id"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

func (p *ActivityProducer) PublishActivity(ctx context.Context, event *UserActivityEvent) error {
	// Generate event ID if not provided
	if event.EventID == "" {
		event.EventID = uuid.New().String()
	}

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	value, err := json.Marshal(event)
	if err != nil {
		log.WithError(err).Error("Failed to marshal activity event")
		return err
	}

	msg := kafka.Message{
		Key:   []byte(event.SessionID), // Partition by session
		Value: value,
	}

	// Async write - errors are logged but don't block
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		log.WithError(err).WithField("event_type", event.EventType).Error("Failed to publish activity")
		return err
	}

	log.WithFields(log.Fields{
		"event_type": event.EventType,
		"user_id":    event.UserID,
	}).Debug("Published activity event")

	return nil
}

func (p *ActivityProducer) Close() error {
	return p.writer.Close()
}
