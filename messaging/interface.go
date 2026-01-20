package messaging

import "context"

// Message represents a generic message for any broker
type Message struct {
	Topic     string                 // Topic/Queue/Exchange name
	Key       []byte                 // Partition key (Kafka) or routing key (RabbitMQ)
	Value     []byte                 // Message payload
	Headers   map[string]string      // Message headers
	Metadata  map[string]interface{} // Additional metadata
	Timestamp int64                  // Unix timestamp
}

// Producer defines the interface for publishing messages
type Producer interface {
	// Publish sends a single message
	Publish(ctx context.Context, msg *Message) error

	// PublishBatch sends multiple messages
	PublishBatch(ctx context.Context, messages []*Message) error

	// Close closes producer and releases resources
	Close() error
}

// Consumer defines the interface for consuming messages
type Consumer interface {
	// Consume starts consuming messages
	// Handler is called for each message
	// Returns when context is cancelled or fatal error occurs
	Consume(ctx context.Context, handler MessageHandler) error

	// Close closes consumer and releases resources
	Close() error
}

// MessageHandler processes incoming messages
// Return error to reject/nack message (broker will retry)
// Return nil to ack message
type MessageHandler func(ctx context.Context, msg *Message) error
