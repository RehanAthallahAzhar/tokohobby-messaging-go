package kafka

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Message represents a message (duplicated to avoid import cycle)
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Metadata  map[string]interface{}
	Timestamp int64
}

// ProducerConfig for Kafka producer
type ProducerConfig struct {
	Brokers   []string
	Topic     string
	AsyncMode bool
	BatchSize int
}

// Producer implements messaging.Producer for Kafka
type Producer struct {
	writer *kafkago.Writer
}

// NewProducerFromMessaging creates Kafka producer from messaging config
// This is called by messaging factory
func NewProducerFromMessaging(brokers []string, topic string, asyncMode bool, batchSize int) *Producer {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		BatchSize:    batchSize,
		BatchTimeout: 10 * time.Millisecond,
		Async:        asyncMode,
	}

	return &Producer{writer: writer}
}

// NewProducer creates a Kafka producer from Kafka-specific config
func NewProducer(cfg *ProducerConfig) *Producer {
	return NewProducerFromMessaging(cfg.Brokers, cfg.Topic, cfg.AsyncMode, cfg.BatchSize)
}

func (p *Producer) Publish(ctx context.Context, msg *Message) error {
	kafkaMsg := kafkago.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Time:  time.Unix(msg.Timestamp, 0),
	}

	// Convert headers
	if len(msg.Headers) > 0 {
		kafkaMsg.Headers = make([]kafkago.Header, 0, len(msg.Headers))
		for k, v := range msg.Headers {
			kafkaMsg.Headers = append(kafkaMsg.Headers, kafkago.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	return p.writer.WriteMessages(ctx, kafkaMsg)
}

func (p *Producer) PublishBatch(ctx context.Context, messages []*Message) error {
	kafkaMsgs := make([]kafkago.Message, len(messages))

	for i, msg := range messages {
		kafkaMsgs[i] = kafkago.Message{
			Key:   msg.Key,
			Value: msg.Value,
			Time:  time.Unix(msg.Timestamp, 0),
		}

		if len(msg.Headers) > 0 {
			kafkaMsgs[i].Headers = make([]kafkago.Header, 0, len(msg.Headers))
			for k, v := range msg.Headers {
				kafkaMsgs[i].Headers = append(kafkaMsgs[i].Headers, kafkago.Header{
					Key:   k,
					Value: []byte(v),
				})
			}
		}
	}

	return p.writer.WriteMessages(ctx, kafkaMsgs...)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
