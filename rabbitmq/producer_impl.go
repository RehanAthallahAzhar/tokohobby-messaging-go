package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message represents a message (duplicated to avoid import cycle)
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp int64
}

// Producer implements messaging.Producer for RabbitMQ
type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewProducerFromMessaging creates RabbitMQ producer (called by messaging factory)
func NewProducerFromMessaging(brokers []string, topic string) *Producer {
	conn, err := amqp.Dial("amqp://" + brokers[0])
	if err != nil {
		return nil
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil
	}

	return &Producer{
		conn:    conn,
		channel: channel,
	}
}

func (p *Producer) Publish(ctx context.Context, msg *Message) error {
	headers := make(amqp.Table)
	for k, v := range msg.Headers {
		headers[k] = v
	}

	publishing := amqp.Publishing{
		ContentType: "application/json",
		Body:        msg.Value,
		Headers:     headers,
	}

	return p.channel.PublishWithContext(
		ctx,
		"",        // exchange
		msg.Topic, // routing key
		false,
		false,
		publishing,
	)
}

func (p *Producer) PublishBatch(ctx context.Context, messages []*Message) error {
	for _, msg := range messages {
		if err := p.Publish(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) Close() error {
	if p.channel != nil {
		p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
