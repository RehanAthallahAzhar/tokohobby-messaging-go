package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	rmq *RabbitMQ
}

func NewPublisher(rmq *RabbitMQ) *Publisher {
	return &Publisher{rmq: rmq}
}

type PublishOptions struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

// Publish send message to RabbitMQ with retry
func (p *Publisher) Publish(ctx context.Context, opts PublishOptions, body interface{}) error {
	// Marshal body to JSON
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Retry logic
	var lastErr error
	for attempt := 0; attempt <= p.rmq.config.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("Retry attempt %d/%d", attempt, p.rmq.config.MaxRetries)
			time.Sleep(p.rmq.config.RetryDelay)
		}

		// Get channel
		ch, err := p.rmq.GetChannel()
		if err != nil {
			lastErr = err
			continue
		}

		// Publish with context timeout
		publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		err = ch.PublishWithContext(
			publishCtx,
			opts.Exchange,   // exchange
			opts.RoutingKey, // routing key
			opts.Mandatory,  // mandatory
			opts.Immediate,  // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         data,
				DeliveryMode: amqp.Persistent, // Persistent untuk survive restart
				Timestamp:    time.Now(),
			},
		)
		cancel()

		if err == nil {
			log.Printf("Message published to exchange=%s, routingKey=%s",
				opts.Exchange, opts.RoutingKey)
			return nil
		}

		lastErr = err
		log.Printf("Publish failed: %v", err)
	}

	return fmt.Errorf("failed to publish after %d attempts: %w",
		p.rmq.config.MaxRetries, lastErr)
}

// DeclareExchange declare exchange if not exists
func (p *Publisher) DeclareExchange(name, kind string, durable bool) error {
	ch, err := p.rmq.GetChannel()
	if err != nil {
		return err
	}
	return ch.ExchangeDeclare(
		name,    // name
		kind,    // type: direct, topic, fanout, headers
		durable, // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
}
