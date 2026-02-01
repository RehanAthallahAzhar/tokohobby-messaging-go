package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageHandler func(ctx context.Context, body []byte) error
type Consumer struct {
	rmq         *RabbitMQ
	queueName   string
	workerCount int
	handler     MessageHandler
}

type ConsumerOptions struct {
	QueueName   string
	WorkerCount int  // amount of goroutine workers
	AutoAck     bool // Auto acknowledge
}

func NewConsumer(rmq *RabbitMQ, opts ConsumerOptions, handler MessageHandler) *Consumer {
	if opts.WorkerCount == 0 {
		opts.WorkerCount = 5 // Default 5 workers
	}
	return &Consumer{
		rmq:         rmq,
		queueName:   opts.QueueName,
		workerCount: opts.WorkerCount,
		handler:     handler,
	}
}

// DeclareQueue create queue if not exists
func (c *Consumer) DeclareQueue(durable, autoDelete bool) error {
	ch, err := c.rmq.GetChannel()
	if err != nil {
		return err
	}
	_, err = ch.QueueDeclare(
		c.queueName, // name
		durable,     // durable
		autoDelete,  // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	return err
}

// BindQueue bind queue to exchange with routing key
func (c *Consumer) BindQueue(exchange, routingKey string) error {
	ch, err := c.rmq.GetChannel()
	if err != nil {
		return err
	}
	return ch.QueueBind(
		c.queueName, // queue name
		routingKey,  // routing key
		exchange,    // exchange
		false,       // no-wait
		nil,         // args
	)
}

// Start start consume messages with worker pool
func (c *Consumer) Start(ctx context.Context) error {
	ch, err := c.rmq.GetChannel()
	if err != nil {
		return err
	}
	// Register consumer
	msgs, err := ch.Consume(
		c.queueName, // queue
		"",          // consumer tag (auto-generated)
		false,       // auto-ack (manual ack untuk reliability)
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Printf("Consumer started on queue '%s' with %d workers", c.queueName, c.workerCount)

	// Create worker pool with  goroutines
	var wg sync.WaitGroup

	// Channel for distribute messages to workers
	jobChan := make(chan amqp.Delivery, c.workerCount*2)

	// Start workers
	for i := 0; i < c.workerCount; i++ {
		wg.Add(1)
		go c.worker(ctx, i, jobChan, &wg)
	}
	// Distribute messages to workers
	go func() {
		for msg := range msgs {
			select {
			case jobChan <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	log.Println("Shutting down consumer...")

	// Close job channel and wait workers done
	close(jobChan)
	wg.Wait()
	log.Println("Consumer stopped gracefully")
	return nil
}

// worker goroutine for process messages
func (c *Consumer) worker(ctx context.Context, id int, jobs <-chan amqp.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("👷 Worker %d started", id)
	for {
		select {
		case msg, ok := <-jobs:
			if !ok {
				log.Printf("👷 Worker %d stopped", id)
				return
			}

			// Process message
			err := c.handler(ctx, msg.Body)

			if err != nil {
				log.Printf("Worker %d: Error processing message: %v", id, err)
				// Nack with requeue
				msg.Nack(false, true)
			} else {
				log.Printf("Worker %d: Message processed successfully", id)
				// Ack message
				msg.Ack(false)
			}
		case <-ctx.Done():
			log.Printf("Worker %d stopping...", id)
			return
		}
	}
}

// UnmarshalMessage helper for unmarshal JSON message
func UnmarshalMessage(body []byte, v interface{}) error {
	return json.Unmarshal(body, v)
}
