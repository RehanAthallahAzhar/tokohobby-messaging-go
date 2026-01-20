package rabbitmq

import (
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	config    *RabbitMQConfig
	conn      *amqp.Connection // Koneksi TCP ke RabbitMQ
	channel   *amqp.Channel    // Channel untuk komunikasi dengan RabbitMQ
	mu        sync.RWMutex     // for thread-safe operations from multiple goroutines (race condition)
	closed    bool             // flag to indicate if connection is closed
	closeChan chan struct{}    // channel to signal connection close
	reconnect chan struct{}    // channel to signal reconnect
}

func NewRabbitMQ(config *RabbitMQConfig) (*RabbitMQ, error) {
	if config == nil {
		config = DefaultConfig()
	}

	rmq := &RabbitMQ{
		config:    config,
		closeChan: make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}

	if err := rmq.connect(); err != nil {
		return nil, err
	}

	go rmq.handleReconnect()
	return rmq, nil
}

func (r *RabbitMQ) connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error

	// Create connection
	r.conn, err = amqp.Dial(r.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	// Create channel
	r.channel, err = r.conn.Channel()
	if err != nil {
		r.conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Set QoS (Quality of Service) for consumer
	/*
		Qos is a way to control the flow of messages between the client and the server.
		It is used to prevent the server from sending messages to the client faster than the client can process them.

		Prefetch count -> restrict message count
			example: if we set prefetch count is 10, it will send max 10 messages to consumer
					if consumer is processing 1 message, it will send 1 next message
		Prefetch size -> restrict message byte size,
			example: dont send more than 1024 bytes if prefetch size is 1024
		Global -> false means only for this consumer, true means for all consumer
	*/
	err = r.channel.Qos(
		r.config.PrefetchCount, // prefetch count
		0,                      // prefetch size
		false,                  // global
	)
	if err != nil {
		r.channel.Close()
		r.conn.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}
	log.Println("Connected to RabbitMQ successfully")

	// Monitor connection close
	go r.monitorConnection()

	return nil
}

// monitorConnection monitors if connection is closed
func (r *RabbitMQ) monitorConnection() {
	connClose := r.conn.NotifyClose(make(chan *amqp.Error))

	select {
	case err := <-connClose:
		if err != nil {
			log.Printf("RabbitMQ connection closed: %v", err)
			// Trigger reconnect
			select {
			case r.reconnect <- struct{}{}:
			default:
			}
		}
	case <-r.closeChan:
		return
	}
}

// handleReconnect handles automatic reconnection
func (r *RabbitMQ) handleReconnect() {
	for {
		select {
		case <-r.reconnect:
			r.mu.RLock()
			if r.closed {
				r.mu.RUnlock()
				return
			}
			r.mu.RUnlock()
			log.Println("Attempting to reconnect to RabbitMQ...")

			// Wait before reconnect
			time.Sleep(r.config.ReconnectDelay)

			// Try to reconnect
			if err := r.connect(); err != nil {
				log.Printf("Reconnect failed: %v", err)
				// Retry again
				select {
				case r.reconnect <- struct{}{}:
				default:
				}
			} else {
				log.Println("Reconnected to RabbitMQ successfully")
			}

		case <-r.closeChan:
			return
		}
	}
}

// GetChannel returns channel for use
func (r *RabbitMQ) GetChannel() (*amqp.Channel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, fmt.Errorf("RabbitMQ connection is closed")
	}

	if r.channel == nil {
		return nil, fmt.Errorf("channel is not initialized")
	}

	return r.channel, nil
}

// Close closes connection gracefully
func (r *RabbitMQ) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.closeChan)
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
	log.Println("RabbitMQ connection closed")
	return nil
}
