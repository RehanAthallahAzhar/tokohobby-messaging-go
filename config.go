package messaging

import "time"

type RabbitMQConfig struct {
	URL            string
	MaxRetries     int
	RetryDelay     time.Duration
	PrefetchCount  int
	ReconnectDelay time.Duration
}

func DefaultConfig() *RabbitMQConfig {
	return &RabbitMQConfig{
		URL:            "amqp://admin:admin123@localhost:5672/tokohobby",
		MaxRetries:     3,
		RetryDelay:     2 * time.Second,
		PrefetchCount:  10,
		ReconnectDelay: 5 * time.Second,
	}
}
