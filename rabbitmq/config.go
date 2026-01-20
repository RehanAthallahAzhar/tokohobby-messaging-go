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
		URL:            "amqp://user:user123@localhost:5672/tokohobby",
		MaxRetries:     3,
		RetryDelay:     2 * time.Second,
		PrefetchCount:  10,
		ReconnectDelay: 5 * time.Second,
	}
}

/*

URL: Connection string ke RabbitMQ
MaxRetries: Berapa kali retry saat publish gagal
RetryDelay: Jeda antar retry
PrefetchCount: Berapa message consumer ambil sekaligus
ReconnectDelay: Jeda sebelum reconnect jika connection putus

*/
