package messaging

// Config contains broker configuration
type Config struct {
	// BrokerType specifies which broker to use ("kafka" or "rabbitmq")
	BrokerType string

	// Brokers is list of broker addresses
	Brokers []string

	// Topic is the topic/queue name for consumer
	Topic string

	// GroupID is consumer group ID (Kafka) or queue name (RabbitMQ)
	GroupID string

	// ClientID is optional client identifier
	ClientID string

	// RetryEnabled enables retry mechanism
	RetryEnabled bool

	// MaxRetries is maximum number of retries
	MaxRetries int
}

// ProducerConfig extends Config with producer-specific options
type ProducerConfig struct {
	Config

	// AsyncMode enables fire-and-forget publishing
	AsyncMode bool

	// BatchSize for batching messages
	BatchSize int
}

// ConsumerConfig extends Config with consumer-specific options
type ConsumerConfig struct {
	Config

	// AutoCommit enables automatic offset commit (Kafka)
	AutoCommit bool

	// PrefetchCount for RabbitMQ
	PrefetchCount int
}
