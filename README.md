# TokoHobby Messaging Library

Enterprise-grade, broker-agnostic messaging abstraction for Go microservices.

## Architecture

```
messaging/          # PUBLIC API - Import this in services
├── interface.go    # Producer, Consumer interfaces
├── config.go       # Config structs
└── producer.go     # Factory functions

kafka/              # Kafka implementation (internal)
├── producer.go
└── consumer.go

rabbitmq/           # RabbitMQ implementation (internal)
├── producer_impl.go
├── consumer_impl.go
├── publisher.go    # Legacy/specific use
├── consumer.go     # Legacy/specific use
└── ...
```

## Design Principles

### ✅ Broker-Agnostic

Services depend on **interfaces**, not implementations:

```go
import "github.com/RehanAthallahAzhar/tokohobby-messaging-go/messaging"

// Config-driven broker selection
cfg := &messaging.ProducerConfig{
    Config: messaging.Config{
        BrokerType: "kafka",  // or "rabbitmq"
        Brokers:    []string{"localhost:9092"},
        Topic:      "events",
    },
    AsyncMode: true,
    BatchSize: 100,
}

producer, _ := messaging.NewProducer(cfg)
defer producer.Close()
```

### ✅ Clean Architecture

- **`messaging/`** = Public API (services import this)
- **`kafka/`, `rabbitmq/`** = Implementation details
- **Factory pattern** = Hide broker selection logic

### ✅ Interface-Based

```go
type Producer interface {
    Publish(ctx context.Context, msg *Message) error
    PublishBatch(ctx context.Context, messages []*Message) error
    Close() error
}

type Consumer interface {
    Consume(ctx context.Context, handler MessageHandler) error
    Close() error
}
```

## Usage Examples

### Generic Producer (Recommended)

```go
import "github.com/RehanAthallahAzhar/tokohobby-messaging-go/messaging"

cfg := &messaging.ProducerConfig{
    Config: messaging.Config{
        BrokerType: "kafka",
        Brokers:    []string{"localhost:9092"},
        Topic:      "user-activity",
    },
    AsyncMode: true,
    BatchSize: 100,
}

producer, _ := messaging.NewProducer(cfg)
defer producer.Close()

msg := &messaging.Message{
    Value: []byte(`{"event":"login"}`),
    Key:   []byte("user-123"),
}

producer.Publish(ctx, msg)
```

### Generic Consumer

```go
cfg := &messaging.ConsumerConfig{
    Config: messaging.Config{
        BrokerType: "kafka",
        Brokers:    []string{"localhost:9092"},
        Topic:      "user-activity",
        GroupID:    "analytics-service",
    },
    AutoCommit: false,
}

consumer, _ := messaging.NewConsumer(cfg)
defer consumer.Close()

consumer.Consume(ctx, func(ctx context.Context, msg *messaging.Message) error {
    // Process message
    fmt.Printf("Received: %s\n", string(msg.Value))
    return nil
})
```

### Switching Brokers

**Zero code changes** - just update config:

```go
// Change from Kafka to RabbitMQ
cfg.BrokerType = "rabbitmq"
cfg.Brokers = []string{"localhost:5672"}
```

## Benefits

### 1. Testability

Mock the interface:

```go
type mockProducer struct{}

func (m *mockProducer) Publish(ctx, msg) error { return nil }
func (m *mockProducer) PublishBatch(ctx, msgs) error { return nil }
func (m *mockProducer) Close() error { return nil }

// Test service without real broker
service := NewService(mockProducer)
```

### 2. Flexibility

- Swap Kafka ↔ RabbitMQ via config
- No code changes in services
- Easy A/B testing of brokers

### 3. Maintainability

- Broker logic isolated
- Single responsibility
- Easy to extend (add new brokers)

## Legacy Support

Existing RabbitMQ code still works:

```go
import "github.com/RehanAthallahAzhar/tokohobby-messaging-go/rabbitmq"

rmq, _ := rabbitmq.NewRabbitMQ(config)
publisher := rabbitmq.NewPublisher(rmq)
```

## Key Files

- **messaging/interface.go** - Core interfaces
- **messaging/config.go** - Configuration structs
- **messaging/producer.go** - Factory functions
- **kafka/producer.go** - Kafka implementation
- **rabbitmq/producer_impl.go** - RabbitMQ implementation

---

**Status:** ✅ Production-Ready  
**Version:** 3.0 - Enterprise Architecture  
**Pattern:** Factory + Interface Segregation
