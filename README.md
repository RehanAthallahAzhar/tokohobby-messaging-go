# TokoHobby Messaging Library

Lightweight, specialized messaging implementations for TokoHobby microservices.

## Structure

```
messaging/
├── internal/
│   └── kafka/
│       └── producer.go     # ActivityProducer for user activity tracking
│
├── rabbitmq/               # RabbitMQ utilities (existing)
│   ├── rabbitmq.go
│   ├── publisher.go
│   ├── consumer.go
│   └── setup.go
│
├── go.mod
└── README.md
```

## Usage

### Kafka Activity Producer

```go
import "github.com/RehanAthallahAzhar/tokohobby-messaging-go/internal/kafka"

// Create producer
producer := kafka.NewActivityProducer([]string{"localhost:9092"})
defer producer.Close()

// Publish user activity  
producer.PublishActivity(ctx, &kafka.UserActivityEvent{
    EventType: "LOGIN",
    UserID:    &userID,
    SessionID: sessionID,
    Metadata: map[string]interface{}{
        "ip_address": "192.168.1.1",
        "user_agent": "Mozilla/5.0...",
    },
})
```

### Event Types

- `LOGIN` - User logged in
- `LOGOUT` - User logged out
- `PRODUCT_VIEW` - User viewed product
- `BLOG_READ` - User read blog post
- `CART_ADD` - Added to cart
- `SEARCH` - Search query

## Design Philosophy

**Simple & Focused**

- No over-abstraction
- Single responsibility packages
- Minimal dependencies
- Direct, idiomatic Go

### Why `internal/kafka`?

- **Encapsulation**: Implementation is private to this library
- **Clarity**: Services know they're using Kafka (intentional coupling for this use case)
- **No import cycles**: Self-contained, no circular dependencies

## RabbitMQ

RabbitMQ utilities are in the `rabbitmq/` package for command/event messaging patterns.

---

**Status:** ✅ Production-Ready  
**Version:** 2.0 - Simplified
