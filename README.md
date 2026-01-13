# Messaging Library

Shared RabbitMQ library for TokoHobby microservices.

## Features

- ✅ Publisher/Consumer patterns
- ✅ Topic & Direct exchanges
- ✅ Auto-reconnection
- ✅ Error handling & retries
- ✅ Worker pools
- ✅ Exchange & Queue setup utilities

## Usage

### Publisher

```go
import messaging "github.com/RehanAthallahAzhar/tokohobby-messaging-go"

rmq := messaging.NewRabbitMQ(config)
publisher := messaging.NewPublisher(rmq)

err := publisher.Publish(ctx, messaging.PublishOptions{
    Exchange:   "order.events",
    RoutingKey: "order.created",
}, orderEvent)
```

### Consumer

```go
consumer := messaging.NewConsumer(rmq, options, handlerFunc)
consumer.DeclareQueue(true, false)
consumer.BindQueue("order.events", "order.#")
consumer.Start(ctx)
```

## Exchange Setup

```go
// Order events (Topic)
messaging.SetupOrderExchange(rmq)

// User events (Direct)
messaging.SetupUserExchange(rmq)

// Product events (Topic)
messaging.SetupProductExchange(rmq)
```

## Configuration

```go
config := &RabbitMQConfig{
    URL:            "amqp://admin:admin123@localhost:5672/tokohobby",
    MaxRetries:     5,
    RetryDelay:     5 * time.Second,
    PrefetchCount:  10,
    ReconnectDelay: 5 * time.Second,
}
```

## Local Development

This library is used as a local module:

```go
// go.mod
replace github.com/RehanAthallahAzhar/tokohobby-messaging-go => ../messaging
```
