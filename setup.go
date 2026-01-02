package messaging

import "log"

// SetupUserExchange setup exchange for user events
func SetupUserExchange(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}
	// Declare exchange
	err = ch.ExchangeDeclare(
		"user.events", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return err
	}
	log.Println("Exchange 'user.events' created")
	return nil
}

// SetupWelcomeEmailQueue setup queue for welcome email
func SetupWelcomeEmailQueue(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}
	// Declare queue
	_, err = ch.QueueDeclare(
		"user.welcome.email", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return err
	}
	// Bind queue to exchange
	err = ch.QueueBind(
		"user.welcome.email", // queue name
		"user.registered",    // routing key
		"user.events",        // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}
	log.Println("Queue 'user.welcome.email' created and bound")
	return nil
}
