package rmq

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection struct for managing RabbitMQ connections
type Connection struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Msgs    <-chan amqp.Delivery
}

// ConnectRMQ establishes connection to RabbitMQ and sets up the queue consumer
func ConnectRMQ(rmqURL, queueName, consumerName string) (*Connection, error) {
	conn, err := amqp.Dial(rmqURL)
	if err != nil {
		log.Printf("Error connecting to RabbitMQ: %v", err)
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Error creating channel: %v", err)
		conn.Close()
		return nil, err
	}

	msgs, err := ch.Consume(
		queueName,     // queue name
		consumerName,  // consumer name
		true,          // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		log.Printf("Error setting up consumer: %v", err)
		ch.Close()
		conn.Close()
		return nil, err
	}

	return &Connection{
		Conn:    conn,
		Channel: ch,
		Msgs:    msgs,
	}, nil
}

// Close closes the RabbitMQ connection
func (rc *Connection) Close() error {
	if rc.Channel != nil {
		if err := rc.Channel.Close(); err != nil {
			log.Printf("Error closing channel: %v", err)
			return err
		}
	}

	if rc.Conn != nil {
		if err := rc.Conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
			return err
		}
	}

	return nil
}
