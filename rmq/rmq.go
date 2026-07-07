package rmq

import (
	"encoding/json"
	"fmt"
	"log"

	"math"

	"0ralo.website/m/db"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection struct for managing RabbitMQ connections
type Connection struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Msgs    <-chan amqp.Delivery
}

const DLX_EXCHANGE = "tasks.dlx"

var RETRY_QUEUES = [7]string{
	"tasks.retry.1s",
	"tasks.retry.2s",
	"tasks.retry.4s",
	"tasks.retry.8s",
	"tasks.retry.16s",
	"tasks.retry.32s",
	"tasks.retry.64s",
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
		queueName,    // queue name
		consumerName, // consumer name
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
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

type TaskRMQ struct {
	TaskId uuid.UUID `json:"task_id"`
}

type TaskRMQError struct {
	TaskId   uuid.UUID `json:"task_id"`
	Error    string    `json:"error"`
	Attempts int       `json:"attempts"`
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

func SendToRetry(rc *Connection, task db.Task, error string) error {
	channel := rc.Channel
	index := int(math.Min(float64(task.AttemptCount-1), 7))
	if task.AttemptCount < task.MaxAttempts {
		payload, err := json.Marshal(TaskRMQ{TaskId: task.ID})
		if err != nil {
			log.Printf("Cannot marshal task")
			return err
		}
		err = channel.Publish(
			"tasks.dlx",
			RETRY_QUEUES[index],
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        payload,
			},
		)
		if err != nil {
			log.Printf("Cannot publish message back to rmq")
		}

		log.Printf("send to %v", RETRY_QUEUES[index])
	} else {
		payload, err := json.Marshal(TaskRMQError{TaskId: task.ID, Error: fmt.Sprintf("Attempt limit reached: %v + error: %v", task.AttemptCount, error), Attempts: task.AttemptCount})
		err = channel.Publish(
			"tasks.dlx",
			"failed",
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        payload,
			},
		)
		if err != nil {
			log.Printf("Cannot publish message back to rmq")
			return err
		}
		log.Printf("Successfuly send to failed queue")
	}

	return nil
}
