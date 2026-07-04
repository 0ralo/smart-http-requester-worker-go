package main

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type JSONB map[string]any

func (j *JSONB) Scan(value any) error {
	if value == nil {
		*j = nil
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("invalid type for JSONB")
	}

	return json.Unmarshal(bytes, j)
}

func (j JSONB) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

type Task struct {
	ID           uuid.UUID  `db:"id"`
	UserID       *int       `db:"user_id"` // nullable
	URL          string     `db:"url"`
	Method       string     `db:"method"`
	Headers      JSONB      `db:"headers"`
	Result       JSONB      `db:"result"`
	Body         *string    `db:"body"` // nullable
	Status       string     `db:"status"`
	AttemptCount int        `db:"attempt_count"`
	MaxAttempts  int        `db:"max_attempts"`
	CreatedAt    *time.Time `db:"created_at"`
	UpdatedAt    *time.Time `db:"updated_at"`
}

func main() {
	db, err := sqlx.Connect("postgres", "postgresql://dev:dev@127.0.0.1:5432/development?sslmode=disable")
	if err != nil {
		log.Fatal("Error while connecting to databse %v", err)
		os.Exit(-1)
	}
	defer db.Close()

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Error while connecting to rmq")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Error while getting channel")
	}
	defer ch.Close()
	msgs, err := ch.Consume(
		"tasks.queue",
		"go-consumer", // consumer
		true,          // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		log.Fatal("Error while getting consume thing")
	}

	for d := range msgs {
		log.Printf(" [x] %s", d.Body)
	}

}
