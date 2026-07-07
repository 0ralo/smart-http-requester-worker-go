package db

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// JSONB type for working with JSON fields in PostgreSQL
type JSONB map[string]string

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

// Task struct for HTTP request tasks
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

// ConnectDB establishes a connection to the database
func ConnectDB(dbURL string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		log.Printf("Error connecting to database: %v", err)
		return nil, err
	}

	// Verify the connection
	if err := db.Ping(); err != nil {
		log.Printf("Error pinging database: %v", err)
		return nil, err
	}

	return db, nil
}

func GetTaskById(db *sqlx.DB, task_id uuid.UUID) (Task, error) {
	var task Task
	err := db.Get(&task, "select id, user_id, url, method, headers, body, status, attempt_count, max_attempts, result, created_at, updated_at from tasks where id = $1", task_id)
	return task, err
}

func UpdateTaskCount(db *sqlx.DB, task_id uuid.UUID) (Task, error) {
	var task Task
	err := db.Get(&task, "update tasks set updated_at = now(), attempt_count = attempt_count + 1 where id = $1 returning *", task_id)
	return task, err
}

func UpdateResult(db *sqlx.DB, task_id uuid.UUID, result string) error {
	_, err := db.Exec("update tasks set status='done', attempt_count = attempt_count + 1, updated_at=now(), result=jsonb_build_object('result', $1::text) where id = $2", result, task_id)
	return err
}
