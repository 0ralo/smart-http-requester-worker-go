package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"maps"

	"0ralo.website/m/config"
	"0ralo.website/m/db"
	"0ralo.website/m/rmq"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func process_message(conn *sqlx.DB, data []byte) ([]byte, error) {
	uid, err := get_uid_from_payload(data)
	if err != nil {
		return nil, err
	}
	task, err := db.GetTaskById(conn, *uid)
	if err != nil {
		return nil, err
	}
	if task.AttemptCount == task.MaxAttempts {
		return nil, errors.New("Attempts limit reached")
	}
	data, err = perform_request(task)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func perform_request(task db.Task) ([]byte, error) {
	client := http.Client{}
	context30s, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if slices.Contains([]string{"PATCH", "DELETE", "PUT", "POST", "GET"}, task.Method) {
		req, err := http.NewRequestWithContext(
			context30s,
			task.Method,
			task.URL,
			bytes.NewReader([]byte(*task.Body)),
		)
		if err != nil {
			return nil, err
		}
		for key, value := range maps.All(task.Headers) {
			req.Header.Set(key, value)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if slices.Contains([]int{4, 5}, resp.StatusCode/100) {
			return nil, fmt.Errorf("Error: status code %d", resp.StatusCode)
		}
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return bodyBytes, nil
	} else {
		return nil, errors.New("Unknown method passed")
	}
}

type TaskRMQ struct {
	TaskId uuid.UUID `json:"task_id"`
}

func get_uid_from_payload(data []byte) (*uuid.UUID, error) {
	var task TaskRMQ
	err := json.Unmarshal(data, &task)
	if err != nil {
		log.Println("Cannot unmarshal rmq task")
		return nil, errors.New("Cannot unmarshal rmq task")
	}
	return &task.TaskId, nil
}

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Connect to database
	database, err := db.ConnectDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()
	log.Println("✓ Connected to database")

	// Connect to RabbitMQ
	rmqConn, err := rmq.ConnectRMQ(cfg.RMQUrl, cfg.RMQQueue, cfg.RMQConsumer)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rmqConn.Close()
	log.Println("✓ Connected to RabbitMQ")

	// Initialize Semaphore
	const maxWorkers = 5
	sem := make(chan struct{}, maxWorkers)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages from RabbitMQ queue
	log.Println("✓ Listening for messages...")
	for {
		select {
		case d := <-rmqConn.Msgs:
			log.Printf("[x] Received message: %s", d.Body)
			sem <- struct{}{}
			go func() {
				defer func() {
					<-sem
				}()
				data, err := process_message(database, d.Body)
				if err != nil {
					fmt.Printf("error %v", err)
				} else {
					fmt.Printf("data %s", data)
				}
			}()
		case sig := <-sigChan:
			log.Printf("Received signal: %v", sig)
			return
		}
	}
}
