package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"0ralo.website/m/config"
	"0ralo.website/m/db"
	"0ralo.website/m/rmq"
)

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

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages from RabbitMQ queue
	log.Println("✓ Listening for messages...")
	for {
		select {
		case d := <-rmqConn.Msgs:
			log.Printf("[x] Received message: %s", d.Body)
			// TODO: Implement task processing logic here
		case sig := <-sigChan:
			log.Printf("Received signal: %v", sig)
			return
		}
	}
}
