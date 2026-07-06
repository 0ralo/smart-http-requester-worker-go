package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL string
	RMQUrl      string
	RMQQueue    string
	RMQConsumer string
}

func LoadConfig() *Config {
	// Try to load .env file, fall back to environment variables if not found
	_ = godotenv.Load()

	return &Config{
		DatabaseURL: getEnv("DATABASE_URL", "postgresql://dev:dev@127.0.0.1:5432/development?sslmode=disable"),
		RMQUrl:      getEnv("RMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RMQQueue:    getEnv("RMQ_QUEUE", "tasks.queue"),
		RMQConsumer: getEnv("RMQ_CONSUMER", "go-consumer"),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
