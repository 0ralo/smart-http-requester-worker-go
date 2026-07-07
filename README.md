# Smart HTTP Requester Worker (Go)

A worker for processing HTTP tasks from a RabbitMQ queue. It consumes tasks, sends HTTP requests, stores results in PostgreSQL, and manages retry and failed-task handling.

## Overview

This service is the Go implementation of the worker component in the Smart HTTP Requester system. Its purpose is to receive queued tasks, execute requests against external HTTP endpoints, and persist the outcome in a database.

## Features

- consume tasks from RabbitMQ;
- execute HTTP requests with GET, POST, PUT, PATCH, and DELETE methods;
- pass request headers and body;
- store execution results in PostgreSQL;
- update attempt counters and forward tasks to retry or failed queues;
- shut down gracefully on SIGINT and SIGTERM.

## How it works

1. The worker reads configuration from environment variables or a .env file.
2. It connects to PostgreSQL and RabbitMQ.
3. It starts listening to the task queue.
4. For each message:
   - it extracts the task identifier;
   - loads the task from the database;
   - performs the HTTP request;
   - saves the result or reschedules the task for retry.

## Architecture

The project is organized into several modules:

- config — configuration loading;
- db — PostgreSQL access and task models;
- rmq — RabbitMQ connection and retry/failed queue handling;
- main.go — entry point and message processing loop.

## Project structure

```text
.
├── config/
│   └── config.go
├── db/
│   └── db.go
├── rmq/
│   └── rmq.go
├── main.go
├── go.mod
├── go.sum
├── .env.example
├── .gitignore
└── README.md
```

## Requirements

- Go 1.26+
- PostgreSQL
- RabbitMQ

## Configuration

Copy the example configuration file:

```bash
cp .env.example .env
```

The following environment variables are supported:

- DATABASE_URL — PostgreSQL connection string;
- RMQ_URL — RabbitMQ address;
- RMQ_QUEUE — task queue name;
- RMQ_CONSUMER — consumer name.

Example:

```env
DATABASE_URL=postgresql://dev:dev@127.0.0.1:5432/development?sslmode=disable
RMQ_URL=amqp://guest:guest@localhost:5672/
RMQ_QUEUE=tasks.queue
RMQ_CONSUMER=go-consumer
```

## Running

Install dependencies:

```bash
go mod download
```

Run the application:

```bash
go run .
```

Or build a binary:

```bash
go build -o worker
./worker
```

## Dependencies

The project uses:

- github.com/joho/godotenv — load .env files;
- github.com/jmoiron/sqlx — convenient PostgreSQL access;
- github.com/lib/pq — PostgreSQL driver;
- github.com/rabbitmq/amqp091-go — RabbitMQ client;
- github.com/google/uuid — UUID handling.

## Future improvements

Possible follow-up improvements include:

- extracting the processing logic into a dedicated package;
- reusing a shared HTTP client;
- more structured logging and metrics;
- test coverage for the main processing flows.
