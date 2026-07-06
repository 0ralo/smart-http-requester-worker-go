# Smart HTTP Requester Worker (Go)

Рабочий процесс обработки задач HTTP запросов из RabbitMQ очереди с сохранением результатов в PostgreSQL.

## Структура проекта

```
.
├── config/          # Модуль конфигурации
│   └── config.go   # Загрузка параметров из .env
├── db/              # Модуль работы с базой данных
│   └── db.go       # Подключение к PostgreSQL, типы данных
├── rmq/             # Модуль работы с RabbitMQ
│   └── rmq.go      # Подключение к RMQ, управление потреблением
├── main.go          # Главный файл с основной логикой
├── go.mod          # Модуль Go
├── go.sum          # Хеши зависимостей
└── .env.example    # Пример переменных окружения
```

## Модули

### config/config.go
Загружает конфигурацию из файла `.env` или переменных окружения.

**Переменные окружения:**
- `DATABASE_URL` - строка подключения к PostgreSQL
- `RMQ_URL` - URL подключения к RabbitMQ
- `RMQ_QUEUE` - имя очереди RabbitMQ
- `RMQ_CONSUMER` - имя потребителя

**Значения по умолчанию:**
- БД: `postgresql://dev:dev@127.0.0.1:5432/development?sslmode=disable`
- RMQ: `amqp://guest:guest@localhost:5672/`
- Очередь: `tasks.queue`
- Потребитель: `go-consumer`

### db/db.go
Содержит типы данных и функции для работы с БД:

- **JSONB** - тип для работы с JSON полями PostgreSQL
- **Task** - структура задачи HTTP запроса
- **ConnectDB()** - подключение к БД с проверкой соединения

### rmq/rmq.go
Содержит логику работы с RabbitMQ:

- **Connection** - структура для управления соединением с RMQ
- **ConnectRMQ()** - подключение к RMQ и инициализация потребителя
- **Close()** - корректное закрытие соединения

### main.go
Главная программа с основной логикой:

1. Загрузка конфигурации
2. Подключение к базе данных
3. Подключение к RabbitMQ
4. Цикл обработки сообщений из очереди
5. Graceful shutdown при SIGINT/SIGTERM

## Использование

### Подготовка конфигурации

1. Скопируйте пример конфигурации:
```bash
cp .env.example .env
```

2. Отредактируйте `.env` с вашими параметрами подключения

### Запуск

```bash
go build -o worker
./worker
```

Или прямо:
```bash
go run main.go
```

## Зависимости

- `github.com/joho/godotenv` - загрузка .env файлов
- `github.com/jmoiron/sqlx` - работа с БД
- `github.com/lib/pq` - драйвер PostgreSQL
- `github.com/rabbitmq/amqp091-go` - клиент RabbitMQ
- `github.com/google/uuid` - работа с UUID

## Развитие

Основная логика обработки задач должна быть реализована в:
- `main.go` в блоке `case d := <-rmqConn.Msgs:`
- Рекомендуется создать отдельный модуль для обработки (например, `handler/handler.go`)
