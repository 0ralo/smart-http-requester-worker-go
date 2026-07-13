FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /my-go-app .

FROM alpine:3.20

RUN adduser -D appuser
USER appuser

WORKDIR /

COPY --from=builder /my-go-app /my-go-app

EXPOSE 8080

ENTRYPOINT ["/my-go-app"]
