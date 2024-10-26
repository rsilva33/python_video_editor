package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"imersaofc/internal/converter"
	"imersaofc/internal/rabbitmq"

	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
)

// connectPostgres establishes a connection with PostgreSQL using environment variables for configuration.
func connectPostgres() (*sql.DB, error) {
	user := getEnvOrDefault("POSTGRES_USER", "user")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	dbname := getEnvOrDefault("POSTGRES_DB", "converter")
	host := getEnvOrDefault("POSTGRES_HOST", "host.docker.internal")
	sslmode := getEnvOrDefault("POSTGRES_SSL_MODE", "disable")
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=%s", user, password, dbname, host, sslmode)
	db, err := sql.Open("postgres", connStr)

	if err != nil {
		slog.Error("Failed to connect to PostgreSQL", slog.String("error", err.Error()))
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		slog.Error("Failed to ping PostgreSQL", slog.String("error", err.Error()))
		return nil, err
	}

	slog.Info("Connected to PostgreSQL successfully")
	return db, nil
}

// getEnvOrDefault fetches the value of an environment variable or returns a default value if it's not set.
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func main() {
	// mergeChunks("mediatest/media/uploads/1", "merged.mp4")
	db, err := connectPostgres()
	if err != nil {
		panic(err)
	}

	rabbitMQURL := getEnvOrDefault("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	rabbitClient, err := rabbitmq.NewRabbitClient(rabbitMQURL)
	if err != nil {
		panic(err)
	}
	defer rabbitClient.Close()

	convertionExch := getEnvOrDefault("CONVERSION_EXCHANGE", "conversion_exchange")
	queueName := getEnvOrDefault("CONVERSION_QUEUE", "video_conversion_queue")
	convertionKey := getEnvOrDefault("CONVERSION_KEY", "convertion")
	confirmationKey := getEnvOrDefault("CONFIRMATION_KEY", "finish-conversion")
	confirmationQueue := getEnvOrDefault("CONFIRMATION_QUEUE", "video-confirmation_queue")

	vc := converter.NewVideoConverter(rabbitClient, db)
	//vc.Handle([]byte(`{"video_id": 2, "path": "mediatest/media/uploads/"} 	`))

	msgs, err := rabbitClient.ConsumeMessages(convertionExch, convertionKey, queueName)
	if err != nil {
		slog.Error("failed to consume menssages", slog.String("error", err.Error()))
	}

	// fica lendo indefinidamente todas mensagens que chega
	for d := range msgs {
		go func(delivery amqp.Delivery) {
			vc.Handle(delivery, convertionExch, confirmationKey, confirmationQueue)
		}(d)
	}
}
