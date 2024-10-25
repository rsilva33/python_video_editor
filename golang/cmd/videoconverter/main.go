package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"imersaofc/internal/converter"

	_ "github.com/lib/pq"
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

	vc := converter.NewVideoConverter(db)
	vc.Handle([]byte(`{"video_id": 1, "path": "mediatest/media/uploads/1/"}`))
}
