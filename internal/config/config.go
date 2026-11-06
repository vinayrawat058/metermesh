// Package config loads node-specific settings from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the settings for one MeterMesh node.
type Config struct {
	NodeID        string
	ListenAddr    string
	PostgresURL   string
	RedisURL      string
	QueueSize     int
	FlushInterval time.Duration
}

// Load reads process environment variables. Unset values fall back to a
// single-node local default so `go run ./cmd/server` can start on its own.
func Load() (Config, error) {
	cfg := Config{
		NodeID:        env("NODE_ID", "local"),
		ListenAddr:    env("LISTEN_ADDR", ":8080"),
		PostgresURL:   env("POSTGRES_URL", "postgres://metermesh:metermesh@localhost:5432/metermesh?sslmode=disable"),
		RedisURL:      env("REDIS_URL", "redis://localhost:6379/0"),
		QueueSize:     1024,
		FlushInterval: 5 * time.Second,
	}

	if raw := os.Getenv("FLUSH_INTERVAL"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, fmt.Errorf("FLUSH_INTERVAL must be a positive duration such as 5s")
		}
		cfg.FlushInterval = d
	}

	if raw := os.Getenv("USAGE_QUEUE_SIZE"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("USAGE_QUEUE_SIZE must be a positive integer")
		}
		cfg.QueueSize = n
	}

	return cfg, nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
