package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("NODE_ID", "")
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("POSTGRES_URL", "")
	t.Setenv("REDIS_URL", "")
	t.Setenv("USAGE_QUEUE_SIZE", "")
	t.Setenv("FLUSH_INTERVAL", "")
	t.Setenv("ADVERTISE_URL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeID != "local" || cfg.ListenAddr != ":8080" || cfg.QueueSize != 1024 {
		t.Fatalf("defaults = %+v", cfg)
	}
	if cfg.PostgresURL == "" || cfg.RedisURL == "" {
		t.Fatalf("database urls should have local defaults, got %+v", cfg)
	}
	if cfg.FlushInterval != 5*time.Second {
		t.Fatalf("flush interval = %s", cfg.FlushInterval)
	}
	if cfg.AdvertiseURL != "http://127.0.0.1:8080" {
		t.Fatalf("advertise = %s", cfg.AdvertiseURL)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("NODE_ID", "node1")
	t.Setenv("LISTEN_ADDR", ":9090")
	t.Setenv("POSTGRES_URL", "postgres://db/metermesh")
	t.Setenv("REDIS_URL", "redis://cache:6379/1")
	t.Setenv("USAGE_QUEUE_SIZE", "8")
	t.Setenv("FLUSH_INTERVAL", "2s")
	t.Setenv("ADVERTISE_URL", "http://node1:8080")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeID != "node1" || cfg.ListenAddr != ":9090" || cfg.QueueSize != 8 {
		t.Fatalf("overrides = %+v", cfg)
	}
	if cfg.PostgresURL != "postgres://db/metermesh" || cfg.RedisURL != "redis://cache:6379/1" {
		t.Fatalf("urls = %+v", cfg)
	}
	if cfg.FlushInterval != 2*time.Second {
		t.Fatalf("flush interval = %s", cfg.FlushInterval)
	}
	if cfg.AdvertiseURL != "http://node1:8080" {
		t.Fatalf("advertise = %s", cfg.AdvertiseURL)
	}
}

func TestLoadRejectsBadFlushInterval(t *testing.T) {
	t.Setenv("FLUSH_INTERVAL", "soon")
	if _, err := Load(); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadRejectsBadQueueSize(t *testing.T) {
	t.Setenv("USAGE_QUEUE_SIZE", "nope")
	if _, err := Load(); err == nil {
		t.Fatal("expected error")
	}
}
