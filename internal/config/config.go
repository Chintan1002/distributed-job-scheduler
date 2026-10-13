package config

import (
	"fmt"
	"os"
	"time"
)

// Config holds runtime settings, loaded from environment variables with
// local-dev defaults matching docker-compose.yml. No secrets live in code —
// the default credentials below are the throwaway local-dev ones from compose.
type Config struct {
	DatabaseURL  string
	APIAddr      string
	PollInterval time.Duration

	// Timing-wheel scheduler (M2).
	WheelTick      time.Duration // wheel granularity
	WheelLookahead time.Duration // how far ahead each reload looks
	WheelReload    time.Duration // how often the wheel refills from Postgres

	// Distribution (M3).
	RedisURL string
	QueueKey string // Redis list holding ready-to-run jobs
	WorkerID string // identifies this worker in logs

	// Leader election (M4).
	NodeID    string        // identifies this scheduler node in the election
	LeaderKey string        // Redis key acting as the leader lease
	LeaderTTL time.Duration // lease duration; failover happens within this window

	// Retries (M6).
	RetryBase time.Duration // first retry delay; doubles each attempt
	RetryCap  time.Duration // upper bound on the retry delay
}

func Load() Config {
	return Config{
		DatabaseURL:    getenv("DATABASE_URL", "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"),
		APIAddr:        getenv("API_ADDR", ":8080"),
		PollInterval:   getduration("POLL_INTERVAL", 500*time.Millisecond),
		WheelTick:      getduration("WHEEL_TICK", 100*time.Millisecond),
		WheelLookahead: getduration("WHEEL_LOOKAHEAD", 60*time.Second),
		WheelReload:    getduration("WHEEL_RELOAD", 2*time.Second),
		RedisURL:       getenv("REDIS_URL", "redis://localhost:6379/0"),
		QueueKey:       getenv("QUEUE_KEY", "jobs:ready"),
		WorkerID:       getenv("WORKER_ID", defaultInstanceID()),
		NodeID:         getenv("NODE_ID", defaultInstanceID()),
		LeaderKey:      getenv("LEADER_KEY", "scheduler:leader"),
		LeaderTTL:      getduration("LEADER_TTL", 10*time.Second),
		RetryBase:      getduration("RETRY_BASE", time.Second),
		RetryCap:       getduration("RETRY_CAP", time.Minute),
	}
}

func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "node"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getduration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
