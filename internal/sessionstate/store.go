package sessionstate

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"
)

// Snapshot captures the persisted session state.
type Snapshot struct {
	SessionID     string    `json:"session_id"`
	RunID         string    `json:"run_id"`
	SeedURL       string    `json:"seed_url"`
	Status        string    `json:"status"`
	Processed     int64     `json:"processed"`
	Pending       int64     `json:"pending"`
	TotalEnqueued int64     `json:"total_enqueued"`
	LastURL       string    `json:"last_url"`
	LastDomain    string    `json:"last_domain"`
	Message       string    `json:"message"`
	CreatedAt     time.Time `json:"created_at"`
	StartedAt     time.Time `json:"started_at"`
	UserID        string    `json:"user_id"`
	UserName      string    `json:"user_name"`
}

// Store persists snapshots so running sessions survive process restarts.
type Store interface {
	Save(ctx context.Context, snap Snapshot) error
	Remove(ctx context.Context, sessionID string) error
	Exists(ctx context.Context, sessionID string) (bool, error)
	Get(ctx context.Context, sessionID string) (Snapshot, bool, error)
	List(ctx context.Context) ([]Snapshot, error)
	Close() error
}

// RedisConfig configures a Redis-backed store.
type RedisConfig struct {
	Host     string
	Port     string
	DB       int
	Password string
	Key      string
	Timeout  time.Duration
}

// NewRedisStoreFromEnv initialises a Redis store using standard env vars.
func NewRedisStoreFromEnv() (Store, error) {
	host := strings.TrimSpace(os.Getenv("REDIS_HOST"))
	if host == "" {
		return nil, nil
	}
	port := strings.TrimSpace(os.Getenv("REDIS_PORT"))
	if port == "" {
		port = "6379"
	}
	db := 0
	if raw := strings.TrimSpace(os.Getenv("REDIS_DB")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		db = value
	}
	password := os.Getenv("REDIS_PASSWORD")
	return NewRedisStore(RedisConfig{
		Host:     host,
		Port:     port,
		DB:       db,
		Password: password,
	})
}
