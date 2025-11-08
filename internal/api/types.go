package api

import (
	"time"

	"xgen-crawler/internal/config"
	"xgen-crawler/internal/crawler"
)

// CreateSessionRequest captures the payload used to launch a crawler session.
type CreateSessionRequest struct {
	SeedURL         string            `json:"seed_url"`
	Depth           int               `json:"depth"`
	UserAgent       string            `json:"user_agent"`
	AllowedDomains  []string          `json:"allowed_domains"`
	RemoveAds       bool              `json:"remove_ads"`
	RemoveScripts   bool              `json:"remove_scripts"`
	RemoveStyles    bool              `json:"remove_styles"`
	TrimWhitespace  bool              `json:"trim_whitespace"`
	Headers         map[string]string `json:"headers,omitempty"`
	MaxPages        *int              `json:"max_pages,omitempty"`
	ProxyURL        string            `json:"proxy_url,omitempty"`
	RateLimit       *RateLimitRequest `json:"rate_limit,omitempty"`
	Robots          RobotsRequest     `json:"robots"`
	Media           MediaRequest      `json:"media"`
	VectorDB        *VectorDBRequest  `json:"vector_db,omitempty"`
	DiscoveryBoosts *DiscoveryRequest `json:"discovery,omitempty"`

	UserID   string `json:"-"`
	UserName string `json:"-"`
}

// RobotsRequest captures robots.txt preferences.
type RobotsRequest struct {
	Respect bool `json:"respect"`
}

// MediaRequest controls media extraction toggle.
type MediaRequest struct {
	Enabled bool `json:"enabled"`
}

// VectorDBRequest configures the downstream vector database.
type VectorDBRequest struct {
	Provider        string `json:"provider"`
	Endpoint        string `json:"endpoint"`
	APIKey          string `json:"api_key,omitempty"`
	Index           string `json:"index"`
	Namespace       string `json:"namespace"`
	Dimension       int    `json:"dimension"`
	EmbeddingModel  string `json:"embedding_model"`
	UpsertBatchSize int    `json:"upsert_batch_size"`
}

// RateLimitRequest describes optional per-domain throttling.
type RateLimitRequest struct {
	Requests int `json:"requests"`
	// WindowSeconds expresses the rate window in seconds.
	WindowSeconds int `json:"window_seconds"`
}

// DiscoveryRequest allows optional overrides for discovery behaviour.
type DiscoveryRequest struct {
	FollowExternal  *bool    `json:"follow_external,omitempty"`
	MaxLinksPerPage *int     `json:"max_links_per_page,omitempty"`
	IncludePatterns []string `json:"include_patterns,omitempty"`
	ExcludePatterns []string `json:"exclude_patterns,omitempty"`
}

// SessionStatus captures the lifecycle stage of a session.
type SessionStatus string

const (
	SessionStatusPending    SessionStatus = "pending"
	SessionStatusRunning    SessionStatus = "running"
	SessionStatusCancelling SessionStatus = "cancelling"
	SessionStatusCompleted  SessionStatus = "completed"
	SessionStatusCancelled  SessionStatus = "cancelled"
	SessionStatusFailed     SessionStatus = "failed"
)

// SessionSummary surfaces the high-level state for a crawler session.
type SessionSummary struct {
	SessionID     string        `json:"session_id"`
	RunID         string        `json:"run_id"`
	SeedURL       string        `json:"seed_url"`
	Status        SessionStatus `json:"status"`
	Processed     int64         `json:"processed_pages"`
	Pending       int64         `json:"pending_pages"`
	TotalEnqueued int64         `json:"total_enqueued"`
	LastURL       string        `json:"last_url,omitempty"`
	LastDomain    string        `json:"last_domain,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
	StartedAt     *time.Time    `json:"started_at,omitempty"`
	CompletedAt   *time.Time    `json:"completed_at,omitempty"`
	Message       string        `json:"message,omitempty"`
	Error         string        `json:"error,omitempty"`
}

// SessionDetail extends the summary with the effective configuration.
type SessionDetail struct {
	Session SessionSummary `json:"session"`
	Config  config.Config  `json:"config"`
}

// SSEEvent envelopes session state for Server-Sent Event clients.
type SSEEvent struct {
	Type      string                 `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Session   SessionSummary         `json:"session"`
	Progress  *crawler.ProgressEvent `json:"progress,omitempty"`
}

// StartIndexRequest triggers vector indexing for an existing session.
type StartIndexRequest struct {
	VectorDB *VectorDBRequest `json:"vector_db,omitempty"`
	BatchSize int            `json:"batch_size,omitempty"`
}
