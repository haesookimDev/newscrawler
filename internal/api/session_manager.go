package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"xgen-crawler/internal/config"
	"xgen-crawler/internal/crawler"
)

var (
	// ErrSessionRunning is returned when attempting to start a session that is already running.
	ErrSessionRunning = errors.New("session already running")
	// ErrMaxConcurrency signals that the global concurrency limit has been reached.
	ErrMaxConcurrency = errors.New("maximum concurrent sessions reached")
)

// SessionManager coordinates crawler engine lifecycles keyed by session identifier.
type SessionManager struct {
	mu             sync.RWMutex
	sessions       map[string]*Session
	baseConfig     config.Config
	maxConcurrency int
	running        int
	rootCtx        context.Context
}

// NewSessionManager constructs a manager with the provided defaults.
func NewSessionManager(base config.Config, maxConcurrency int, rootCtx context.Context) *SessionManager {
	if maxConcurrency <= 0 {
		maxConcurrency = 5
	}
	if rootCtx == nil {
		rootCtx = context.Background()
	}
	return &SessionManager{
		sessions:       make(map[string]*Session),
		baseConfig:     deepCopyConfig(base),
		maxConcurrency: maxConcurrency,
		rootCtx:        rootCtx,
	}
}

// StartSession validates the request, materialises a config, and launches a crawler run.
func (m *SessionManager) StartSession(req CreateSessionRequest) (*Session, error) {
	normalizedSeed, parsedSeed, err := normalizeSeedURL(req.SeedURL)
	if err != nil {
		return nil, err
	}
	sessionID := strings.ToLower(parsedSeed.Hostname())
	if sessionID == "" {
		return nil, fmt.Errorf("invalid session id derived from seed url %q", req.SeedURL)
	}

	runID := generateRunID()
	cfg, err := m.buildConfig(req, sessionID, runID, normalizedSeed, parsedSeed)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	session, exists := m.sessions[sessionID]
	if !exists {
		session = newSession(sessionID, m)
		m.sessions[sessionID] = session
	}
	if session.isActiveLocked() {
		m.mu.Unlock()
		return nil, ErrSessionRunning
	}
	if m.running >= m.maxConcurrency {
		m.mu.Unlock()
		return nil, ErrMaxConcurrency
	}
	m.running++
	m.mu.Unlock()

	if err := session.startRun(m.rootCtx, cfg, normalizedSeed, runID); err != nil {
		m.mu.Lock()
		if m.running > 0 {
			m.running--
		}
		m.mu.Unlock()
		return nil, err
	}
	return session, nil
}

// ListSessions captures current summaries for all sessions.
func (m *SessionManager) ListSessions() []SessionSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()
	summaries := make([]SessionSummary, 0, len(m.sessions))
	for _, session := range m.sessions {
		summaries = append(summaries, session.Snapshot())
	}
	return summaries
}

// GetSession returns the backing session by id.
func (m *SessionManager) GetSession(id string) (*Session, bool) {
	id = strings.ToLower(strings.TrimSpace(id))
	m.mu.RLock()
	defer m.mu.RUnlock()
	session, ok := m.sessions[id]
	return session, ok
}

// GetSessionDetail captures the latest summary + config snapshot for a session.
func (m *SessionManager) GetSessionDetail(id string) (SessionDetail, bool) {
	session, ok := m.GetSession(id)
	if !ok {
		return SessionDetail{}, false
	}
	summary := session.Snapshot()
	cfg := session.ConfigSnapshot()
	return SessionDetail{
		Session: summary,
		Config:  cfg,
	}, true
}

// CancelSession requests cancellation of the active crawler run for the session.
func (m *SessionManager) CancelSession(id string) error {
	session, ok := m.GetSession(id)
	if !ok {
		return fmt.Errorf("session %q not found", id)
	}
	if !session.Cancel("cancel requested via API") {
		return fmt.Errorf("session %q not running", id)
	}
	return nil
}

// Shutdown stops all active sessions.
func (m *SessionManager) Shutdown() {
	m.mu.RLock()
	snapshot := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		snapshot = append(snapshot, s)
	}
	m.mu.RUnlock()

	for _, session := range snapshot {
		session.Cancel("manager shutdown")
	}
}

func (m *SessionManager) buildConfig(req CreateSessionRequest, sessionID, runID, seedURL string, parsed *url.URL) (config.Config, error) {
	if req.Depth <= 0 {
		return config.Config{}, fmt.Errorf("depth must be > 0")
	}
	if strings.TrimSpace(req.UserAgent) == "" {
		return config.Config{}, fmt.Errorf("user_agent is required")
	}
	if len(req.AllowedDomains) == 0 {
		return config.Config{}, fmt.Errorf("allowed_domains must include at least one domain")
	}
	cfg := deepCopyConfig(m.baseConfig)

	cfg.Crawl.UserAgent = strings.TrimSpace(req.UserAgent)
	cfg.Crawl.MaxDepth = req.Depth
	if req.MaxPages != nil && *req.MaxPages > 0 {
		cfg.Crawl.MaxPages = *req.MaxPages
	}
	if req.ProxyURL != "" {
		cfg.Crawl.ProxyURL = strings.TrimSpace(req.ProxyURL)
	}
	if cfg.Crawl.Headers == nil {
		cfg.Crawl.Headers = make(map[string]string)
	}
	for k, v := range req.Headers {
		cfg.Crawl.Headers[k] = v
	}

	cfg.Crawl.Seeds = []config.SeedConfig{
		{
			URL:      seedURL,
			MaxDepth: req.Depth,
			Label:    sessionID,
		},
	}

	cfg.Crawl.AllowedDomains = normalizeDomainList(req.AllowedDomains)
	host := strings.ToLower(parsed.Hostname())
	if host != "" && !contains(cfg.Crawl.AllowedDomains, host) {
		cfg.Crawl.AllowedDomains = append(cfg.Crawl.AllowedDomains, host)
	}

	cfg.Preprocess.RemoveAds = req.RemoveAds
	cfg.Preprocess.RemoveScripts = req.RemoveScripts
	cfg.Preprocess.RemoveStyles = req.RemoveStyles
	cfg.Preprocess.TrimWhitespace = req.TrimWhitespace

	cfg.Robots.Respect = req.Robots.Respect
	cfg.Media.Enabled = req.Media.Enabled

	cfg.VectorDB.Provider = strings.TrimSpace(req.VectorDB.Provider)
	if cfg.VectorDB.Provider == "" {
		cfg.VectorDB.Provider = "qdrant"
	}
	cfg.VectorDB.Endpoint = strings.TrimSpace(req.VectorDB.Endpoint)
	cfg.VectorDB.APIKey = strings.TrimSpace(req.VectorDB.APIKey)
	cfg.VectorDB.Index = strings.TrimSpace(req.VectorDB.Index)
	cfg.VectorDB.Namespace = strings.TrimSpace(req.VectorDB.Namespace)
	cfg.VectorDB.Dimension = req.VectorDB.Dimension
	cfg.VectorDB.EmbeddingModel = strings.TrimSpace(req.VectorDB.EmbeddingModel)
	if req.VectorDB.UpsertBatchSize > 0 {
		cfg.VectorDB.UpsertBatchSize = req.VectorDB.UpsertBatchSize
	}

	if req.RateLimit != nil {
		cfg.Crawl.RateLimitPerDomain.Requests = req.RateLimit.Requests
		if req.RateLimit.WindowSeconds > 0 {
			cfg.Crawl.RateLimitPerDomain.Window = config.DurationFrom(time.Duration(req.RateLimit.WindowSeconds) * time.Second)
		}
	}

	if req.DiscoveryBoosts != nil {
		if req.DiscoveryBoosts.FollowExternal != nil {
			cfg.Crawl.Discovery.FollowExternal = *req.DiscoveryBoosts.FollowExternal
		}
		if req.DiscoveryBoosts.MaxLinksPerPage != nil && *req.DiscoveryBoosts.MaxLinksPerPage > 0 {
			cfg.Crawl.Discovery.MaxLinksPerPage = *req.DiscoveryBoosts.MaxLinksPerPage
		}
		if len(req.DiscoveryBoosts.IncludePatterns) > 0 {
			cfg.Crawl.Discovery.IncludePatterns = append([]string(nil), req.DiscoveryBoosts.IncludePatterns...)
		}
		if len(req.DiscoveryBoosts.ExcludePatterns) > 0 {
			cfg.Crawl.Discovery.ExcludePatterns = append([]string(nil), req.DiscoveryBoosts.ExcludePatterns...)
		}
	}

	cfg.Job.ScraperID = sessionID
	cfg.Job.RunID = runID
	if cfg.Job.Metadata == nil {
		cfg.Job.Metadata = make(map[string]string)
	}
	cfg.Job.Metadata["seed_url"] = seedURL
	cfg.Job.Metadata["session_id"] = sessionID

	if err := cfg.Validate(); err != nil {
		return config.Config{}, err
	}
	return cfg, nil
}

func (m *SessionManager) notifyCompletion() {
	m.mu.Lock()
	if m.running > 0 {
		m.running--
	}
	m.mu.Unlock()
}

// Session tracks the lifecycle and state of a crawler engine instance.
type Session struct {
	id string

	mu           sync.Mutex
	seedURL      string
	status       SessionStatus
	runID        string
	createdAt    time.Time
	startedAt    *time.Time
	completedAt  *time.Time
	processed    int64
	pending      int64
	totalEnqueue int64
	lastURL      string
	lastDomain   string
	message      string
	lastError    string
	config       config.Config

	cancel context.CancelFunc

	subscribers map[chan SSEEvent]struct{}
	subMu       sync.RWMutex

	manager *SessionManager
}

func newSession(id string, manager *SessionManager) *Session {
	return &Session{
		id:          id,
		status:      SessionStatusPending,
		createdAt:   time.Now(),
		subscribers: make(map[chan SSEEvent]struct{}),
		manager:     manager,
		config:      deepCopyConfig(manager.baseConfig),
	}
}

func (s *Session) isActiveLocked() bool {
	return s.status == SessionStatusRunning || s.status == SessionStatusCancelling
}

func (s *Session) startRun(parentCtx context.Context, cfg config.Config, seedURL, runID string) error {
	engine, err := crawler.NewEngine(cfg, crawler.WithSessionID(s.id), crawler.WithProgressSink(s))
	if err != nil {
		return err
	}

	ctx := parentCtx
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)

	started := time.Now()

	s.mu.Lock()
	s.seedURL = seedURL
	s.runID = runID
	s.status = SessionStatusRunning
	s.startedAt = &started
	s.completedAt = nil
	s.processed = 0
	s.pending = 0
	s.totalEnqueue = 0
	s.lastURL = ""
	s.lastDomain = ""
	s.message = "running"
	s.lastError = ""
	s.config = cfg
	s.cancel = cancel
	s.mu.Unlock()

	s.broadcast("session_started", nil)

	go func() {
		err := engine.Run(runCtx)
		s.handleCompletion(err)
	}()
	return nil
}

// Report satisfies crawler.ProgressSink.
func (s *Session) Report(evt crawler.ProgressEvent) {
	s.mu.Lock()
	s.processed = evt.ProcessedPages
	s.pending = evt.PendingPages
	s.totalEnqueue = evt.TotalEnqueued
	if evt.URL != "" {
		s.lastURL = evt.URL
	}
	if evt.Domain != "" {
		s.lastDomain = evt.Domain
	}
	if s.status == SessionStatusRunning {
		s.message = "running"
	}
	s.mu.Unlock()

	copyEvt := evt
	s.broadcast("progress", &copyEvt)
}

func (s *Session) handleCompletion(err error) {
	now := time.Now()
	s.mu.Lock()
	status := SessionStatusCompleted
	message := "completed"
	errorText := ""
	switch {
	case errors.Is(err, context.Canceled):
		status = SessionStatusCancelled
		message = "cancelled"
	case err != nil:
		status = SessionStatusFailed
		message = "failed"
		errorText = err.Error()
	}
	s.status = status
	s.completedAt = &now
	s.message = message
	s.lastError = errorText
	s.cancel = nil
	s.mu.Unlock()

	eventType := "session_completed"
	switch status {
	case SessionStatusCancelled:
		eventType = "session_cancelled"
	case SessionStatusFailed:
		eventType = "session_failed"
	}
	s.broadcast(eventType, nil)
	s.manager.notifyCompletion()
}

// Cancel attempts to stop the running engine.
func (s *Session) Cancel(reason string) bool {
	s.mu.Lock()
	if s.status != SessionStatusRunning || s.cancel == nil {
		s.mu.Unlock()
		return false
	}
	s.status = SessionStatusCancelling
	s.message = reason
	cancel := s.cancel
	s.mu.Unlock()
	s.broadcast("session_cancelling", nil)
	cancel()
	return true
}

// Snapshot returns a copy of the public session state.
func (s *Session) Snapshot() SessionSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

// ConfigSnapshot returns a defensive copy of the session config.
func (s *Session) ConfigSnapshot() config.Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return deepCopyConfig(s.config)
}

func (s *Session) snapshotLocked() SessionSummary {
	summary := SessionSummary{
		SessionID:     s.id,
		RunID:         s.runID,
		SeedURL:       s.seedURL,
		Status:        s.status,
		Processed:     s.processed,
		Pending:       s.pending,
		TotalEnqueued: s.totalEnqueue,
		LastURL:       s.lastURL,
		LastDomain:    s.lastDomain,
		CreatedAt:     s.createdAt,
		Message:       s.message,
		Error:         s.lastError,
	}
	if s.startedAt != nil {
		started := *s.startedAt
		summary.StartedAt = &started
	}
	if s.completedAt != nil {
		completed := *s.completedAt
		summary.CompletedAt = &completed
	}
	return summary
}

// Subscribe registers an SSE subscriber for the session.
func (s *Session) Subscribe() (<-chan SSEEvent, func()) {
	ch := make(chan SSEEvent, 16)

	s.subMu.Lock()
	s.subscribers[ch] = struct{}{}
	s.subMu.Unlock()

	initial := SSEEvent{
		Type:      "snapshot",
		Timestamp: time.Now(),
		Session:   s.Snapshot(),
	}
	select {
	case ch <- initial:
	default:
	}

	cancel := func() {
		s.subMu.Lock()
		if _, ok := s.subscribers[ch]; ok {
			delete(s.subscribers, ch)
			close(ch)
		}
		s.subMu.Unlock()
	}
	return ch, cancel
}

func (s *Session) broadcast(eventType string, progress *crawler.ProgressEvent) {
	snapshot := s.Snapshot()
	envelope := SSEEvent{
		Type:      eventType,
		Timestamp: time.Now(),
		Session:   snapshot,
	}
	if progress != nil {
		copyProgress := *progress
		envelope.Progress = &copyProgress
	}

	s.subMu.RLock()
	defer s.subMu.RUnlock()
	for ch := range s.subscribers {
		select {
		case ch <- envelope:
		default:
		}
	}
}

func deepCopyConfig(base config.Config) config.Config {
	cfg := base

	cfg.Crawl.Seeds = append([]config.SeedConfig(nil), base.Crawl.Seeds...)
	cfg.Crawl.AllowedDomains = append([]string(nil), base.Crawl.AllowedDomains...)
	cfg.Crawl.ExcludedDomains = append([]string(nil), base.Crawl.ExcludedDomains...)
	cfg.Crawl.AllowedContentTypes = append([]string(nil), base.Crawl.AllowedContentTypes...)
	cfg.Crawl.Discovery.IncludePatterns = append([]string(nil), base.Crawl.Discovery.IncludePatterns...)
	cfg.Crawl.Discovery.ExcludePatterns = append([]string(nil), base.Crawl.Discovery.ExcludePatterns...)

	if base.Crawl.Headers != nil {
		cfg.Crawl.Headers = make(map[string]string, len(base.Crawl.Headers))
		for k, v := range base.Crawl.Headers {
			cfg.Crawl.Headers[k] = v
		}
	} else {
		cfg.Crawl.Headers = nil
	}

	cfg.Preprocess.AdSelectors = append([]string(nil), base.Preprocess.AdSelectors...)
	cfg.Preprocess.ExtraDropClasses = append([]string(nil), base.Preprocess.ExtraDropClasses...)
	cfg.Robots.Overrides = append([]string(nil), base.Robots.Overrides...)
	cfg.Media.AllowedContentTypes = append([]string(nil), base.Media.AllowedContentTypes...)

	if base.Job.Metadata != nil {
		cfg.Job.Metadata = make(map[string]string, len(base.Job.Metadata))
		for k, v := range base.Job.Metadata {
			cfg.Job.Metadata[k] = v
		}
	} else {
		cfg.Job.Metadata = make(map[string]string)
	}

	return cfg
}

func normalizeSeedURL(seed string) (string, *url.URL, error) {
	if strings.TrimSpace(seed) == "" {
		return "", nil, fmt.Errorf("seed_url is required")
	}
	parsed, err := url.Parse(strings.TrimSpace(seed))
	if err != nil {
		return "", nil, fmt.Errorf("parse seed_url: %w", err)
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "https"
	}
	if parsed.Host == "" {
		return "", nil, fmt.Errorf("seed_url %q missing host", seed)
	}
	return parsed.String(), parsed, nil
}

func normalizeDomainList(domains []string) []string {
	out := make([]string, 0, len(domains))
	seen := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		host := strings.ToLower(strings.TrimSpace(domain))
		if host == "" {
			continue
		}
		if _, exists := seen[host]; exists {
			continue
		}
		seen[host] = struct{}{}
		out = append(out, host)
	}
	return out
}

func contains(list []string, target string) bool {
	for _, v := range list {
		if strings.EqualFold(v, target) {
			return true
		}
	}
	return false
}

func generateRunID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
