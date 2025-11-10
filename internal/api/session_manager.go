package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"xgen-crawler/internal/config"
	"xgen-crawler/internal/crawler"
	"xgen-crawler/internal/sessionstate"
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
	logger         *slog.Logger
	stateStore     sessionstate.Store
}

// NewSessionManager constructs a manager with the provided defaults.
func NewSessionManager(base config.Config, maxConcurrency int, rootCtx context.Context, logger *slog.Logger, store sessionstate.Store) *SessionManager {
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
		logger:         logger,
		stateStore:     store,
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

	if m.stateStore != nil {
		exists, err := m.stateStore.Exists(m.rootCtx, sessionID)
		if err != nil && m.logger != nil {
			m.logger.Warn("redis exists check failed", "session_id", sessionID, "error", err)
		}
		if err == nil && exists {
			return nil, ErrSessionRunning
		}
	}

	runID := generateRunID()
	cfg, err := m.buildConfig(req, sessionID, runID, normalizedSeed, parsedSeed)
	if err != nil {
		return nil, err
	}

	if m.logger != nil {
		m.logger.Info("session start requested",
			"session_id", sessionID,
			"seed_url", normalizedSeed,
			"user_id", req.UserID,
			"user_name", req.UserName,
			"run_id", runID)
	}

	m.mu.Lock()
	session, exists := m.sessions[sessionID]
	if !exists {
		session = newSession(sessionID, m, req.UserID, req.UserName)
		m.sessions[sessionID] = session
	}
	if session.isActiveLocked() {
		if m.logger != nil {
			m.logger.Warn("session already running",
				"session_id", sessionID,
				"run_id", session.runID)
		}
		m.mu.Unlock()
		return nil, ErrSessionRunning
	}
	if m.running >= m.maxConcurrency {
		if m.logger != nil {
			m.logger.Warn("concurrency limit reached",
				"limit", m.maxConcurrency,
				"session_id", sessionID)
		}
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
		if m.logger != nil {
			m.logger.Error("start run failed",
				"session_id", sessionID,
				"run_id", runID,
				"error", err)
		}
		return nil, err
	}
	if m.logger != nil {
		m.logger.Info("session run started",
			"session_id", sessionID,
			"run_id", runID,
			"running", m.currentRunning())
	}
	m.persistSnapshot(session)
	return session, nil
}

// ListSessions captures current summaries for all sessions.
func (m *SessionManager) ListSessions() []SessionSummary {
	m.mu.RLock()
	summaries := make([]SessionSummary, 0, len(m.sessions))
	existing := make(map[string]struct{}, len(m.sessions))
	for id, session := range m.sessions {
		summaries = append(summaries, session.Snapshot())
		existing[id] = struct{}{}
	}
	m.mu.RUnlock()

	if m.stateStore != nil {
		snaps, err := m.stateStore.List(m.rootCtx)
		if err != nil && m.logger != nil {
			m.logger.Warn("list redis sessions failed", "error", err)
		}
		for _, snap := range snaps {
			if _, ok := existing[snap.SessionID]; ok {
				continue
			}
			summaries = append(summaries, snapshotToSummary(snap))
		}
	}
	return summaries
}

// EvictInactiveSessions persists and removes old, unused sessions to keep memory bounded.
func (m *SessionManager) EvictInactiveSessions(maxInMemory int, idleAfter time.Duration) int {
	if m == nil || maxInMemory <= 0 || m.stateStore == nil {
		return 0
	}

	type candidate struct {
		session    *Session
		lastActive time.Time
	}

	now := time.Now()

	m.mu.Lock()
	current := len(m.sessions)
	if current <= maxInMemory {
		m.mu.Unlock()
		return 0
	}
	candidates := make([]candidate, 0, current-maxInMemory)
	for _, session := range m.sessions {
		summary := session.Snapshot()
		if summary.Status == SessionStatusRunning || summary.Status == SessionStatusCancelling {
			continue
		}
		lastActive := summary.CreatedAt
		if summary.CompletedAt != nil {
			lastActive = *summary.CompletedAt
		} else if summary.StartedAt != nil {
			lastActive = *summary.StartedAt
		}
		if idleAfter > 0 && now.Sub(lastActive) < idleAfter {
			continue
		}
		candidates = append(candidates, candidate{
			session:    session,
			lastActive: lastActive,
		})
	}
	if len(candidates) == 0 {
		m.mu.Unlock()
		return 0
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastActive.Before(candidates[j].lastActive)
	})

	target := current - maxInMemory
	if target < len(candidates) {
		candidates = candidates[:target]
	}

	for _, c := range candidates {
		delete(m.sessions, c.session.id)
	}
	m.mu.Unlock()

	for _, c := range candidates {
		m.persistSnapshot(c.session)
	}

	if len(candidates) > 0 && m.logger != nil {
		m.logger.Info("evicted inactive sessions",
			"count", len(candidates),
			"max_resident", maxInMemory)
	}
	return len(candidates)
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
		if m.stateStore != nil {
			snap, found, err := m.stateStore.Get(m.rootCtx, id)
			if err != nil && m.logger != nil {
				m.logger.Warn("redis get session failed", "session_id", id, "error", err)
			}
			if err == nil && found {
				return SessionDetail{
					Session: snapshotToSummary(snap),
					Config:  deepCopyConfig(m.baseConfig),
				}, true
			}
		}
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
		if m.logger != nil {
			m.logger.Warn("cancel rejected",
				"session_id", id)
		}
		return fmt.Errorf("session %q not running", id)
	}
	if m.logger != nil {
		m.logger.Info("cancel requested",
			"session_id", id)
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

	if req.VectorDB != nil {
		cfg.VectorDB.Provider = strings.TrimSpace(req.VectorDB.Provider)
		cfg.VectorDB.Endpoint = strings.TrimSpace(req.VectorDB.Endpoint)
		cfg.VectorDB.APIKey = strings.TrimSpace(req.VectorDB.APIKey)
		cfg.VectorDB.Index = strings.TrimSpace(req.VectorDB.Index)
		cfg.VectorDB.Namespace = strings.TrimSpace(req.VectorDB.Namespace)
		cfg.VectorDB.Dimension = req.VectorDB.Dimension
		cfg.VectorDB.EmbeddingModel = strings.TrimSpace(req.VectorDB.EmbeddingModel)
		if req.VectorDB.UpsertBatchSize > 0 {
			cfg.VectorDB.UpsertBatchSize = req.VectorDB.UpsertBatchSize
		}
	} else {
		cfg.VectorDB = config.VectorDBConfig{}
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
	if req.UserID != "" {
		cfg.Job.Metadata["x_user_id"] = req.UserID
	}
	if req.UserName != "" {
		cfg.Job.Metadata["x_user_name"] = req.UserName
	}

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
	if m.logger != nil {
		m.logger.Info("session completed", "running", m.currentRunning())
	}
}

func (m *SessionManager) currentRunning() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

func (m *SessionManager) persistSnapshot(session *Session) {
	if m.stateStore == nil || session == nil {
		return
	}
	snap := session.toSnapshot()
	if err := m.stateStore.Save(m.rootCtx, snap); err != nil && m.logger != nil {
		m.logger.Warn("persist session snapshot failed", "session_id", session.id, "error", err)
	}
}

func (m *SessionManager) removeSnapshot(sessionID string) {
	if m.stateStore == nil || sessionID == "" {
		return
	}
	if err := m.stateStore.Remove(m.rootCtx, sessionID); err != nil && m.logger != nil {
		m.logger.Warn("remove session snapshot failed", "session_id", sessionID, "error", err)
	}
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

	manager  *SessionManager
	userID   string
	userName string
}

func newSession(id string, manager *SessionManager, userID, userName string) *Session {
	return &Session{
		id:          id,
		status:      SessionStatusPending,
		createdAt:   time.Now(),
		subscribers: make(map[chan SSEEvent]struct{}),
		manager:     manager,
		config:      deepCopyConfig(manager.baseConfig),
		userID:      strings.TrimSpace(userID),
		userName:    strings.TrimSpace(userName),
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
	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Info("session started",
			"session_id", s.id,
			"run_id", runID,
			"user_id", s.userID,
			"user_name", s.userName)
	}
	if s.manager != nil {
		s.manager.persistSnapshot(s)
	}

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

	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Debug("page processed",
			"session_id", evt.SessionID,
			"run_id", evt.RunID,
			"processed", evt.ProcessedPages,
			"pending", evt.PendingPages,
			"url", evt.URL)
	}
	if s.manager != nil {
		s.manager.persistSnapshot(s)
	}
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
	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Info("session finished",
			"session_id", s.id,
			"run_id", s.runID,
			"status", status,
			"error", errorText)
	}
	s.manager.notifyCompletion()
	if s.manager != nil {
		s.manager.removeSnapshot(s.id)
	}
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
	if s.manager != nil {
		s.manager.persistSnapshot(s)
	}
	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Info("session cancelling",
			"session_id", s.id,
			"run_id", s.runID,
			"reason", reason)
	}
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

func (s *Session) toSnapshot() sessionstate.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := sessionstate.Snapshot{
		SessionID:     s.id,
		RunID:         s.runID,
		SeedURL:       s.seedURL,
		Status:        string(s.status),
		Processed:     s.processed,
		Pending:       s.pending,
		TotalEnqueued: s.totalEnqueue,
		LastURL:       s.lastURL,
		LastDomain:    s.lastDomain,
		Message:       s.message,
		CreatedAt:     s.createdAt,
		UserID:        s.userID,
		UserName:      s.userName,
	}
	if s.startedAt != nil {
		snap.StartedAt = *s.startedAt
	}
	return snap
}

func snapshotToSummary(snap sessionstate.Snapshot) SessionSummary {
	summary := SessionSummary{
		SessionID:     snap.SessionID,
		RunID:         snap.RunID,
		SeedURL:       snap.SeedURL,
		Status:        SessionStatus(snap.Status),
		Processed:     snap.Processed,
		Pending:       snap.Pending,
		TotalEnqueued: snap.TotalEnqueued,
		LastURL:       snap.LastURL,
		LastDomain:    snap.LastDomain,
		CreatedAt:     snap.CreatedAt,
		Message:       snap.Message,
	}
	if !snap.StartedAt.IsZero() {
		started := snap.StartedAt
		summary.StartedAt = &started
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
