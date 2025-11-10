package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"xgen-crawler/internal/config"
	"xgen-crawler/internal/storage"
)

const (
	maxResidentSessions          = 128
	sessionEvictionInterval      = time.Minute
	sessionEvictionIdleWindow    = 15 * time.Minute
	defaultDocumentSyncBatchSize = 64
	crawlerDocumentType          = "crawler_page"
)

// Server exposes the HTTP API for managing crawler sessions.
type PageStore interface {
	ListPages(ctx context.Context, sessionID string, params storage.PageListParams) (storage.PageListResult, error)
	ListAllPages(ctx context.Context, params storage.PageListParams, sessionID string) (storage.GlobalPageListResult, error)
	ListSessionPageOverviews(ctx context.Context, params storage.PageListParams) (storage.SessionPageOverviewResult, error)
	GetPageByURL(ctx context.Context, sessionID, url string) (storage.PageDetail, error)
	CountPagesNeedingIndex(ctx context.Context, sessionID string) (int64, error)
	FetchPagesNeedingIndex(ctx context.Context, sessionID string, limit int) ([]storage.IndexCandidate, error)
	MarkPageIndexed(ctx context.Context, sessionID, url string) error
	FetchPagesReadyForDocumentSync(ctx context.Context, sessionID string, limit int) ([]storage.DocumentSyncCandidate, error)
	MarkPageDocumentIntegrated(ctx context.Context, sessionID, url string) error
	DeleteSessionData(ctx context.Context, sessionID string) error
}

type Server struct {
	manager   *SessionManager
	pageStore PageStore
	docStore  storage.DocumentSyncStore
	mux       *http.ServeMux
	logger    *slog.Logger
	indexMu   sync.Mutex
	indexJobs map[string]*indexJob
}

const defaultIndexBatchSize = 32

var embeddingHTTPClient = &http.Client{
	Timeout: 15 * time.Second,
}

// NewServer wires handlers onto an HTTP mux.
func NewServer(manager *SessionManager, store PageStore, docStore storage.DocumentSyncStore, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		manager:   manager,
		pageStore: store,
		docStore:  docStore,
		mux:       http.NewServeMux(),
		logger:    logger,
		indexJobs: make(map[string]*indexJob),
	}
	s.routes()
	s.startSessionEvictionLoop()
	return s
}

func (s *Server) startSessionEvictionLoop() {
	if s == nil || s.manager == nil || s.manager.stateStore == nil || maxResidentSessions <= 0 {
		return
	}
	ctx := s.manager.rootCtx
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(sessionEvictionInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				evicted := s.manager.EvictInactiveSessions(maxResidentSessions, sessionEvictionIdleWindow)
				if evicted > 0 && s.logger != nil {
					s.logger.Info("evicted stale sessions",
						"count", evicted,
						"max_resident", maxResidentSessions)
				}
			}
		}
	}()
}

// ServeHTTP satisfies the http.Handler interface.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setCORSHeaders(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/api/crawler/sessions", s.handleSessions)
	s.mux.HandleFunc("/api/crawler/sessions/", s.handleSessionByID)
	s.mux.HandleFunc("/api/crawler/pages", s.handleAllPages)
	s.mux.HandleFunc("/api/crawler/page-sessions", s.handlePageSessions)
	s.mux.HandleFunc("/openapi.yaml", s.handleOpenAPI)
	s.mux.HandleFunc("/docs", s.handleDocs)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, r, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"timestamp": time.Now().UTC(),
	})
}

func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	if s.logger != nil {
		s.logger.Debug("http request", "method", r.Method, "path", r.URL.Path)
	}
	switch r.Method {
	case http.MethodGet:
		s.listSessions(w, r)
	case http.MethodPost:
		s.createSession(w, r)
	default:
		methodNotAllowed(w, r, http.MethodGet, http.MethodPost)
	}
}

func (s *Server) handleSessionByID(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/api/crawler/sessions/")
	if trimmed == "" {
		http.NotFound(w, r)
		return
	}
	trimmed = strings.Trim(trimmed, "/")
	if trimmed == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(trimmed, "/")
	sessionID, err := urlPathDecode(parts[0])
	if err != nil {
		http.Error(w, "invalid session id", http.StatusBadRequest)
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			s.getSession(w, r, sessionID)
		case http.MethodDelete:
			s.deleteSessionData(w, r, sessionID)
		default:
			methodNotAllowed(w, r, http.MethodGet, http.MethodDelete)
		}
		return
	}

	switch parts[1] {
	case "events":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, r, http.MethodGet)
			return
		}
		s.streamSessionEvents(w, r, sessionID)
	case "cancel":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, r, http.MethodPost)
			return
		}
		s.cancelSession(w, r, sessionID)
	case "pages":
		if s.pageStore == nil {
			http.Error(w, "page store not configured", http.StatusNotImplemented)
			return
		}
		if len(parts) == 2 {
			if r.Method != http.MethodGet {
				methodNotAllowed(w, r, http.MethodGet)
				return
			}
			s.listSessionPages(w, r, sessionID)
			return
		}
		if r.Method != http.MethodGet {
			methodNotAllowed(w, r, http.MethodGet)
			return
		}
		s.getPageDetail(w, r, sessionID, parts[2])
	case "index":
		if len(parts) == 2 {
			switch r.Method {
			case http.MethodPost:
				s.startIndexJob(w, r, sessionID)
			case http.MethodGet:
				s.getIndexJobStatus(w, r, sessionID)
			default:
				methodNotAllowed(w, r, http.MethodGet, http.MethodPost)
			}
			return
		}
		if len(parts) == 3 && parts[2] == "events" {
			if r.Method != http.MethodGet {
				methodNotAllowed(w, r, http.MethodGet)
				return
			}
			s.streamIndexEvents(w, r, sessionID)
			return
		}
		http.NotFound(w, r)
	case "documents":
		if len(parts) != 2 {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			methodNotAllowed(w, r, http.MethodPost)
			return
		}
		s.syncSessionDocuments(w, r, sessionID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var req CreateSessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid json payload: %v", err), http.StatusBadRequest)
		return
	}
	req.UserID = strings.TrimSpace(r.Header.Get("X-User-ID"))
	req.UserName = strings.TrimSpace(r.Header.Get("X-User-Name"))
	if req.UserID == "" || req.UserName == "" {
		if s.logger != nil {
			s.logger.Warn("missing user headers",
				"path", r.URL.Path,
				"method", r.Method)
		}
		http.Error(w, "missing X-User-ID or X-User-Name header", http.StatusBadRequest)
		return
	}
	if s.logger != nil {
		s.logger.Info("create session",
			"session_host", req.SeedURL,
			"user_id", req.UserID,
			"user_name", req.UserName)
	}
	session, err := s.manager.StartSession(req)
	if err != nil {
		switch {
		case errors.Is(err, ErrSessionRunning):
			http.Error(w, err.Error(), http.StatusConflict)
		case errors.Is(err, ErrMaxConcurrency):
			http.Error(w, err.Error(), http.StatusTooManyRequests)
		default:
			if s.logger != nil {
				s.logger.Error("create session failed", "error", err)
			}
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
	if s.logger != nil {
		s.logger.Info("session created",
			"session_id", session.id,
			"run_id", session.runID)
	}
	writeJSON(w, http.StatusCreated, session.Snapshot())
}

func (s *Server) listSessions(w http.ResponseWriter, r *http.Request) {
	if s.logger != nil {
		s.logger.Debug("list sessions")
	}
	summaries := s.manager.ListSessions()
	writeJSON(w, http.StatusOK, summaries)
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request, id string) {
	if s.logger != nil {
		s.logger.Debug("get session", "session_id", id)
	}
	detail, ok := s.manager.GetSessionDetail(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) cancelSession(w http.ResponseWriter, r *http.Request, id string) {
	if s.logger != nil {
		s.logger.Info("cancel session request", "session_id", id)
	}
	if err := s.manager.CancelSession(id); err != nil {
		if s.logger != nil {
			s.logger.Warn("cancel session failed", "session_id", id, "error", err)
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.abortIndexJob(id, "session cancelled")
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) deleteSessionData(w http.ResponseWriter, r *http.Request, id string) {
	if s.pageStore == nil {
		http.Error(w, "page store not configured", http.StatusNotImplemented)
		return
	}
	if s.logger != nil {
		s.logger.Info("delete session data request", "session_id", id)
	}
	if s.manager != nil {
		if session, ok := s.manager.GetSession(id); ok {
			summary := session.Snapshot()
			if summary.Status == SessionStatusRunning || summary.Status == SessionStatusCancelling {
				http.Error(w, "session is running; cancel before deleting data", http.StatusConflict)
				return
			}
		}
	}
	s.abortIndexJob(id, "session deleted")
	ctx := r.Context()
	if err := s.pageStore.DeleteSessionData(ctx, id); err != nil {
		if s.logger != nil {
			s.logger.Error("delete session data failed", "session_id", id, "error", err)
		}
		http.Error(w, "failed to delete session data", http.StatusInternalServerError)
		return
	}
	if err := s.deleteVectorData(ctx, id); err != nil {
		if s.logger != nil {
			s.logger.Error("delete vector data failed", "session_id", id, "error", err)
		}
		http.Error(w, "failed to delete vector data", http.StatusBadGateway)
		return
	}
	if s.manager != nil {
		s.manager.removeSnapshot(id)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) streamSessionEvents(w http.ResponseWriter, r *http.Request, id string) {
	session, ok := s.manager.GetSession(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if s.logger != nil {
		s.logger.Info("stream events", "session_id", id)
	}
	eventCh, cancel := session.Subscribe()
	defer cancel()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case evt, open := <-eventCh:
			if !open {
				return
			}
			payload, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\n", evt.Type)
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprint(w, "event: heartbeat\ndata: {}\n\n")
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) listSessionPages(w http.ResponseWriter, r *http.Request, sessionID string) {
	if s.logger != nil {
		s.logger.Debug("list session pages", "session_id", sessionID)
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	search := r.URL.Query().Get("search")

	params := storage.PageListParams{
		Page:     page,
		PageSize: pageSize,
		Search:   search,
	}
	res, err := s.pageStore.ListPages(r.Context(), sessionID, params)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("list pages failed", "session_id", sessionID, "error", err)
		}
		http.Error(w, "failed to list pages", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) getPageDetail(w http.ResponseWriter, r *http.Request, sessionID, pageID string) {
	url, err := storage.DecodePageID(pageID)
	if err != nil {
		http.Error(w, "invalid page id", http.StatusBadRequest)
		return
	}
	detail, err := s.pageStore.GetPageByURL(r.Context(), sessionID, url)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		if s.logger != nil {
			s.logger.Error("page detail failed", "session_id", sessionID, "page_id", pageID, "error", err)
		}
		http.Error(w, "failed to load page", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) startIndexJob(w http.ResponseWriter, r *http.Request, sessionID string) {
	if s.pageStore == nil {
		http.Error(w, "page store not configured", http.StatusNotImplemented)
		return
	}
	userID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	userName := strings.TrimSpace(r.Header.Get("X-User-Name"))
	if userID == "" || userName == "" {
		http.Error(w, "missing X-User-ID or X-User-Name header", http.StatusBadRequest)
		return
	}

	var req StartIndexRequest
	if r.Body != nil {
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, fmt.Sprintf("invalid json payload: %v", err), http.StatusBadRequest)
			return
		}
	}

	vectorCfg, ok := s.resolveVectorConfig(sessionID, req.VectorDB)
	if !ok {
		http.Error(w, "vector_db configuration missing; provide overrides or configure defaults for this session", http.StatusPreconditionFailed)
		return
	}

	if err := s.populateEmbeddingMetadata(r.Context(), &vectorCfg, userID, userName); err != nil {
		if s.logger != nil {
			s.logger.Error("embedding metadata fetch failed",
				"session_id", sessionID,
				"error", err)
		}
		http.Error(w, "failed to load embedding configuration", http.StatusBadGateway)
		return
	}
	if vectorCfg.Dimension <= 0 {
		http.Error(w, "vector_db configuration missing embedding dimension", http.StatusPreconditionFailed)
		return
	}

	if !vectorConfigComplete(vectorCfg) {
		http.Error(w, "vector_db configuration incomplete; provider and endpoint are required", http.StatusPreconditionFailed)
		return
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = vectorCfg.UpsertBatchSize
	}
	if batchSize <= 0 {
		batchSize = defaultIndexBatchSize
	}

	total, err := s.pageStore.CountPagesNeedingIndex(r.Context(), sessionID)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("count needs_index failed", "session_id", sessionID, "error", err)
		}
		http.Error(w, "failed to count pending pages", http.StatusInternalServerError)
		return
	}
	if total == 0 {
		writeJSON(w, http.StatusOK, idleIndexEvent(sessionID, total))
		return
	}

	s.indexMu.Lock()
	if existing, ok := s.indexJobs[sessionID]; ok {
		if !existing.isFinished() {
			s.indexMu.Unlock()
			http.Error(w, "index job already running for this session", http.StatusConflict)
			return
		}
	}
	parentCtx := context.Background()
	if s.manager != nil && s.manager.rootCtx != nil {
		parentCtx = s.manager.rootCtx
	}
	job := newIndexJob(parentCtx, sessionID, total)
	s.indexJobs[sessionID] = job
	s.indexMu.Unlock()

	if s.logger != nil {
		s.logger.Info("index job started",
			"session_id", sessionID,
			"total_pending", total,
			"batch_size", batchSize)
	}

	go func() {
		defer s.clearIndexJob(sessionID, job)
		s.runIndexJob(job, vectorCfg, batchSize, userID, userName)
	}()

	writeJSON(w, http.StatusAccepted, job.snapshot("index_started"))
}

func (s *Server) getIndexJobStatus(w http.ResponseWriter, r *http.Request, sessionID string) {
	if s.pageStore == nil {
		http.Error(w, "page store not configured", http.StatusNotImplemented)
		return
	}
	s.indexMu.Lock()
	job, ok := s.indexJobs[sessionID]
	s.indexMu.Unlock()
	if ok {
		writeJSON(w, http.StatusOK, job.snapshot("index_status"))
		return
	}
	total, err := s.pageStore.CountPagesNeedingIndex(r.Context(), sessionID)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("count needs_index failed", "session_id", sessionID, "error", err)
		}
		http.Error(w, "failed to count pending pages", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, idleIndexEvent(sessionID, total))
}

func (s *Server) streamIndexEvents(w http.ResponseWriter, r *http.Request, sessionID string) {
	job, ok := s.getIndexJob(sessionID)
	if !ok {
		http.NotFound(w, r)
		return
	}

	events, cancel := job.subscribe()
	defer cancel()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case evt, open := <-events:
			if !open {
				return
			}
			if err := writeIndexEvent(w, flusher, evt); err != nil {
				return
			}
		case <-heartbeat.C:
			fmt.Fprint(w, "event: heartbeat\ndata: {}\n\n")
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) syncSessionDocuments(w http.ResponseWriter, r *http.Request, sessionID string) {
	if s.pageStore == nil {
		http.Error(w, "page store not configured", http.StatusNotImplemented)
		return
	}
	if s.docStore == nil {
		http.Error(w, "document sync not supported", http.StatusNotImplemented)
		return
	}
	userID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	userName := strings.TrimSpace(r.Header.Get("X-User-Name"))
	if userID == "" || userName == "" {
		http.Error(w, "missing X-User-ID or X-User-Name header", http.StatusBadRequest)
		return
	}
	var req DocumentSyncRequest
	if r.Body != nil {
		defer r.Body.Close()
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, fmt.Sprintf("invalid json payload: %v", err), http.StatusBadRequest)
			return
		}
	}
	vectorCfg, ok := s.resolveVectorConfig(sessionID, req.VectorDB)
	if !ok {
		http.Error(w, "vector_db configuration missing; provide overrides or configure defaults for this session", http.StatusPreconditionFailed)
		return
	}
	if err := s.populateEmbeddingMetadata(r.Context(), &vectorCfg, userID, userName); err != nil {
		if s.logger != nil {
			s.logger.Error("embedding metadata fetch failed",
				"session_id", sessionID,
				"error", err)
		}
		http.Error(w, "failed to load embedding configuration", http.StatusBadGateway)
		return
	}
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = vectorCfg.UpsertBatchSize
	}
	if batchSize <= 0 {
		batchSize = defaultDocumentSyncBatchSize
	}
	collectionMakeName, collectionName := deriveCollectionIdentifiers(sessionID, vectorCfg)
	userIDPtr := parseUserIDPointer(userID)
	collectionRecord := storage.VectorCollectionRecord{
		UserID:             userIDPtr,
		CollectionMakeName: collectionMakeName,
		CollectionName:     collectionName,
		Description:        fmt.Sprintf("Crawled pages for session %s", sessionID),
		RegisteredAt:       time.Now().UTC(),
		VectorSize:         vectorCfg.Dimension,
		InitEmbeddingModel: strings.TrimSpace(vectorCfg.EmbeddingModel),
		IsShared:           false,
		SharePermissions:   "read",
	}
	ctx := r.Context()
	if err := s.docStore.UpsertVectorCollection(ctx, collectionRecord); err != nil {
		if s.logger != nil {
			s.logger.Error("vector collection upsert failed",
				"session_id", sessionID,
				"collection", collectionName,
				"error", err)
		}
		http.Error(w, "failed to prepare collection", http.StatusBadGateway)
		return
	}
	if s.logger != nil {
		s.logger.Info("document sync started",
			"session_id", sessionID,
			"collection", collectionName,
			"batch_size", batchSize)
	}
	integrated := 0
	for {
		if err := ctx.Err(); err != nil {
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
			return
		}
		candidates, err := s.pageStore.FetchPagesReadyForDocumentSync(ctx, sessionID, batchSize)
		if err != nil {
			if s.logger != nil {
				s.logger.Error("fetch pages for document sync failed",
					"session_id", sessionID,
					"error", err)
			}
			http.Error(w, "failed to load pages for document sync", http.StatusInternalServerError)
			return
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			chunk := buildChunkRecord(candidate, collectionName, userIDPtr, vectorCfg)
			if err := s.docStore.UpsertVectorChunk(ctx, chunk); err != nil {
				if s.logger != nil {
					s.logger.Error("vector chunk upsert failed",
						"session_id", sessionID,
						"url", candidate.URL,
						"error", err)
				}
				http.Error(w, "failed to store document chunk", http.StatusBadGateway)
				return
			}
			if err := s.pageStore.MarkPageDocumentIntegrated(ctx, sessionID, candidate.URL); err != nil {
				if s.logger != nil {
					s.logger.Error("mark document integrated failed",
						"session_id", sessionID,
						"url", candidate.URL,
						"error", err)
				}
				http.Error(w, "failed to record document integration", http.StatusInternalServerError)
				return
			}
			integrated++
		}
	}
	status := "completed"
	if integrated == 0 {
		status = "idle"
	}
	if s.logger != nil {
		s.logger.Info("document sync finished",
			"session_id", sessionID,
			"collection", collectionName,
			"integrated", integrated)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session_id":           sessionID,
		"collection_name":      collectionName,
		"collection_make_name": collectionMakeName,
		"integrated_documents": integrated,
		"status":               status,
		"timestamp":            time.Now().UTC(),
	})
}

func (s *Server) handleAllPages(w http.ResponseWriter, r *http.Request) {
	if s.pageStore == nil {
		http.Error(w, "page store not configured", http.StatusNotImplemented)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, r, http.MethodGet)
		return
	}
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	search := r.URL.Query().Get("search")
	params := storage.PageListParams{
		Page:     page,
		PageSize: pageSize,
		Search:   search,
	}
	if sessionID != "" {
		res, err := s.pageStore.ListPages(r.Context(), sessionID, params)
		if err != nil {
			if s.logger != nil {
				s.logger.Error("list session pages failed", "session_id", sessionID, "error", err)
			}
			http.Error(w, "failed to list pages", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, res)
		return
	}
	res, err := s.pageStore.ListAllPages(r.Context(), params, "")
	if err != nil {
		if s.logger != nil {
			s.logger.Error("list all pages failed", "error", err)
		}
		http.Error(w, "failed to list pages", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handlePageSessions(w http.ResponseWriter, r *http.Request) {
	if s.pageStore == nil {
		http.Error(w, "page store not configured", http.StatusNotImplemented)
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, r, http.MethodGet)
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	search := r.URL.Query().Get("search")
	params := storage.PageListParams{
		Page:     page,
		PageSize: pageSize,
		Search:   search,
	}
	result, err := s.pageStore.ListSessionPageOverviews(r.Context(), params)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("list session page overviews failed", "error", err)
		}
		http.Error(w, "failed to list session pages", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) runIndexJob(job *indexJob, vectorCfg config.VectorDBConfig, batchSize int, userID, userName string) {
	ctx := job.Context()
	vectorStore, err := storage.NewVectorStore(vectorCfg, s.logger)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("vector store init failed", "session_id", job.sessionID, "error", err)
		}
		s.failIndexJob(job, fmt.Errorf("vector store init failed: %w", err))
		return
	}
	if vectorStore == nil {
		s.failIndexJob(job, errors.New("vector store not configured"))
		return
	}

	processed := make(map[string]struct{})

	for {
		if err := ctx.Err(); err != nil {
			job.cancelWithReason("index job cancelled")
			return
		}
		candidates, err := s.pageStore.FetchPagesNeedingIndex(ctx, job.sessionID, batchSize)
		if err != nil {
			s.failIndexJob(job, fmt.Errorf("fetch pages needing index: %w", err))
			return
		}
		pending := make([]storage.IndexCandidate, 0, len(candidates))
		for _, candidate := range candidates {
			if _, seen := processed[candidate.URL]; seen {
				continue
			}
			pending = append(pending, candidate)
		}
		if len(pending) == 0 {
			job.complete()
			if s.logger != nil {
				s.logger.Info("index job completed", "session_id", job.sessionID)
			}
			return
		}

		for _, candidate := range pending {
			if err := ctx.Err(); err != nil {
				job.cancelWithReason("index job cancelled")
				return
			}
			processed[candidate.URL] = struct{}{}
			url := candidate.URL
			markdown := strings.TrimSpace(candidate.Markdown)
			if markdown == "" {
				if s.logger != nil {
					s.logger.Warn("skipping page without markdown for indexing",
						"session_id", job.sessionID,
						"url", url)
				}
				if err := s.pageStore.MarkPageIndexed(ctx, job.sessionID, url); err != nil {
					s.failIndexJob(job, fmt.Errorf("mark page indexed: %w", err))
					return
				}
				job.progress(url)
				continue
			}

			doc := storage.Document{
				URL:           url,
				FinalURL:      candidate.FinalURL,
				Markdown:      candidate.Markdown,
				ExtractedText: candidate.ExtractedText,
				Metadata:      candidate.Metadata,
				SessionID:     job.sessionID,
				ContentHash:   candidate.ContentHash,
				NeedsIndex:    true,
				UserID:        userID,
				UserName:      userName,
			}

			if err := vectorStore.UpsertEmbedding(ctx, doc); err != nil {
				s.handleIndexCandidateFailure(job, url, err)
				continue
			}
			if err := s.pageStore.MarkPageIndexed(ctx, job.sessionID, url); err != nil {
				s.failIndexJob(job, fmt.Errorf("mark page indexed: %w", err))
				return
			}
			job.progress(url)
		}
	}
}

func setCORSHeaders(w http.ResponseWriter, r *http.Request) {
	origin := resolveOrigin(r)
	w.Header().Set("Access-Control-Allow-Origin", origin)
	if origin != "*" {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-User-ID, X-User-Name")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Max-Age", "86400")
}

func resolveOrigin(r *http.Request) string {
	raw := strings.TrimSpace(os.Getenv("CORS_ALLOW_ORIGIN"))
	req := strings.TrimSpace(r.Header.Get("Origin"))
	if req == "" {
		req = strings.TrimSpace(r.Header.Get("X-Forwarded-Origin"))
	}
	if raw == "" {
		if req != "" {
			return req
		}
		return "*"
	}
	parts := strings.Split(raw, ",")
	reqOrigin := req
	var fallback string
	for _, part := range parts {
		allowed := strings.TrimSpace(part)
		if allowed == "" {
			continue
		}
		if fallback == "" {
			fallback = allowed
		}
		if allowed == "*" {
			return "*"
		}
		if reqOrigin != "" && strings.EqualFold(allowed, reqOrigin) {
			return reqOrigin
		}
	}
	if fallback != "" {
		return fallback
	}
	if reqOrigin != "" {
		return reqOrigin
	}
	return "*"
}

func methodNotAllowed(w http.ResponseWriter, r *http.Request, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func urlPathDecode(segment string) (string, error) {
	return url.PathUnescape(segment)
}

func (s *Server) resolveVectorConfig(sessionID string, override *VectorDBRequest) (config.VectorDBConfig, bool) {
	var cfg config.VectorDBConfig
	var have bool

	if s.manager != nil {
		base := s.manager.baseConfig.VectorDB
		have = have || vectorConfigProvided(base)
		if session, ok := s.manager.GetSession(sessionID); ok {
			snap := session.ConfigSnapshot().VectorDB
			base = mergeVectorConfig(base, snap)
			have = have || vectorConfigProvided(snap)
		}
		cfg = base
	}

	if override != nil {
		overrideCfg := vectorRequestToConfig(override)
		cfg = mergeVectorConfig(cfg, overrideCfg)
		have = have || vectorConfigProvided(overrideCfg)
	}

	if !have {
		return config.VectorDBConfig{}, false
	}
	return cfg, true
}

func idleIndexEvent(sessionID string, pending int64) indexEvent {
	return indexEvent{
		Type:      "index_idle",
		Timestamp: time.Now(),
		SessionID: sessionID,
		Status:    "idle",
		Total:     pending,
		Processed: 0,
		Message:   "no pages pending indexing",
	}
}

func (s *Server) getIndexJob(sessionID string) (*indexJob, bool) {
	s.indexMu.Lock()
	job, ok := s.indexJobs[sessionID]
	s.indexMu.Unlock()
	return job, ok
}

func (s *Server) clearIndexJob(sessionID string, job *indexJob) {
	if s == nil || job == nil {
		return
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	current, ok := s.indexJobs[sessionID]
	if ok && current == job {
		delete(s.indexJobs, sessionID)
	}
}

func (s *Server) abortIndexJob(sessionID, reason string) {
	if s == nil {
		return
	}
	s.indexMu.Lock()
	job, ok := s.indexJobs[sessionID]
	if ok {
		delete(s.indexJobs, sessionID)
	}
	s.indexMu.Unlock()
	if ok && job != nil {
		if strings.TrimSpace(reason) == "" {
			reason = "index job cancelled"
		}
		job.cancelWithReason(reason)
	}
}

func (s *Server) deleteVectorData(ctx context.Context, sessionID string) error {
	vectorCfg, ok := s.resolveVectorConfig(sessionID, nil)
	if !ok || !vectorConfigComplete(vectorCfg) {
		return nil
	}
	vectorStore, err := storage.NewVectorStore(vectorCfg, s.logger)
	if err != nil {
		return fmt.Errorf("vector store init failed: %w", err)
	}
	if vectorStore == nil {
		return nil
	}
	if err := vectorStore.DeleteSessionVectors(ctx, sessionID); err != nil {
		return fmt.Errorf("delete vector data: %w", err)
	}
	return nil
}

func (s *Server) failIndexJob(job *indexJob, err error) {
	job.fail(err)
	if s.logger != nil {
		s.logger.Error("index job failed",
			"session_id", job.sessionID,
			"error", err)
	}
}

func (s *Server) handleIndexCandidateFailure(job *indexJob, url string, err error) {
	if s.logger != nil {
		s.logger.Warn("index candidate failed; skipping for this run",
			"session_id", job.sessionID,
			"url", url,
			"error", err)
	}
	msg := fmt.Sprintf("failed to index %s: %v", url, err)
	job.progressWithMessage(url, msg)
}

type embeddingConfigResponse struct {
	ProviderInfo struct {
		Provider  string `json:"provider"`
		Model     string `json:"model"`
		Dimension int    `json:"dimension"`
	} `json:"provider_info"`
}

func (s *Server) populateEmbeddingMetadata(ctx context.Context, cfg *config.VectorDBConfig, userID, userName string) error {
	if cfg == nil {
		return fmt.Errorf("vector config missing")
	}
	originalProvider := strings.TrimSpace(cfg.Provider)
	info, err := s.fetchEmbeddingConfig(ctx, userID, userName)
	if err != nil {
		return err
	}
	if info.ProviderInfo.Dimension <= 0 {
		return fmt.Errorf("invalid embedding dimension %d", info.ProviderInfo.Dimension)
	}
	cfg.Dimension = info.ProviderInfo.Dimension
	if model := strings.TrimSpace(info.ProviderInfo.Model); model != "" {
		cfg.EmbeddingModel = model
	}
	if provider := strings.TrimSpace(info.ProviderInfo.Provider); provider != "" && originalProvider == "" {
		cfg.Provider = provider
	}
	return nil
}

func (s *Server) fetchEmbeddingConfig(ctx context.Context, userID, userName string) (embeddingConfigResponse, error) {
	base := strings.TrimSpace(os.Getenv("XGEN_EMBEDDING_BASE_URL"))
	if base == "" {
		base = "http://embedding-service:8000"
	}
	base = strings.TrimRight(base, "/")
	configURL := fmt.Sprintf("%s/api/embedding/config-status", base)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, nil)
	if err != nil {
		return embeddingConfigResponse{}, err
	}
	req.Header.Set("X-User-ID", userID)
	req.Header.Set("X-User-Name", userName)

	resp, err := embeddingHTTPClient.Do(req)
	if err != nil {
		return embeddingConfigResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return embeddingConfigResponse{}, fmt.Errorf("embedding config status failed: status %d body %s", resp.StatusCode, string(msg))
	}
	var parsed embeddingConfigResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return embeddingConfigResponse{}, fmt.Errorf("decode embedding config: %w", err)
	}
	return parsed, nil
}

func parseUserIDPointer(raw string) *int64 {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	if v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
		return &v
	}
	return nil
}

func deriveCollectionIdentifiers(sessionID string, cfg config.VectorDBConfig) (string, string) {
	cleanSession := strings.TrimSpace(sessionID)
	if cleanSession == "" {
		cleanSession = "crawler-session"
	}
	makeName := fmt.Sprintf("crawler-%s", cleanSession)
	if idx := strings.TrimSpace(cfg.Index); idx != "" {
		makeName = fmt.Sprintf("%s-%s", idx, cleanSession)
	}
	collectionName := cleanSession
	if ns := strings.TrimSpace(cfg.Namespace); ns != "" {
		collectionName = fmt.Sprintf("%s-%s", ns, cleanSession)
	}
	return makeName, collectionName
}

func buildChunkRecord(candidate storage.DocumentSyncCandidate, collectionName string, userID *int64, cfg config.VectorDBConfig) storage.VectorChunkRecord {
	meta := candidate.Metadata
	chunkText := strings.TrimSpace(candidate.Markdown)
	if chunkText == "" {
		chunkText = strings.TrimSpace(candidate.ExtractedText)
	}
	if chunkText == "" {
		chunkText = firstNonEmptyMeta(meta, "summary", "description")
	}
	if chunkText == "" {
		chunkText = candidate.URL
	}
	summary := firstNonEmptyMeta(meta, "summary", "description")
	keywords := splitMetadataList(meta, "keywords", "tags")
	topics := splitMetadataList(meta, "topics")
	entities := splitMetadataList(meta, "entities")
	concepts := splitMetadataList(meta, "main_concepts")
	sentiment := firstNonEmptyMeta(meta, "sentiment")
	language := firstNonEmptyMeta(meta, "language", "lang", "locale")
	if language == "" {
		language = "unknown"
	}
	complexity := firstNonEmptyMeta(meta, "complexity_level", "reading_level")
	fileName := deriveFileName(candidate.URL, meta)
	chunkID := storage.GeneratePointID(candidate.ContentHash, candidate.URL)
	documentID := strings.TrimSpace(candidate.ContentHash)
	if documentID == "" {
		documentID = candidate.URL
	}
	provider := strings.TrimSpace(cfg.Provider)
	if provider == "" {
		provider = "default"
	}
	model := strings.TrimSpace(cfg.EmbeddingModel)
	chunkSize := utf8.RuneCountInString(chunkText)
	if chunkSize == 0 {
		chunkSize = len(chunkText)
	}
	return storage.VectorChunkRecord{
		UserID:             userID,
		CollectionName:     collectionName,
		FileName:           fileName,
		ChunkID:            chunkID,
		ChunkText:          chunkText,
		ChunkIndex:         1,
		TotalChunks:        1,
		ChunkSize:          chunkSize,
		Summary:            summary,
		Keywords:           keywords,
		Topics:             topics,
		Entities:           entities,
		Sentiment:          sentiment,
		DocumentID:         documentID,
		DocumentType:       crawlerDocumentType,
		Language:           language,
		ComplexityLevel:    complexity,
		MainConcepts:       concepts,
		EmbeddingProvider:  provider,
		EmbeddingModelName: model,
		EmbeddingDimension: cfg.Dimension,
	}
}

func deriveFileName(rawURL string, meta map[string]string) string {
	if title := strings.TrimSpace(meta["title"]); title != "" {
		return title
	}
	if u, err := url.Parse(rawURL); err == nil {
		host := strings.TrimSpace(u.Host)
		if host != "" {
			return host
		}
		path := strings.Trim(u.Path, "/")
		if path != "" {
			return path
		}
	}
	return rawURL
}

func firstNonEmptyMeta(meta map[string]string, keys ...string) string {
	for _, key := range keys {
		if meta == nil {
			continue
		}
		if value := strings.TrimSpace(meta[key]); value != "" {
			return value
		}
	}
	return ""
}

func splitMetadataList(meta map[string]string, keys ...string) []string {
	raw := firstNonEmptyMeta(meta, keys...)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
