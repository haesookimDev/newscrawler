package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"xgen-crawler/internal/storage"
)

// Server exposes the HTTP API for managing crawler sessions.
type PageStore interface {
	ListPages(ctx context.Context, sessionID string, params storage.PageListParams) (storage.PageListResult, error)
	ListAllPages(ctx context.Context, params storage.PageListParams, sessionID string) (storage.GlobalPageListResult, error)
	ListSessionPageOverviews(ctx context.Context, params storage.PageListParams) (storage.SessionPageOverviewResult, error)
	GetPageByURL(ctx context.Context, sessionID, url string) (storage.PageDetail, error)
}

type Server struct {
	manager   *SessionManager
	pageStore PageStore
	mux       *http.ServeMux
	logger    *slog.Logger
}

// NewServer wires handlers onto an HTTP mux.
func NewServer(manager *SessionManager, store PageStore, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		manager:   manager,
		pageStore: store,
		mux:       http.NewServeMux(),
		logger:    logger,
	}
	s.routes()
	return s
}

// ServeHTTP satisfies the http.Handler interface.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		default:
			methodNotAllowed(w, r, http.MethodGet)
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
	w.WriteHeader(http.StatusAccepted)
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
