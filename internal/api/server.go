package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Server exposes the HTTP API for managing crawler sessions.
type Server struct {
	manager *SessionManager
	mux     *http.ServeMux
}

// NewServer wires handlers onto an HTTP mux.
func NewServer(manager *SessionManager) *Server {
	s := &Server{
		manager: manager,
		mux:     http.NewServeMux(),
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
	session, err := s.manager.StartSession(req)
	if err != nil {
		switch {
		case errors.Is(err, ErrSessionRunning):
			http.Error(w, err.Error(), http.StatusConflict)
		case errors.Is(err, ErrMaxConcurrency):
			http.Error(w, err.Error(), http.StatusTooManyRequests)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
	writeJSON(w, http.StatusCreated, session.Snapshot())
}

func (s *Server) listSessions(w http.ResponseWriter, r *http.Request) {
	summaries := s.manager.ListSessions()
	writeJSON(w, http.StatusOK, summaries)
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request, id string) {
	detail, ok := s.manager.GetSessionDetail(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) cancelSession(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.manager.CancelSession(id); err != nil {
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
