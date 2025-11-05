package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"xgen-crawler/internal/config"
)

func TestServerHandlers(t *testing.T) {
	cfg := config.Default()
	cfg.Crawl.Seeds = []config.SeedConfig{
		{URL: "https://example.com", MaxDepth: 1},
	}

	manager := NewSessionManager(cfg, 1, context.Background())
	server := NewServer(manager)

	assertRoute(t, server, http.MethodGet, "/health", http.StatusOK, "application/json")
	assertRoute(t, server, http.MethodGet, "/openapi.yaml", http.StatusOK, "application/yaml")
	assertRoute(t, server, http.MethodGet, "/docs", http.StatusOK, "text/html; charset=utf-8")
}

func assertRoute(t *testing.T, h http.Handler, method, path string, wantStatus int, wantContentType string) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != wantStatus {
		t.Fatalf("%s %s: expected status %d, got %d (body=%s)", method, path, wantStatus, rr.Code, rr.Body.String())
	}
	if wantContentType != "" {
		if got := rr.Header().Get("Content-Type"); got != wantContentType {
			t.Fatalf("%s %s: expected content-type %s, got %s", method, path, wantContentType, got)
		}
	}
	if rr.Body.Len() == 0 {
		t.Fatalf("%s %s: expected non-empty body", method, path)
	}
}
