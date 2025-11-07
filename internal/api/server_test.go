package api

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"xgen-crawler/internal/config"
	"xgen-crawler/internal/storage"
)

type fakePageStore struct{}

func (fakePageStore) ListPages(ctx context.Context, sessionID string, params storage.PageListParams) (storage.PageListResult, error) {
	return storage.PageListResult{
		Total:    0,
		Page:     1,
		PageSize: 20,
		Items:    []storage.PageSummary{},
	}, nil
}

func (fakePageStore) ListAllPages(ctx context.Context, params storage.PageListParams, sessionID string) (storage.GlobalPageListResult, error) {
	return storage.GlobalPageListResult{
		Total:    0,
		Page:     1,
		PageSize: 20,
		Items:    []storage.PageSummary{},
	}, nil
}

func (fakePageStore) ListSessionPageOverviews(ctx context.Context, params storage.PageListParams) (storage.SessionPageOverviewResult, error) {
	return storage.SessionPageOverviewResult{
		Total:    0,
		Page:     1,
		PageSize: 20,
		Items:    []storage.SessionPageOverview{},
	}, nil
}

func (fakePageStore) GetPageByURL(ctx context.Context, sessionID, url string) (storage.PageDetail, error) {
	return storage.PageDetail{}, sql.ErrNoRows
}

func TestServerHandlers(t *testing.T) {
	cfg := config.Default()
	cfg.Crawl.Seeds = []config.SeedConfig{
		{URL: "https://example.com", MaxDepth: 1},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := NewSessionManager(cfg, 1, context.Background(), logger, nil)
	server := NewServer(manager, fakePageStore{}, logger)

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
