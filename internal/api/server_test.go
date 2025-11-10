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

func (fakePageStore) CountPagesNeedingIndex(ctx context.Context, sessionID string) (int64, error) {
	return 0, nil
}

func (fakePageStore) FetchPagesNeedingIndex(ctx context.Context, sessionID string, limit int) ([]storage.IndexCandidate, error) {
	return nil, nil
}

func (fakePageStore) MarkPageIndexed(ctx context.Context, sessionID, url string) error {
	return nil
}

func (fakePageStore) FetchPagesReadyForDocumentSync(ctx context.Context, sessionID string, limit int) ([]storage.DocumentSyncCandidate, error) {
	return nil, nil
}

func (fakePageStore) MarkPageDocumentIntegrated(ctx context.Context, sessionID, url string) error {
	return nil
}

func (fakePageStore) DeleteSessionData(ctx context.Context, sessionID string) error {
	return nil
}

func TestServerHandlers(t *testing.T) {
	cfg := config.Default()
	cfg.Crawl.Seeds = []config.SeedConfig{
		{URL: "https://example.com", MaxDepth: 1},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := NewSessionManager(cfg, 1, context.Background(), logger, nil)
	server := NewServer(manager, fakePageStore{}, nil, logger)

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

func TestResolveVectorConfigFallbackToBaseConfig(t *testing.T) {
	base := config.Default()
	base.VectorDB.Provider = "qdrant"
	base.VectorDB.Endpoint = "http://vector.example.com"

	manager := NewSessionManager(base, 1, context.Background(), nil, nil)
	server := NewServer(manager, nil, nil, nil)

	cfg, ok := server.resolveVectorConfig("nonexistent", nil)
	if !ok {
		t.Fatalf("expected fallback to base vector config")
	}
	if cfg.Provider != base.VectorDB.Provider || cfg.Endpoint != base.VectorDB.Endpoint {
		t.Fatalf("unexpected vector config: %#v", cfg)
	}
}

func TestPopulateEmbeddingMetadataDoesNotOverrideVectorProvider(t *testing.T) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/embedding/config-status" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("X-User-ID") == "" || r.Header.Get("X-User-Name") == "" {
			t.Fatalf("missing auth headers")
		}
		io.WriteString(w, `{
			"provider_info": {
				"provider": "openai",
				"model": "text-embedding-3-large",
				"dimension": 1536
			}
		}`)
	}))
	defer ts.Close()
	t.Setenv("XGEN_EMBEDDING_BASE_URL", ts.URL)

	server := &Server{}
	cfg := config.VectorDBConfig{
		Provider: "qdrant",
		Endpoint: "http://qdrant:6333",
	}
	err := server.populateEmbeddingMetadata(context.Background(), &cfg, "user-1", "alice")
	if err != nil {
		t.Fatalf("populateEmbeddingMetadata returned error: %v", err)
	}
	if cfg.Provider != "qdrant" {
		t.Fatalf("expected provider to remain qdrant, got %s", cfg.Provider)
	}
	if cfg.Dimension != 1536 {
		t.Fatalf("expected dimension 1536, got %d", cfg.Dimension)
	}
	if cfg.EmbeddingModel != "text-embedding-3-large" {
		t.Fatalf("expected embedding model updated, got %s", cfg.EmbeddingModel)
	}
}
