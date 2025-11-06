package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"xgen-crawler/internal/config"
)

// QdrantStore persists embeddings to a Qdrant collection per session.
type QdrantStore struct {
	endpoint     string
	apiKey       string
	embeddingURL string

	httpClient *http.Client

	mu          sync.Mutex
	collections map[string]int // collection -> vector dimension
}

// NewQdrantStore initialises a Qdrant-backed VectorStore.
func NewQdrantStore(cfg config.VectorDBConfig, embeddingBase string) (*QdrantStore, error) {
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("qdrant endpoint not configured")
	}
	endpoint = strings.TrimRight(endpoint, "/")

	if embeddingBase == "" {
		return nil, fmt.Errorf("embedding base url not configured")
	}
	embeddingBase = strings.TrimRight(embeddingBase, "/")

	client := &http.Client{
		Timeout: 15 * time.Second,
	}

	return &QdrantStore{
		endpoint:     endpoint,
		apiKey:       strings.TrimSpace(cfg.APIKey),
		embeddingURL: embeddingBase,
		httpClient:   client,
		collections:  make(map[string]int),
	}, nil
}

type embeddingConfigResponse struct {
	ClientInitialized bool `json:"client_initialized"`
	ClientAvailable   bool `json:"client_available"`
	ProviderInfo      struct {
		Provider         string `json:"provider"`
		Model            string `json:"model"`
		Dimension        int    `json:"dimension"`
		APIKeyConfigured bool   `json:"api_key_configured"`
		Available        bool   `json:"available"`
	} `json:"provider_info"`
}

type embeddingQueryRequest struct {
	Text string `json:"text"`
}

type embeddingQueryResponse struct {
	Embedding []float64 `json:"embedding"`
	Dimension int       `json:"dimension"`
	Provider  string    `json:"provider"`
	Model     string    `json:"model"`
}

// UpsertEmbedding embeds the document markdown and stores it in Qdrant.
func (s *QdrantStore) UpsertEmbedding(ctx context.Context, doc Document) error {
	if !doc.NeedsIndex {
		return nil
	}
	markdown := strings.TrimSpace(doc.Markdown)
	if markdown == "" {
		return nil
	}
	if doc.SessionID == "" {
		return fmt.Errorf("missing session id for vector upsert")
	}
	if doc.UserID == "" || doc.UserName == "" {
		return fmt.Errorf("missing user identity in document metadata")
	}

	dimension, err := s.ensureCollection(ctx, doc.SessionID, doc.UserID, doc.UserName)
	if err != nil {
		return fmt.Errorf("ensure collection: %w", err)
	}

	embedding, err := s.fetchEmbedding(ctx, markdown, doc.UserID, doc.UserName)
	if err != nil {
		return fmt.Errorf("generate embedding: %w", err)
	}

	if dimension > 0 && len(embedding) != dimension {
		return fmt.Errorf("embedding dimension mismatch: expected %d, got %d", dimension, len(embedding))
	}

	payload := map[string]any{
		"url":            doc.URL,
		"final_url":      doc.FinalURL,
		"session_id":     doc.SessionID,
		"content_hash":   doc.ContentHash,
		"markdown":       doc.Markdown,
		"extracted_text": doc.ExtractedText,
		"metadata":       doc.Metadata,
	}

	point := map[string]any{
		"id":      doc.URL,
		"vector":  embedding,
		"payload": payload,
	}

	body := map[string]any{
		"points": []any{point},
	}

	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal qdrant payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.collectionPointsURL(doc.SessionID), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("build qdrant request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.apiKey != "" {
		req.Header.Set("api-key", s.apiKey)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("qdrant upsert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("qdrant upsert failed: status %d body %s", resp.StatusCode, string(msg))
	}
	return nil
}

func (s *QdrantStore) ensureCollection(ctx context.Context, collection, userID, userName string) (int, error) {
	s.mu.Lock()
	if dim, ok := s.collections[collection]; ok {
		s.mu.Unlock()
		return dim, nil
	}
	s.mu.Unlock()

	cfg, err := s.fetchEmbeddingConfig(ctx, userID, userName)
	if err != nil {
		return 0, err
	}
	dimension := cfg.ProviderInfo.Dimension
	if dimension <= 0 {
		return 0, fmt.Errorf("embedding service returned invalid dimension %d", dimension)
	}

	body := map[string]any{
		"vectors": map[string]any{
			"size":     dimension,
			"distance": "Cosine",
		},
	}
	data, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("marshal collection payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.collectionURL(collection), bytes.NewReader(data))
	if err != nil {
		return 0, fmt.Errorf("build collection request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.apiKey != "" {
		req.Header.Set("api-key", s.apiKey)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("create collection: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusConflict {
		msg, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("create collection failed: status %d body %s", resp.StatusCode, string(msg))
	}

	s.mu.Lock()
	s.collections[collection] = dimension
	s.mu.Unlock()
	return dimension, nil
}

func (s *QdrantStore) fetchEmbeddingConfig(ctx context.Context, userID, userName string) (*embeddingConfigResponse, error) {
	configURL := fmt.Sprintf("%s/api/embedding/config-status", s.embeddingURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-User-ID", userID)
	req.Header.Set("X-User-Name", userName)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("embedding config status failed: status %d body %s", resp.StatusCode, string(msg))
	}

	var parsed embeddingConfigResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode embedding config: %w", err)
	}
	return &parsed, nil
}

func (s *QdrantStore) fetchEmbedding(ctx context.Context, text, userID, userName string) ([]float64, error) {
	queryURL := fmt.Sprintf("%s/api/embedding/query-embedding", s.embeddingURL)
	payload := embeddingQueryRequest{Text: text}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, queryURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", userID)
	req.Header.Set("X-User-Name", userName)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("embedding query failed: status %d body %s", resp.StatusCode, string(msg))
	}

	var parsed embeddingQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode embedding response: %w", err)
	}
	return parsed.Embedding, nil
}

func (s *QdrantStore) collectionURL(collection string) string {
	return fmt.Sprintf("%s/collections/%s", s.endpoint, url.PathEscape(collection))
}

func (s *QdrantStore) collectionPointsURL(collection string) string {
	return fmt.Sprintf("%s/collections/%s/points", s.endpoint, url.PathEscape(collection))
}
