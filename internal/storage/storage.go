package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"newscrawler/internal/config"
	"newscrawler/pkg/types"
)

// Document captures the essential information extracted from a crawl result.
type Document struct {
	URL         string
	FinalURL    string
	Depth       int
	RetrievedAt time.Time
	StatusCode  int
	HTML        []byte
	CleanHTML   []byte
	Metadata    map[string]string
}

// RelationalStore persists structured crawl data into a SQL database.
type RelationalStore interface {
	SavePage(ctx context.Context, doc Document) error
}

// VectorStore persists embeddings into a vector database.
type VectorStore interface {
	UpsertEmbedding(ctx context.Context, doc Document) error
}

// Pipeline fans out crawl results to relational and vector stores.
type Pipeline struct {
	relational RelationalStore
	vector     VectorStore
}

// NewPipeline constructs a storage pipeline.
func NewPipeline(rel RelationalStore, vec VectorStore) *Pipeline {
	if rel == nil && vec == nil {
		return nil
	}
	return &Pipeline{relational: rel, vector: vec}
}

// Persist stores the crawl result in the configured sinks.
func (p *Pipeline) Persist(ctx context.Context, result types.CrawlResult) error {
	if p == nil {
		return nil
	}
	if result.Page == nil || result.Request.URL == nil {
		return fmt.Errorf("invalid crawl result: missing page or url")
	}
	finalURL := result.Request.URL.String()
	if result.Page.FinalURL != nil {
		finalURL = result.Page.FinalURL.String()
	}
	doc := Document{
		URL:         result.Request.URL.String(),
		FinalURL:    finalURL,
		Depth:       result.Request.Depth,
		RetrievedAt: result.Page.FetchedAt,
		StatusCode:  result.Page.StatusCode,
		HTML:        result.Page.Body,
		CleanHTML:   result.Preprocessed,
		Metadata:    result.Metadata,
	}

	if p.relational != nil {
		if err := p.relational.SavePage(ctx, doc); err != nil {
			return fmt.Errorf("relational store: %w", err)
		}
	}
	if p.vector != nil {
		if err := p.vector.UpsertEmbedding(ctx, doc); err != nil {
			return fmt.Errorf("vector store: %w", err)
		}
	}
	return nil
}

// SQLWriter is a simple relational store example backed by database/sql.
type SQLWriter struct {
	db *sql.DB
}

// NewSQLWriter initialises a SQLWriter from configuration.
func NewSQLWriter(cfg config.SQLConfig) (*SQLWriter, error) {
	if cfg.Driver == "" || cfg.DSN == "" {
		return nil, errors.New("sql config missing driver or dsn")
	}
	db, err := sql.Open(cfg.Driver, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open sql connection: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime.Duration > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime.Duration)
	}
	return &SQLWriter{db: db}, nil
}

// SavePage inserts the crawl document into a generic pages table.
func (s *SQLWriter) SavePage(ctx context.Context, doc Document) error {
	if s == nil || s.db == nil {
		return nil
	}
	query := `
        INSERT INTO pages (url, final_url, depth, retrieved_at, status_code, raw_html, clean_html)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (url) DO UPDATE SET
            final_url = EXCLUDED.final_url,
            depth = EXCLUDED.depth,
            retrieved_at = EXCLUDED.retrieved_at,
            status_code = EXCLUDED.status_code,
            raw_html = EXCLUDED.raw_html,
            clean_html = EXCLUDED.clean_html
    `
	_, err := s.db.ExecContext(ctx, query,
		doc.URL,
		doc.FinalURL,
		doc.Depth,
		doc.RetrievedAt,
		doc.StatusCode,
		doc.HTML,
		doc.CleanHTML,
	)
	if err != nil {
		return fmt.Errorf("insert page: %w", err)
	}
	return nil
}

// Close closes the underlying DB connection.
func (s *SQLWriter) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// NoopVectorStore is a placeholder implementation for vector databases.
type NoopVectorStore struct{}

// UpsertEmbedding satisfies the VectorStore interface without persisting data.
func (NoopVectorStore) UpsertEmbedding(ctx context.Context, doc Document) error {
	return nil
}
