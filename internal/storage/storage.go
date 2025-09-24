package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	pq "github.com/lib/pq"

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
	db          *sql.DB
	autoMigrate bool
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		if cfg.CreateIfMissing && shouldAttemptCreateDatabase(cfg.Driver, err) {
			_ = db.Close()
			if err := createDatabase(ctx, cfg); err != nil {
				return nil, err
			}
			db, err = sql.Open(cfg.Driver, cfg.DSN)
			if err != nil {
				return nil, fmt.Errorf("open sql connection: %w", err)
			}
			if err := db.PingContext(ctx); err != nil {
				return nil, fmt.Errorf("ping sql connection: %w", err)
			}
		} else {
			return nil, fmt.Errorf("ping sql connection: %w", err)
		}
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
	writer := &SQLWriter{
		db:          db,
		autoMigrate: cfg.AutoMigrate,
	}
	if cfg.AutoMigrate {
		if err := writer.ensureSchema(context.Background()); err != nil {
			return nil, err
		}
	}
	return writer, nil
}

// SavePage inserts the crawl document into a generic pages table.
func (s *SQLWriter) SavePage(ctx context.Context, doc Document) error {
	if s == nil || s.db == nil {
		return nil
	}
	if err := s.upsertPage(ctx, doc); err != nil {
		if s.autoMigrate && isUndefinedTableErr(err) {
			if schemaErr := s.ensureSchema(ctx); schemaErr != nil {
				return fmt.Errorf("ensure schema: %w", schemaErr)
			}
			if retryErr := s.upsertPage(ctx, doc); retryErr != nil {
				return fmt.Errorf("insert page: %w", retryErr)
			}
			return nil
		}
		return fmt.Errorf("insert page: %w", err)
	}
	return nil
}

func (s *SQLWriter) upsertPage(ctx context.Context, doc Document) error {
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
	if _, err := s.db.ExecContext(ctx, query,
		doc.URL,
		doc.FinalURL,
		doc.Depth,
		doc.RetrievedAt,
		doc.StatusCode,
		doc.HTML,
		doc.CleanHTML,
	); err != nil {
		return err
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

func shouldAttemptCreateDatabase(driver string, err error) bool {
	if !strings.EqualFold(driver, "postgres") {
		return false
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "3D000"
	}
	return strings.Contains(strings.ToLower(err.Error()), "does not exist")
}

func createDatabase(ctx context.Context, cfg config.SQLConfig) error {
	parsed, err := url.Parse(cfg.DSN)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	dbName := strings.TrimPrefix(parsed.Path, "/")
	if dbName == "" {
		return errors.New("dsn missing database name")
	}
	if strings.EqualFold(dbName, "postgres") {
		return fmt.Errorf("target database %q cannot be auto-created", dbName)
	}
	parsed.Path = "/postgres"
	adminDSN := parsed.String()
	adminDB, err := sql.Open(cfg.Driver, adminDSN)
	if err != nil {
		return fmt.Errorf("connect admin database: %w", err)
	}
	defer adminDB.Close()
	if err := adminDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping admin database: %w", err)
	}
	stmt := fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(dbName))
	if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "42P04" {
			return nil
		}
		return fmt.Errorf("create database %q: %w", dbName, err)
	}
	return nil
}

func (s *SQLWriter) ensureSchema(ctx context.Context) error {
	if s == nil || s.db == nil || !s.autoMigrate {
		return nil
	}
	schemaCtx := ctx
	if schemaCtx == nil || schemaCtx.Err() != nil {
		schemaCtx = context.Background()
	}
	schemaCtx, cancel := context.WithTimeout(schemaCtx, 10*time.Second)
	defer cancel()

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS pages (
		    url TEXT PRIMARY KEY,
		    final_url TEXT,
		    depth INT,
		    retrieved_at TIMESTAMPTZ,
		    status_code INT,
		    raw_html BYTEA,
		    clean_html BYTEA
		)`,
		`CREATE INDEX IF NOT EXISTS idx_pages_retrieved_at ON pages (retrieved_at DESC)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(schemaCtx, stmt); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}
	return nil
}

func isUndefinedTableErr(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "42P01"
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "relation") && strings.Contains(lower, "does not exist")
}
