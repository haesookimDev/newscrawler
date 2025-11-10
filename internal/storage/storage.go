package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	pq "github.com/lib/pq"

	"xgen-crawler/internal/config"
	"xgen-crawler/pkg/types"
)

// Document captures the essential information extracted from a crawl result.
type Document struct {
	URL           string
	FinalURL      string
	Depth         int
	RetrievedAt   time.Time
	StatusCode    int
	HTML          []byte
	CleanHTML     []byte
	ExtractedText string
	Markdown      string
	Metadata      map[string]string
	ScraperID     string
	RunID         string
	TenantID      string
	SeedLabel     string
	SessionID     string
	ContentHash   string
	NeedsIndex    bool
	IndexedAt     *time.Time
	UserID        string
	UserName      string
}

// ImageRecord tracks stored image metadata for persistence.
type ImageRecord struct {
	SessionID   string
	PageURL     string
	SourceURL   string
	StoredPath  string
	ContentType string
	SizeBytes   int64
	AltText     string
	Width       int
	Height      int
}

// ImageDocument contains the binary payload and metadata prior to storage.
type ImageDocument struct {
	PageURL     string
	SourceURL   string
	AltText     string
	ContentType string
	Data        []byte
	SizeBytes   int64
	Width       int
	Height      int
}

// RelationalStore persists structured crawl data into a SQL database.
type RelationalStore interface {
	SavePage(ctx context.Context, doc Document) error
	SaveImages(ctx context.Context, images []ImageRecord) error
	MarkPageNeedsIndex(ctx context.Context, sessionID, url string) error
}

// VectorStore persists embeddings into a vector database.
type VectorStore interface {
	UpsertEmbedding(ctx context.Context, doc Document) error
}

// MediaStore persists binary assets to an external storage system.
type MediaStore interface {
	SaveImage(ctx context.Context, image ImageDocument) (string, error)
}

// Pipeline fans out crawl results to relational, vector, and media stores.
type Pipeline struct {
	relational RelationalStore
	vector     VectorStore
	media      MediaStore

	logger      *slog.Logger
	vectorQueue chan Document
	vectorWG    sync.WaitGroup
	closeOnce   sync.Once
}

const (
	defaultVectorQueueSize   = 128
	defaultVectorWorkerCount = 4
)

// NewPipeline constructs a storage pipeline.
func NewPipeline(rel RelationalStore, vec VectorStore, media MediaStore, logger *slog.Logger) *Pipeline {
	if rel == nil && vec == nil && media == nil {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	p := &Pipeline{
		relational: rel,
		vector:     vec,
		media:      media,
		logger:     logger,
	}
	if vec != nil {
		p.startVectorWorkers(defaultVectorWorkerCount, defaultVectorQueueSize)
	}
	return p
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
		URL:           result.Request.URL.String(),
		FinalURL:      finalURL,
		Depth:         result.Request.Depth,
		RetrievedAt:   result.Page.FetchedAt,
		StatusCode:    result.Page.StatusCode,
		HTML:          result.Page.Body,
		Metadata:      result.Metadata,
		ScraperID:     result.Request.ScraperID,
		RunID:         result.Request.RunID,
		TenantID:      result.Request.TenantID,
		SeedLabel:     result.Request.SeedLabel,
		SessionID:     result.Request.SessionID,
		ContentHash:   "",
		NeedsIndex:    false,
		IndexedAt:     nil,
		CleanHTML:     nil,
		ExtractedText: "",
		Markdown:      "",
	}

	if v := strings.TrimSpace(result.Metadata["x_user_id"]); v != "" {
		doc.UserID = v
		delete(result.Metadata, "x_user_id")
	}
	if v := strings.TrimSpace(result.Metadata["x_user_name"]); v != "" {
		doc.UserName = v
		delete(result.Metadata, "x_user_name")
	}

	indexable := false
	if result.Processed != nil {
		doc.CleanHTML = result.Processed.CleanHTML
		doc.ExtractedText = result.Processed.ExtractedText
		doc.Markdown = result.Processed.Markdown
		indexable = true
		doc.ContentHash = computeContentFingerprint(doc.CleanHTML, doc.ExtractedText, doc.Markdown)
	} else {
		doc.ContentHash = computeContentFingerprint(doc.HTML, "", "")
	}
	doc.NeedsIndex = indexable && p.vector == nil

	if p.relational != nil {
		storeDoc := doc
		if err := p.relational.SavePage(ctx, storeDoc); err != nil {
			return fmt.Errorf("relational store: %w", err)
		}
	}

	if len(result.Images) > 0 {
		imageRecords := make([]ImageRecord, 0, len(result.Images))
		for i := range result.Images {
			img := &result.Images[i]
			storedPath := ""
			if p.media != nil && len(img.Data) > 0 {
				stored, err := p.media.SaveImage(ctx, ImageDocument{
					PageURL:     doc.URL,
					SourceURL:   img.SourceURL,
					AltText:     img.AltText,
					ContentType: img.ContentType,
					Data:        img.Data,
					SizeBytes:   img.SizeBytes,
					Width:       img.Width,
					Height:      img.Height,
				})
				if err != nil {
					return fmt.Errorf("media store: %w", err)
				}
				storedPath = stored
			}
			img.StoredPath = storedPath
			img.Data = nil
			if storedPath != "" {
				record := ImageRecord{
					SessionID:   doc.SessionID,
					PageURL:     doc.URL,
					SourceURL:   img.SourceURL,
					StoredPath:  storedPath,
					ContentType: img.ContentType,
					SizeBytes:   img.SizeBytes,
					AltText:     img.AltText,
					Width:       img.Width,
					Height:      img.Height,
				}
				imageRecords = append(imageRecords, record)
			}
		}
		if p.relational != nil && len(imageRecords) > 0 {
			if err := p.relational.SaveImages(ctx, imageRecords); err != nil {
				return fmt.Errorf("relational store images: %w", err)
			}
		}
	}
	if p.vector != nil {
		if indexable {
			vectorDoc := doc
			vectorDoc.NeedsIndex = true
			p.enqueueVectorTask(vectorDoc)
		}
	}
	return nil
}

// Close waits for background workers to finish draining queues.
func (p *Pipeline) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		if p.vectorQueue != nil {
			close(p.vectorQueue)
		}
	})
	p.vectorWG.Wait()
	return nil
}

func (p *Pipeline) startVectorWorkers(workers, queueSize int) {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = defaultVectorQueueSize
	}
	p.vectorQueue = make(chan Document, queueSize)
	for i := 0; i < workers; i++ {
		p.vectorWG.Add(1)
		go p.runVectorWorker(i + 1)
	}
}

func (p *Pipeline) runVectorWorker(workerID int) {
	defer p.vectorWG.Done()
	ctx := context.Background()
	for doc := range p.vectorQueue {
		if err := p.vector.UpsertEmbedding(ctx, doc); err != nil {
			if p.logger != nil {
				p.logger.Error("vector upsert failed",
					"worker", workerID,
					"session_id", doc.SessionID,
					"url", doc.URL,
					"error", err)
			}
			p.markIndexFailure(ctx, doc)
		}
	}
}

func (p *Pipeline) markIndexFailure(ctx context.Context, doc Document) {
	if p == nil || p.relational == nil {
		return
	}
	if doc.SessionID == "" || doc.URL == "" {
		return
	}
	if err := p.relational.MarkPageNeedsIndex(ctx, doc.SessionID, doc.URL); err != nil && p.logger != nil {
		p.logger.Error("failed to flag page for reindex",
			"session_id", doc.SessionID,
			"url", doc.URL,
			"error", err)
	}
}

func (p *Pipeline) enqueueVectorTask(doc Document) {
	if p.vectorQueue == nil {
		return
	}
	task := compactVectorDocument(doc)
	select {
	case p.vectorQueue <- task:
	default:
		if p.logger != nil {
			p.logger.Warn("vector queue full; dropping document",
				"session_id", doc.SessionID,
				"url", doc.URL)
		}
	}
}

func compactVectorDocument(doc Document) Document {
	return Document{
		URL:           doc.URL,
		FinalURL:      doc.FinalURL,
		ExtractedText: doc.ExtractedText,
		Markdown:      doc.Markdown,
		Metadata:      doc.Metadata,
		SessionID:     doc.SessionID,
		ContentHash:   doc.ContentHash,
		NeedsIndex:    doc.NeedsIndex,
		UserID:        doc.UserID,
		UserName:      doc.UserName,
	}
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

func computeContentFingerprint(cleanHTML []byte, extracted, markdown string) string {
	hasher := sha256.New()
	if len(cleanHTML) > 0 {
		_, _ = hasher.Write(cleanHTML)
	}
	_, _ = hasher.Write([]byte{0})
	if extracted != "" {
		_, _ = hasher.Write([]byte(extracted))
	}
	_, _ = hasher.Write([]byte{0})
	if markdown != "" {
		_, _ = hasher.Write([]byte(markdown))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

// SaveImages persists image metadata for a page.
func (s *SQLWriter) SaveImages(ctx context.Context, images []ImageRecord) error {
	if s == nil || s.db == nil {
		return nil
	}
	if len(images) == 0 {
		return nil
	}
	query := `
        INSERT INTO images (session_id, page_url, source_url, stored_path, content_type, size_bytes, alt_text, width, height)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (session_id, page_url, source_url) DO UPDATE SET
            stored_path = EXCLUDED.stored_path,
            content_type = EXCLUDED.content_type,
            size_bytes = EXCLUDED.size_bytes,
            alt_text = EXCLUDED.alt_text,
            width = EXCLUDED.width,
            height = EXCLUDED.height,
            updated_at = NOW()
    `
	for _, img := range images {
		sessionID := strings.TrimSpace(img.SessionID)
		if sessionID == "" {
			return fmt.Errorf("image record missing session id for page %s", img.PageURL)
		}
		if _, err := s.db.ExecContext(ctx, query,
			sessionID,
			img.PageURL,
			img.SourceURL,
			img.StoredPath,
			img.ContentType,
			img.SizeBytes,
			img.AltText,
			img.Width,
			img.Height,
		); err != nil {
			return fmt.Errorf("insert image: %w", err)
		}
	}
	return nil
}

func (s *SQLWriter) upsertPage(ctx context.Context, doc Document) error {
	sessionID := strings.TrimSpace(doc.SessionID)
	if sessionID == "" {
		return fmt.Errorf("missing session id for page %s", doc.URL)
	}
	var metadataJSON any
	if len(doc.Metadata) > 0 {
		encoded, err := json.Marshal(doc.Metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
		metadataJSON = string(encoded)
	}

	query := `
        INSERT INTO pages (
            url, final_url, depth, retrieved_at, status_code,
            raw_html, clean_html, extracted_text, markdown,
            metadata, scraper_id, run_id, tenant_id, seed_label,
            session_id, content_hash, needs_index, indexed_at
        )
        VALUES (
            $1,$2,$3,$4,$5,
            $6,$7,$8,$9,
            $10,$11,$12,$13,$14,
            $15,$16,$17,$18
        )
        ON CONFLICT (session_id, url) DO UPDATE SET
            final_url = EXCLUDED.final_url,
            depth = EXCLUDED.depth,
            retrieved_at = EXCLUDED.retrieved_at,
            status_code = EXCLUDED.status_code,
            raw_html = EXCLUDED.raw_html,
            clean_html = EXCLUDED.clean_html,
            extracted_text = EXCLUDED.extracted_text,
            markdown = EXCLUDED.markdown,
            metadata = EXCLUDED.metadata,
            scraper_id = EXCLUDED.scraper_id,
            run_id = EXCLUDED.run_id,
            tenant_id = EXCLUDED.tenant_id,
            seed_label = EXCLUDED.seed_label,
            session_id = EXCLUDED.session_id,
            content_hash = EXCLUDED.content_hash,
            needs_index = CASE
                WHEN pages.content_hash IS DISTINCT FROM EXCLUDED.content_hash THEN TRUE
                ELSE pages.needs_index
            END,
            indexed_at = CASE
                WHEN pages.content_hash IS DISTINCT FROM EXCLUDED.content_hash THEN NULL
                ELSE pages.indexed_at
            END
    `
	if _, err := s.db.ExecContext(ctx, query,
		doc.URL,
		doc.FinalURL,
		doc.Depth,
		doc.RetrievedAt,
		doc.StatusCode,
		doc.HTML,
		doc.CleanHTML,
		nullIfEmpty(doc.ExtractedText),
		nullIfEmpty(doc.Markdown),
		metadataJSON,
		nullIfEmpty(doc.ScraperID),
		nullIfEmpty(doc.RunID),
		nullIfEmpty(doc.TenantID),
		nullIfEmpty(doc.SeedLabel),
		sessionID,
		nullIfEmpty(doc.ContentHash),
		doc.NeedsIndex,
		timeOrNil(doc.IndexedAt),
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

func nullIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func timeOrNil(t *time.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return *t
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
		    session_id TEXT NOT NULL,
		    url TEXT NOT NULL,
		    final_url TEXT,
		    depth INT,
		    retrieved_at TIMESTAMPTZ,
		    status_code INT,
		    raw_html BYTEA,
		    clean_html BYTEA,
		    extracted_text TEXT,
		    markdown TEXT,
		    metadata JSONB,
		    scraper_id TEXT,
		    run_id TEXT,
		    tenant_id TEXT,
		    seed_label TEXT,
		    content_hash TEXT,
		    needs_index BOOLEAN DEFAULT TRUE,
		    indexed_at TIMESTAMPTZ,
		    PRIMARY KEY (session_id, url)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_pages_retrieved_at ON pages (retrieved_at DESC)`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS metadata JSONB`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS scraper_id TEXT`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS run_id TEXT`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS tenant_id TEXT`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS seed_label TEXT`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS extracted_text TEXT`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS markdown TEXT`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS session_id TEXT`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS content_hash TEXT`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS needs_index BOOLEAN DEFAULT TRUE`,
		`ALTER TABLE pages ADD COLUMN IF NOT EXISTS indexed_at TIMESTAMPTZ`,
		`UPDATE pages SET session_id = url WHERE (session_id IS NULL OR session_id = '')`,
		`ALTER TABLE pages ALTER COLUMN session_id SET NOT NULL`,
		`ALTER TABLE IF EXISTS images DROP CONSTRAINT IF EXISTS images_page_url_fkey`,
		`ALTER TABLE IF EXISTS images DROP CONSTRAINT IF EXISTS images_page_fk`,
		`ALTER TABLE pages DROP CONSTRAINT IF EXISTS pages_pkey`,
		`ALTER TABLE pages DROP CONSTRAINT IF EXISTS pages_session_url_pkey`,
		`ALTER TABLE pages ADD CONSTRAINT pages_session_url_pkey PRIMARY KEY (session_id, url)`,
		`CREATE INDEX IF NOT EXISTS idx_pages_session ON pages (session_id)`,
		`CREATE INDEX IF NOT EXISTS idx_pages_scraper_run ON pages (scraper_id, run_id)`,
		`CREATE TABLE IF NOT EXISTS images (
		    session_id TEXT NOT NULL,
		    page_url TEXT NOT NULL,
		    source_url TEXT NOT NULL,
		    stored_path TEXT NOT NULL,
		    content_type TEXT,
		    size_bytes BIGINT,
		    alt_text TEXT,
		    width INT,
		    height INT,
		    created_at TIMESTAMPTZ DEFAULT NOW(),
		    updated_at TIMESTAMPTZ DEFAULT NOW(),
		    PRIMARY KEY (session_id, page_url, source_url),
		    FOREIGN KEY (session_id, page_url) REFERENCES pages(session_id, url) ON DELETE CASCADE
		)`,
		`ALTER TABLE images ADD COLUMN IF NOT EXISTS session_id TEXT`,
		`UPDATE images SET session_id = p.session_id FROM pages p WHERE (images.session_id IS NULL OR images.session_id = '') AND p.url = images.page_url`,
		`ALTER TABLE images ALTER COLUMN session_id SET NOT NULL`,
		`ALTER TABLE images DROP CONSTRAINT IF EXISTS images_pkey`,
		`ALTER TABLE images DROP CONSTRAINT IF EXISTS images_session_page_source_pkey`,
		`ALTER TABLE images ADD CONSTRAINT images_session_page_source_pkey PRIMARY KEY (session_id, page_url, source_url)`,
		`ALTER TABLE images ADD CONSTRAINT images_page_fk FOREIGN KEY (session_id, page_url) REFERENCES pages(session_id, url) ON DELETE CASCADE`,
		`DROP INDEX IF EXISTS idx_images_page_url`,
		`CREATE INDEX IF NOT EXISTS idx_images_session_page ON images (session_id, page_url)`,
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
