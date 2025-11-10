package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	pq "github.com/lib/pq"
)

const defaultDocumentSyncBatchSize = 64

// DocumentSyncCandidate represents a page that has completed indexing and needs
// to be copied into the downstream document store.
type DocumentSyncCandidate struct {
	SessionID     string
	URL           string
	FinalURL      string
	Markdown      string
	ExtractedText string
	Metadata      map[string]string
	ContentHash   string
	IndexedAt     time.Time
}

// VectorCollectionRecord mirrors the vector_db table schema.
type VectorCollectionRecord struct {
	UserID             *int64
	CollectionMakeName string
	CollectionName     string
	Description        string
	RegisteredAt       time.Time
	VectorSize         int
	InitEmbeddingModel string
	IsShared           bool
	ShareGroup         *string
	SharePermissions   string
}

// VectorChunkRecord mirrors the vector_db_chunk_meta schema.
type VectorChunkRecord struct {
	UserID             *int64
	CollectionName     string
	FileName           string
	ChunkID            string
	ChunkText          string
	ChunkIndex         int
	TotalChunks        int
	ChunkSize          int
	Summary            string
	Keywords           []string
	Topics             []string
	Entities           []string
	Sentiment          string
	DocumentID         string
	DocumentType       string
	Language           string
	ComplexityLevel    string
	MainConcepts       []string
	EmbeddingProvider  string
	EmbeddingModelName string
	EmbeddingDimension int
}

// DocumentSyncStore handles writes to the downstream document/vector database.
type DocumentSyncStore interface {
	UpsertVectorCollection(ctx context.Context, record VectorCollectionRecord) error
	UpsertVectorChunk(ctx context.Context, record VectorChunkRecord) error
}

var _ DocumentSyncStore = (*SQLWriter)(nil)

// FetchPagesReadyForDocumentSync returns up to limit pages that have completed
// indexing but have not yet been copied into the document database.
func (s *SQLWriter) FetchPagesReadyForDocumentSync(ctx context.Context, sessionID string, limit int) ([]DocumentSyncCandidate, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sql store not initialised")
	}
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("session id is required")
	}
	if limit <= 0 || limit > 512 {
		limit = defaultDocumentSyncBatchSize
	}
	query := `
        SELECT url, final_url, markdown, extracted_text, metadata, content_hash, indexed_at
        FROM pages
        WHERE session_id = $1
          AND needs_index = FALSE
          AND indexed_at IS NOT NULL
          AND (document_integrated_at IS NULL OR document_integrated_at < indexed_at)
        ORDER BY indexed_at ASC
        LIMIT $2`
	rows, err := s.db.QueryContext(ctx, query, sessionID, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch pages for document sync: %w", err)
	}
	defer rows.Close()

	candidates := make([]DocumentSyncCandidate, 0, limit)
	for rows.Next() {
		var (
			url           string
			finalURL      sql.NullString
			markdown      sql.NullString
			extracted     sql.NullString
			metadataBytes []byte
			contentHash   sql.NullString
			indexedAt     time.Time
		)
		if err := rows.Scan(&url, &finalURL, &markdown, &extracted, &metadataBytes, &contentHash, &indexedAt); err != nil {
			return nil, fmt.Errorf("scan document sync page: %w", err)
		}
		candidate := DocumentSyncCandidate{
			SessionID:     sessionID,
			URL:           url,
			FinalURL:      finalURL.String,
			Markdown:      markdown.String,
			ExtractedText: extracted.String,
			Metadata:      parseMetadata(metadataBytes),
			ContentHash:   contentHash.String,
			IndexedAt:     indexedAt,
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

// MarkPageDocumentIntegrated records the time a page was copied into the
// downstream document database.
func (s *SQLWriter) MarkPageDocumentIntegrated(ctx context.Context, sessionID, url string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql store not initialised")
	}
	query := `
        UPDATE pages
           SET document_integrated_at = NOW()
         WHERE session_id = $1 AND url = $2`
	res, err := s.db.ExecContext(ctx, query, sessionID, url)
	if err != nil {
		return fmt.Errorf("mark page integrated: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("mark page integrated: no rows updated for %s", url)
	}
	return nil
}

// UpsertVectorCollection ensures the target collection metadata exists.
func (s *SQLWriter) UpsertVectorCollection(ctx context.Context, record VectorCollectionRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql store not initialised")
	}
	makeName := strings.TrimSpace(record.CollectionMakeName)
	if makeName == "" {
		return fmt.Errorf("collection_make_name is required")
	}
	shareGroup := nullableString(record.ShareGroup)
	updateQuery := `
        UPDATE vector_db
           SET description = $1,
               vector_size = $2,
               init_embedding_model = $3,
               is_shared = $4,
               share_group = $5,
               share_permissions = $6
         WHERE collection_make_name = $7`
	updateArgs := []any{
		record.Description,
		record.VectorSize,
		record.InitEmbeddingModel,
		record.IsShared,
		shareGroup,
		record.SharePermissions,
		makeName,
	}
	res, err := s.db.ExecContext(ctx, updateQuery, updateArgs...)
	if err != nil {
		return fmt.Errorf("update vector collection: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows > 0 {
		return nil
	}

	insertQuery := `
        INSERT INTO vector_db (
            user_id, collection_make_name, collection_name, description,
            registered_at, vector_size, init_embedding_model,
            is_shared, share_group, share_permissions
        ) VALUES (
            $1,$2,$3,$4,
            $5,$6,$7,
            $8,$9,$10
        )`
	insertArgs := []any{
		nullableInt64(record.UserID),
		makeName,
		strings.TrimSpace(record.CollectionName),
		record.Description,
		record.RegisteredAt,
		record.VectorSize,
		record.InitEmbeddingModel,
		record.IsShared,
		shareGroup,
		record.SharePermissions,
	}
	if _, err := s.db.ExecContext(ctx, insertQuery, insertArgs...); err != nil {
		return fmt.Errorf("insert vector collection: %w", err)
	}
	return nil
}

// UpsertVectorChunk writes chunk metadata for a crawled page document.
func (s *SQLWriter) UpsertVectorChunk(ctx context.Context, record VectorChunkRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql store not initialised")
	}
	query := `
        INSERT INTO vector_db_chunk_meta (
            user_id, collection_name, file_name, chunk_id, chunk_text,
            chunk_index, total_chunks, chunk_size, summary,
            keywords, topics, entities, sentiment,
            document_id, document_type, language, complexity_level,
            main_concepts, embedding_provider, embedding_model_name, embedding_dimension
        ) VALUES (
            $1,$2,$3,$4,$5,
            $6,$7,$8,$9,
            $10,$11,$12,$13,
            $14,$15,$16,$17,
            $18,$19,$20,$21
        )
        ON CONFLICT (chunk_id) DO UPDATE SET
            chunk_text = EXCLUDED.chunk_text,
            chunk_index = EXCLUDED.chunk_index,
            total_chunks = EXCLUDED.total_chunks,
            chunk_size = EXCLUDED.chunk_size,
            summary = EXCLUDED.summary,
            keywords = EXCLUDED.keywords,
            topics = EXCLUDED.topics,
            entities = EXCLUDED.entities,
            sentiment = EXCLUDED.sentiment,
            document_id = EXCLUDED.document_id,
            document_type = EXCLUDED.document_type,
            language = EXCLUDED.language,
            complexity_level = EXCLUDED.complexity_level,
            main_concepts = EXCLUDED.main_concepts,
            embedding_provider = EXCLUDED.embedding_provider,
            embedding_model_name = EXCLUDED.embedding_model_name,
            embedding_dimension = EXCLUDED.embedding_dimension`
	_, err := s.db.ExecContext(ctx, query,
		nullableInt64(record.UserID),
		strings.TrimSpace(record.CollectionName),
		record.FileName,
		record.ChunkID,
		record.ChunkText,
		record.ChunkIndex,
		record.TotalChunks,
		record.ChunkSize,
		record.Summary,
		pq.Array(record.Keywords),
		pq.Array(record.Topics),
		pq.Array(record.Entities),
		record.Sentiment,
		record.DocumentID,
		record.DocumentType,
		record.Language,
		record.ComplexityLevel,
		pq.Array(record.MainConcepts),
		record.EmbeddingProvider,
		record.EmbeddingModelName,
		record.EmbeddingDimension,
	)
	if err != nil {
		return fmt.Errorf("upsert vector chunk: %w", err)
	}
	return nil
}

func nullableInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}
