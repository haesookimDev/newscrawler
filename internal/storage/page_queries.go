package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PageListParams controls pagination and filtering.
type PageListParams struct {
	Page     int
	PageSize int
	Search   string
}

// PageSummary represents a crawled page in list view.
type PageSummary struct {
	PageID      string     `json:"page_id"`
	VectorID    string     `json:"vector_id"`
	URL         string     `json:"url"`
	FinalURL    string     `json:"final_url"`
	Title       string     `json:"title,omitempty"`
	ContentType string     `json:"content_type,omitempty"`
	StatusCode  int        `json:"status_code"`
	Depth       int        `json:"depth"`
	RetrievedAt time.Time  `json:"retrieved_at"`
	SizeBytes   int64      `json:"size_bytes,omitempty"`
	Tags        []string   `json:"tags,omitempty"`
	NeedsIndex  bool       `json:"needs_index,omitempty"`
	IndexedAt   *time.Time `json:"indexed_at,omitempty"`
	ContentHash string     `json:"content_hash"`
}

// PageListResult wraps summaries with pagination metadata.
type PageListResult struct {
	SessionID string        `json:"session_id"`
	Total     int64         `json:"total"`
	Page      int           `json:"page"`
	PageSize  int           `json:"page_size"`
	Items     []PageSummary `json:"items"`
}

// PageDetail extends summary with full content.
type PageDetail struct {
	PageSummary
	RawHTML       string            `json:"raw_html,omitempty"`
	CleanHTML     string            `json:"clean_html,omitempty"`
	ExtractedText string            `json:"extracted_text,omitempty"`
	Markdown      string            `json:"markdown,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

func (s *SQLWriter) ListPages(ctx context.Context, sessionID string, params PageListParams) (PageListResult, error) {
	if s == nil || s.db == nil {
		return PageListResult{}, fmt.Errorf("sql store not initialised")
	}
	page := params.Page
	if page <= 0 {
		page = 1
	}
	pageSize := params.PageSize
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 20
	}
	search := strings.TrimSpace(params.Search)

	result := PageListResult{
		SessionID: sessionID,
		Page:      page,
		PageSize:  pageSize,
	}

	var (
		totalQuery string
		totalArgs  []any
		listQuery  string
		listArgs   []any
	)
	if search != "" {
		pattern := "%" + search + "%"
		totalQuery = `
            SELECT COUNT(*) FROM pages
            WHERE session_id = $1
              AND (url ILIKE $2 OR final_url ILIKE $2 OR metadata->>'title' ILIKE $2)`
		totalArgs = []any{sessionID, pattern}
		listQuery = `
            SELECT url, final_url, depth, retrieved_at, status_code, metadata, content_hash, needs_index, indexed_at
            FROM pages
            WHERE session_id = $1
              AND (url ILIKE $2 OR final_url ILIKE $2 OR metadata->>'title' ILIKE $2)
            ORDER BY retrieved_at DESC
            LIMIT $3 OFFSET $4`
		listArgs = []any{sessionID, pattern, pageSize, (page - 1) * pageSize}
	} else {
		totalQuery = `SELECT COUNT(*) FROM pages WHERE session_id = $1`
		totalArgs = []any{sessionID}
		listQuery = `
            SELECT url, final_url, depth, retrieved_at, status_code, metadata, content_hash, needs_index, indexed_at
            FROM pages
            WHERE session_id = $1
            ORDER BY retrieved_at DESC
            LIMIT $2 OFFSET $3`
		listArgs = []any{sessionID, pageSize, (page - 1) * pageSize}
	}

	if err := s.db.QueryRowContext(ctx, totalQuery, totalArgs...).Scan(&result.Total); err != nil {
		return PageListResult{}, fmt.Errorf("count pages: %w", err)
	}

	rows, err := s.db.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		return PageListResult{}, fmt.Errorf("list pages: %w", err)
	}
	defer rows.Close()

	items := make([]PageSummary, 0, pageSize)
	for rows.Next() {
		var (
			url           string
			finalURL      sql.NullString
			depth         int
			retrieved     time.Time
			status        int
			metadataBytes []byte
			contentHash   sql.NullString
			needsIndex    bool
			indexedAt     sql.NullTime
		)
		if err := rows.Scan(&url, &finalURL, &depth, &retrieved, &status, &metadataBytes, &contentHash, &needsIndex, &indexedAt); err != nil {
			return PageListResult{}, fmt.Errorf("scan page: %w", err)
		}
		metadata := parseMetadata(metadataBytes)
		item := PageSummary{
			PageID:      EncodePageID(url),
			VectorID:    GeneratePointID(contentHash.String, url),
			URL:         url,
			FinalURL:    finalURL.String,
			Depth:       depth,
			RetrievedAt: retrieved,
			StatusCode:  status,
			ContentHash: contentHash.String,
			NeedsIndex:  needsIndex,
		}
		if indexedAt.Valid {
			item.IndexedAt = &indexedAt.Time
		}
		item.Title = metadata["title"]
		item.ContentType = metadata["content_type"]
		item.SizeBytes = parseSize(metadata["content_length"])
		if tags := metadata["tags"]; tags != "" {
			item.Tags = splitTags(tags)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return PageListResult{}, err
	}
	result.Items = items
	return result, nil
}

func (s *SQLWriter) GetPageByURL(ctx context.Context, sessionID, url string) (PageDetail, error) {
	if s == nil || s.db == nil {
		return PageDetail{}, fmt.Errorf("sql store not initialised")
	}
	query := `
        SELECT url, final_url, depth, retrieved_at, status_code, metadata,
               raw_html, clean_html, extracted_text, markdown,
               needs_index, indexed_at, content_hash, run_id, seed_label
        FROM pages
        WHERE session_id = $1 AND url = $2`
	row := s.db.QueryRowContext(ctx, query, sessionID, url)

	var (
		finalURL      sql.NullString
		depth         int
		retrieved     time.Time
		status        int
		metadataBytes []byte
		rawHTML       []byte
		cleanHTML     []byte
		extracted     sql.NullString
		markdown      sql.NullString
		needsIndex    bool
		indexedAt     sql.NullTime
		contentHash   sql.NullString
		runID         sql.NullString
		seedLabel     sql.NullString
	)

	if err := row.Scan(&url, &finalURL, &depth, &retrieved, &status, &metadataBytes,
		&rawHTML, &cleanHTML, &extracted, &markdown,
		&needsIndex, &indexedAt, &contentHash, &runID, &seedLabel); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PageDetail{}, err
		}
		return PageDetail{}, fmt.Errorf("fetch page: %w", err)
	}

	metadata := parseMetadata(metadataBytes)
	detail := PageDetail{
		PageSummary: PageSummary{
			PageID:      EncodePageID(url),
			VectorID:    GeneratePointID(contentHash.String, url),
			URL:         url,
			FinalURL:    finalURL.String,
			Depth:       depth,
			RetrievedAt: retrieved,
			StatusCode:  status,
			ContentHash: contentHash.String,
			NeedsIndex:  needsIndex,
		},
		RawHTML:       string(rawHTML),
		CleanHTML:     string(cleanHTML),
		ExtractedText: extracted.String,
		Markdown:      markdown.String,
		Metadata:      metadata,
	}
	if indexedAt.Valid {
		detail.IndexedAt = &indexedAt.Time
		detail.PageSummary.IndexedAt = &indexedAt.Time
	}
	detail.PageSummary.Title = metadata["title"]
	detail.PageSummary.ContentType = metadata["content_type"]
	detail.PageSummary.SizeBytes = parseSize(metadata["content_length"])
	if tags := metadata["tags"]; tags != "" {
		detail.PageSummary.Tags = splitTags(tags)
	}
	metaRun := runID.String
	if metaRun != "" {
		detail.Metadata["run_id"] = metaRun
	}
	if seedLabel.String != "" {
		detail.Metadata["seed_label"] = seedLabel.String
	}
	return detail, nil
}

func parseMetadata(data []byte) map[string]string {
	if len(data) == 0 {
		return map[string]string{}
	}
	var meta map[string]string
	if err := json.Unmarshal(data, &meta); err != nil {
		return map[string]string{}
	}
	return meta
}

func parseSize(value string) int64 {
	if value == "" {
		return 0
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		return n
	}
	return 0
}

func splitTags(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, v := range parts {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
