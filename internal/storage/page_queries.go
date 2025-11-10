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
	SessionID   string     `json:"session_id,omitempty"`
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
	Total    int64         `json:"total"`
	Page     int           `json:"page"`
	PageSize int           `json:"page_size"`
	Items    []PageSummary `json:"items"`
}

// GlobalPageListResult is used when listing pages across all sessions.
type GlobalPageListResult struct {
	Total    int64         `json:"total"`
	Page     int           `json:"page"`
	PageSize int           `json:"page_size"`
	Items    []PageSummary `json:"items"`
}

// SessionPageOverview summarises crawl results per session.
type SessionPageOverview struct {
	SessionID     string       `json:"session_id"`
	PageCount     int64        `json:"page_count"`
	LastCrawledAt time.Time    `json:"last_crawled_at"`
	RootPage      *PageSummary `json:"root_page,omitempty"`
}

// SessionPageOverviewResult wraps paginated session summaries.
type SessionPageOverviewResult struct {
	Total    int64                 `json:"total"`
	Page     int                   `json:"page"`
	PageSize int                   `json:"page_size"`
	Items    []SessionPageOverview `json:"items"`
}

// IndexCandidate holds minimal data required for embedding/indexing.
type IndexCandidate struct {
	URL           string
	FinalURL      string
	Markdown      string
	ExtractedText string
	Metadata      map[string]string
	SessionID     string
	ContentHash   string
}

// PageDetail extends summary with full content.
type PageDetail struct {
	PageSummary
	// RawHTML       string            `json:"raw_html,omitempty"`
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
		Page:     page,
		PageSize: pageSize,
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
            SELECT url, final_url, depth, retrieved_at, status_code, metadata, content_hash, needs_index, indexed_at, session_id
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
            SELECT url, final_url, depth, retrieved_at, status_code, metadata, content_hash, needs_index, indexed_at, session_id
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
			rowSession    sql.NullString
		)
		if err := rows.Scan(&url, &finalURL, &depth, &retrieved, &status, &metadataBytes, &contentHash, &needsIndex, &indexedAt, &rowSession); err != nil {
			return PageListResult{}, fmt.Errorf("scan page: %w", err)
		}
		metadata := parseMetadata(metadataBytes)
		item := PageSummary{
			SessionID:   rowSession.String,
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
			SessionID:   sessionID,
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
		// RawHTML:       string(rawHTML),
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

func (s *SQLWriter) ListAllPages(ctx context.Context, params PageListParams, sessionID string) (GlobalPageListResult, error) {
	if s == nil || s.db == nil {
		return GlobalPageListResult{}, fmt.Errorf("sql store not initialised")
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

	result := GlobalPageListResult{
		Page:     page,
		PageSize: pageSize,
	}

	var (
		whereClauses []string
		argsTotal    []any
		argsList     []any
		argIndex     = 1
	)

	if sessionID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("session_id = $%d", argIndex))
		argsTotal = append(argsTotal, sessionID)
		argsList = append(argsList, sessionID)
		argIndex++
	}
	if search != "" {
		pattern := "%" + search + "%"
		whereClauses = append(whereClauses, fmt.Sprintf("(url ILIKE $%d OR final_url ILIKE $%d OR metadata->>'title' ILIKE $%d)", argIndex, argIndex, argIndex))
		argsTotal = append(argsTotal, pattern)
		argsList = append(argsList, pattern)
		argIndex++
	}

	whereSQL := ""
	if len(whereClauses) > 0 {
		whereSQL = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	totalQuery := fmt.Sprintf("SELECT COUNT(*) FROM pages %s", whereSQL)
	if err := s.db.QueryRowContext(ctx, totalQuery, argsTotal...).Scan(&result.Total); err != nil {
		return GlobalPageListResult{}, fmt.Errorf("count pages: %w", err)
	}

	listQuery := fmt.Sprintf(`
        SELECT session_id, url, final_url, depth, retrieved_at, status_code, metadata,
               content_hash, needs_index, indexed_at
        FROM pages
        %s
        ORDER BY retrieved_at DESC
        LIMIT $%d OFFSET $%d`, whereSQL, argIndex, argIndex+1)

	argsList = append(argsList, pageSize, (page-1)*pageSize)

	rows, err := s.db.QueryContext(ctx, listQuery, argsList...)
	if err != nil {
		return GlobalPageListResult{}, fmt.Errorf("list pages: %w", err)
	}
	defer rows.Close()

	items := make([]PageSummary, 0, pageSize)
	for rows.Next() {
		var (
			session       sql.NullString
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
		if err := rows.Scan(&session, &url, &finalURL, &depth, &retrieved, &status, &metadataBytes, &contentHash, &needsIndex, &indexedAt); err != nil {
			return GlobalPageListResult{}, fmt.Errorf("scan page: %w", err)
		}
		metadata := parseMetadata(metadataBytes)
		item := PageSummary{
			SessionID:   session.String,
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
		return GlobalPageListResult{}, err
	}
	result.Items = items
	return result, nil
}

// CountPagesNeedingIndex returns the number of pages in a session awaiting vector indexing.
func (s *SQLWriter) CountPagesNeedingIndex(ctx context.Context, sessionID string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("sql store not initialised")
	}
	var total int64
	query := `SELECT COUNT(*) FROM pages WHERE session_id = $1 AND needs_index = TRUE`
	if err := s.db.QueryRowContext(ctx, query, sessionID).Scan(&total); err != nil {
		return 0, fmt.Errorf("count needs index: %w", err)
	}
	return total, nil
}

// FetchPagesNeedingIndex retrieves a batch of pages that require indexing.
func (s *SQLWriter) FetchPagesNeedingIndex(ctx context.Context, sessionID string, limit int) ([]IndexCandidate, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("sql store not initialised")
	}
	if limit <= 0 {
		limit = 20
	}
	query := `
		SELECT url, final_url, markdown, extracted_text, metadata, content_hash
		FROM pages
		WHERE session_id = $1 AND needs_index = TRUE
		ORDER BY retrieved_at ASC
		LIMIT $2`
	rows, err := s.db.QueryContext(ctx, query, sessionID, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch needs index: %w", err)
	}
	defer rows.Close()

	candidates := make([]IndexCandidate, 0, limit)
	for rows.Next() {
		var (
			url           string
			finalURL      sql.NullString
			markdown      sql.NullString
			extracted     sql.NullString
			metadataBytes []byte
			contentHash   sql.NullString
		)
		if err := rows.Scan(&url, &finalURL, &markdown, &extracted, &metadataBytes, &contentHash); err != nil {
			return nil, fmt.Errorf("scan needs index: %w", err)
		}
		candidate := IndexCandidate{
			URL:           url,
			FinalURL:      finalURL.String,
			Markdown:      markdown.String,
			ExtractedText: extracted.String,
			Metadata:      parseMetadata(metadataBytes),
			SessionID:     sessionID,
			ContentHash:   contentHash.String,
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

// MarkPageIndexed flips needs_index to false for a page once indexing completes.
func (s *SQLWriter) MarkPageIndexed(ctx context.Context, sessionID, url string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql store not initialised")
	}
	query := `
		UPDATE pages
		SET needs_index = FALSE,
		    indexed_at = NOW()
		WHERE session_id = $1 AND url = $2`
	if _, err := s.db.ExecContext(ctx, query, sessionID, url); err != nil {
		return fmt.Errorf("mark indexed: %w", err)
	}
	return nil
}

// MarkPageNeedsIndex flags a page for manual reindexing when automatic indexing fails.
func (s *SQLWriter) MarkPageNeedsIndex(ctx context.Context, sessionID, url string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql store not initialised")
	}
	query := `
		UPDATE pages
		SET needs_index = TRUE,
		    indexed_at = NULL
		WHERE session_id = $1 AND url = $2`
	if _, err := s.db.ExecContext(ctx, query, sessionID, url); err != nil {
		return fmt.Errorf("mark needs index: %w", err)
	}
	return nil
}

// DeleteSessionData removes all persisted rows for a given session.
func (s *SQLWriter) DeleteSessionData(ctx context.Context, sessionID string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sql store not initialised")
	}
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("missing session id")
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM pages WHERE session_id = $1`, sessionID); err != nil {
		return fmt.Errorf("delete session pages: %w", err)
	}
	return nil
}

// ListSessionPageOverviews aggregates per-session stats and root page metadata.
func (s *SQLWriter) ListSessionPageOverviews(ctx context.Context, params PageListParams) (SessionPageOverviewResult, error) {
	if s == nil || s.db == nil {
		return SessionPageOverviewResult{}, fmt.Errorf("sql store not initialised")
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

	result := SessionPageOverviewResult{
		Page:     page,
		PageSize: pageSize,
	}

	var (
		whereClause string
		args        []any
	)
	if search != "" {
		whereClause = "WHERE session_id ILIKE $1"
		args = append(args, "%"+search+"%")
	}

	totalQuery := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT session_id FROM pages %s GROUP BY session_id) t", whereClause)
	if err := s.db.QueryRowContext(ctx, totalQuery, args...).Scan(&result.Total); err != nil {
		return SessionPageOverviewResult{}, fmt.Errorf("count session pages: %w", err)
	}

	listQuery := fmt.Sprintf(`
        WITH counts AS (
            SELECT session_id, COUNT(*) AS page_count, MAX(retrieved_at) AS last_crawled
            FROM pages
            %s
            GROUP BY session_id
        ),
        roots AS (
            SELECT DISTINCT ON (session_id)
                session_id, url, final_url, depth, retrieved_at, status_code,
                metadata, content_hash, needs_index, indexed_at
            FROM pages
            WHERE depth = 0
            ORDER BY session_id, retrieved_at DESC
        )
        SELECT c.session_id, c.page_count, c.last_crawled,
               r.url, r.final_url, r.depth, r.retrieved_at AS root_retrieved,
               r.status_code, r.metadata, r.content_hash, r.needs_index, r.indexed_at
        FROM counts c
        LEFT JOIN roots r ON c.session_id = r.session_id
        ORDER BY c.session_id ASC
        LIMIT $%d OFFSET $%d
    `, whereClause, len(args)+1, len(args)+2)

	listArgs := append(args, pageSize, (page-1)*pageSize)

	rows, err := s.db.QueryContext(ctx, listQuery, listArgs...)
	if err != nil {
		return SessionPageOverviewResult{}, fmt.Errorf("list session pages: %w", err)
	}
	defer rows.Close()

	items := make([]SessionPageOverview, 0, pageSize)
	for rows.Next() {
		var (
			sessionID     string
			pageCount     int64
			lastCrawled   time.Time
			rootURL       sql.NullString
			rootFinal     sql.NullString
			rootDepth     sql.NullInt64
			rootRetrieved sql.NullTime
			rootStatus    sql.NullInt64
			rootMetadata  []byte
			rootHash      sql.NullString
			rootNeeds     bool
			rootIndexed   sql.NullTime
		)
		if err := rows.Scan(&sessionID, &pageCount, &lastCrawled,
			&rootURL, &rootFinal, &rootDepth, &rootRetrieved,
			&rootStatus, &rootMetadata, &rootHash, &rootNeeds, &rootIndexed); err != nil {
			return SessionPageOverviewResult{}, fmt.Errorf("scan session page row: %w", err)
		}
		overview := SessionPageOverview{
			SessionID:     sessionID,
			PageCount:     pageCount,
			LastCrawledAt: lastCrawled,
		}
		if rootURL.Valid {
			meta := parseMetadata(rootMetadata)
			root := PageSummary{
				SessionID:   sessionID,
				PageID:      EncodePageID(rootURL.String),
				VectorID:    GeneratePointID(rootHash.String, rootURL.String),
				URL:         rootURL.String,
				FinalURL:    rootFinal.String,
				Depth:       int(rootDepth.Int64),
				RetrievedAt: rootRetrieved.Time,
				StatusCode:  int(rootStatus.Int64),
				ContentHash: rootHash.String,
				NeedsIndex:  rootNeeds,
			}
			if rootIndexed.Valid {
				root.IndexedAt = &rootIndexed.Time
			}
			root.Title = meta["title"]
			root.ContentType = meta["content_type"]
			root.SizeBytes = parseSize(meta["content_length"])
			if tags := meta["tags"]; tags != "" {
				root.Tags = splitTags(tags)
			}
			overview.RootPage = &root
		}
		items = append(items, overview)
	}
	if err := rows.Err(); err != nil {
		return SessionPageOverviewResult{}, err
	}
	result.Items = items
	return result, nil
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
