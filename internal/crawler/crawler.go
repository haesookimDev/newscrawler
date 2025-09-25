package crawler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"

	"newscrawler/internal/config"
	"newscrawler/internal/fetcher"
	"newscrawler/internal/processor"
	robotsclient "newscrawler/internal/robots"
	"newscrawler/internal/storage"
	"newscrawler/pkg/types"
)

// Engine orchestrates fetching, processing, and persisting crawl results.
type Engine struct {
	cfg       config.Config
	fetcher   fetcher.Fetcher
	processor processor.Processor
	robots    *robotsclient.Agent
	storage   *storage.Pipeline

	limiter   *DomainLimiter
	footprint *Footprint

	logger *slog.Logger

	allowed             map[string]struct{}
	excluded            map[string]struct{}
	allowedContentTypes map[string]struct{}

	includePatterns []*regexp.Regexp
	excludePatterns []*regexp.Regexp

	maxPages int64
	enqueued atomic.Int64

	pool *WorkerPool
	wg   sync.WaitGroup

	closers   []func() error
	closeOnce sync.Once

	respectCanonical  bool
	respectMetaRobots bool
	respectNofollow   bool
	minContentLength  int
	mediaEnabled      bool
	imageClient       *http.Client
	imageAllowedTypes map[string]struct{}
	maxImagesPerPage  int
	maxImageBytes     int64
	imageAcceptLang   string
}

// NewEngine builds a crawler engine from configuration.
func NewEngine(cfg config.Config) (*Engine, error) {
	logger, err := buildLogger(cfg.Logging)
	if err != nil {
		return nil, err
	}

	httpFetcher, err := fetcher.NewHTTPFetcher(fetcher.Options{
		UserAgent:    cfg.Crawl.UserAgent,
		Headers:      cfg.Crawl.Headers,
		Timeout:      cfg.Crawl.RequestTimeout.Duration,
		MaxBodyBytes: cfg.Crawl.MaxBodyBytes,
		ProxyURL:     cfg.Crawl.ProxyURL,
	})
	if err != nil {
		return nil, fmt.Errorf("http fetcher: %w", err)
	}

	var renderer fetcher.Renderer
	if cfg.Rendering.Enabled {
		switch strings.ToLower(cfg.Rendering.Engine) {
		case "chromedp", "chrome":
			renderer = fetcher.NewChromedpRenderer(fetcher.RenderOptions{
				Timeout:            cfg.Rendering.Timeout.Duration,
				WaitForSelector:    cfg.Rendering.WaitForSelector,
				UserAgent:          cfg.Crawl.UserAgent,
				MaxBodyBytes:       cfg.Crawl.MaxBodyBytes,
				DisableHeadless:    cfg.Rendering.DisableHeadless,
				ConcurrentSessions: cfg.Rendering.ConcurrentSessions,
			})
		case "none":
			// Explicit opt-out even if enabled flag toggled.
		default:
			return nil, fmt.Errorf("unsupported rendering engine %q", cfg.Rendering.Engine)
		}
	}

	composite := fetcher.NewComposite(httpFetcher, renderer)

	proc := processor.NewHTMLProcessor(cfg.Preprocess)
	robots := robotsclient.NewAgent(cfg.Robots, httpFetcher.Client())

	var relational storage.RelationalStore
	var closers []func() error
	if cfg.DB.Driver != "" && cfg.DB.DSN != "" {
		sqlWriter, err := storage.NewSQLWriter(cfg.DB)
		if err != nil {
			return nil, err
		}
		relational = sqlWriter
		closers = append(closers, sqlWriter.Close)
	}

	var vector storage.VectorStore
	if cfg.VectorDB.Provider != "" {
		vector = storage.NoopVectorStore{}
	}

	var media storage.MediaStore
	mediaEnabled := false
	if cfg.Media.Enabled {
		if relational == nil {
			return nil, errors.New("media storage requires sql database configuration")
		}
		store, err := storage.NewFileMediaStore(cfg.Media.Directory)
		if err != nil {
			return nil, fmt.Errorf("media store: %w", err)
		}
		media = store
		mediaEnabled = true
	}

	pipeline := storage.NewPipeline(relational, vector, media)

	footprint := NewFootprint(cfg.Crawl.Footprint.MaxEntries, cfg.Crawl.Footprint.TTL.Duration, cfg.Crawl.Footprint.Enabled)
	limiter := NewDomainLimiter(cfg.Crawl.PerDomainDelay.Duration, RateLimiterSettings{
		Requests: cfg.Crawl.RateLimitPerDomain.Requests,
		Window:   cfg.Crawl.RateLimitPerDomain.Window.Duration,
	})

	allowed := make(map[string]struct{}, len(cfg.Crawl.AllowedDomains))
	for _, v := range cfg.Crawl.AllowedDomains {
		allowed[v] = struct{}{}
	}
	excluded := make(map[string]struct{}, len(cfg.Crawl.ExcludedDomains))
	for _, v := range cfg.Crawl.ExcludedDomains {
		excluded[v] = struct{}{}
	}
	allowedTypes := make(map[string]struct{}, len(cfg.Crawl.AllowedContentTypes))
	for _, ct := range cfg.Crawl.AllowedContentTypes {
		ct = strings.TrimSpace(strings.ToLower(ct))
		if ct == "" {
			continue
		}
		allowedTypes[ct] = struct{}{}
	}
	imageTypes := make(map[string]struct{}, len(cfg.Media.AllowedContentTypes))
	if mediaEnabled {
		for _, ct := range cfg.Media.AllowedContentTypes {
			ct = strings.TrimSpace(strings.ToLower(ct))
			if ct == "" {
				continue
			}
			imageTypes[ct] = struct{}{}
		}
	}

	include, err := compilePatterns(cfg.Crawl.Discovery.IncludePatterns)
	if err != nil {
		return nil, fmt.Errorf("invalid include pattern: %w", err)
	}
	exclude, err := compilePatterns(cfg.Crawl.Discovery.ExcludePatterns)
	if err != nil {
		return nil, fmt.Errorf("invalid exclude pattern: %w", err)
	}

	maxPages := int64(cfg.Crawl.MaxPages)
	if maxPages <= 0 {
		maxPages = math.MaxInt64
	}

	return &Engine{
		cfg:                 cfg,
		fetcher:             composite,
		processor:           proc,
		robots:              robots,
		storage:             pipeline,
		limiter:             limiter,
		footprint:           footprint,
		logger:              logger,
		allowed:             allowed,
		excluded:            excluded,
		allowedContentTypes: allowedTypes,
		includePatterns:     include,
		excludePatterns:     exclude,
		maxPages:            maxPages,
		closers:             closers,
		respectCanonical:    cfg.Crawl.RespectCanonical,
		respectMetaRobots:   cfg.Crawl.RespectMetaRobots,
		respectNofollow:     cfg.Crawl.Discovery.RespectNofollow,
		minContentLength:    cfg.Crawl.Discovery.MinContentLength,
		mediaEnabled:        mediaEnabled,
		imageClient:         httpFetcher.Client(),
		imageAllowedTypes:   imageTypes,
		maxImagesPerPage:    cfg.Media.MaxPerPage,
		maxImageBytes:       cfg.Media.MaxSizeBytes,
		imageAcceptLang:     cfg.Crawl.Headers["Accept-Language"],
	}, nil
}

// Run executes the crawl until completion or context cancellation.
func (e *Engine) Run(ctx context.Context) error {
	pool, err := NewWorkerPool(ctx, e.cfg.Worker.Concurrency, e.cfg.Worker.QueueSize)
	if err != nil {
		return err
	}
	e.pool = pool
	defer pool.Close()
	defer e.Close()
	e.logger.Info("crawler engine started",
		"concurrency", e.cfg.Worker.Concurrency,
		"queue", e.cfg.Worker.QueueSize,
		"max_depth", e.cfg.Crawl.MaxDepth,
		"max_pages", e.maxPages,
	)
	defer e.logger.Info("crawler engine stopped")

	seeds, err := e.buildSeedRequests()
	if err != nil {
		return err
	}

	for _, req := range seeds {
		e.logger.Info("enqueue seed", "url", req.URL.String(), "max_depth", req.MaxDepth)
		e.enqueue(ctx, req)
	}

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		e.logger.Warn("context cancelled, shutting down")
		<-done
		return ctx.Err()
	case <-done:
		return nil
	}
}

// Close releases resources owned by the engine.
func (e *Engine) Close() error {
	var err error
	e.closeOnce.Do(func() {
		for _, closer := range e.closers {
			if cerr := closer(); cerr != nil {
				if err == nil {
					err = cerr
				} else {
					err = errors.Join(err, cerr)
				}
			}
		}
	})
	return err
}

func (e *Engine) enqueue(ctx context.Context, req types.CrawlRequest) {
	if req.URL == nil {
		return
	}
	if req.Depth > req.MaxDepth {
		return
	}
	if !e.shouldVisit(req) {
		return
	}
	if e.enqueued.Load() >= e.maxPages {
		return
	}
	if e.enqueued.Add(1) > e.maxPages {
		e.enqueued.Add(-1)
		return
	}

	e.footprint.MarkVisited(req.URL, req.Depth, req.Depth >= req.MaxDepth)

	req.EnqueuedAt = time.Now()
	e.wg.Add(1)
	if err := e.pool.Submit(ctx, func(workerCtx context.Context) {
		defer e.wg.Done()
		e.handleRequest(workerCtx, req)
	}); err != nil {
		e.wg.Done()
		e.enqueued.Add(-1)
		e.logger.Error("enqueue failed", "url", req.URL.String(), "error", err)
	}
}

func (e *Engine) handleRequest(ctx context.Context, req types.CrawlRequest) {
	if ctx.Err() != nil || req.URL == nil {
		return
	}

	defer e.footprint.MarkExplored(req.URL, req.Depth)
	logger := e.logger.With("url", req.URL.String(), "depth", req.Depth)
	logger.Debug("processing request")

	if !e.robots.Allowed(ctx, req.URL) {
		logger.Debug("blocked by robots")
		return
	}

	if err := e.limiter.Wait(ctx, req.URL.Hostname()); err != nil {
		logger.Warn("domain limiter interrupted", "error", err)
		return
	}

	page, err := e.fetchWithRetry(ctx, req)
	if err != nil {
		logger.Warn("fetch failed", "error", err)
		return
	}

	if !e.isContentTypeAllowed(page.ContentType) {
		logger.Debug("skipping unsupported content type", "content_type", page.ContentType)
		return
	}

	metadata := map[string]string{
		"content_length": strconv.Itoa(len(page.Body)),
		"status_code":    strconv.Itoa(page.StatusCode),
	}
	if ct := normaliseContentType(page.ContentType); ct != "" {
		metadata["content_type"] = ct
	}
	if page.ResponseLatency > 0 {
		metadata["response_time_ms"] = strconv.FormatInt(page.ResponseLatency.Milliseconds(), 10)
	}

	result := types.CrawlResult{Request: req, Page: page, Metadata: metadata}

	if e.processor != nil {
		cleaned, err := e.processor.Process(ctx, page)
		if err != nil {
			logger.Debug("processor error", "error", err)
		} else {
			result.Preprocessed = cleaned
		}
	}

	doc, directives, err := e.analyseDocument(page)
	if err != nil {
		logger.Debug("document analysis failed", "error", err)
	}
	if directives.Title != "" {
		metadata["title"] = directives.Title
	}
	if directives.Description != "" {
		metadata["description"] = directives.Description
	}
	if directives.Language != "" {
		metadata["language"] = directives.Language
	}
	if directives.MetaRobots != "" {
		metadata["meta_robots"] = directives.MetaRobots
	}
	if directives.Noindex {
		metadata["robots:noindex"] = "true"
	}
	if directives.Nofollow {
		metadata["robots:nofollow"] = "true"
	}
	if directives.Canonical != nil {
		metadata["canonical_url"] = directives.Canonical.String()
	}
	if e.respectCanonical && directives.Canonical != nil && e.acceptLink(req.URL, directives.Canonical) {
		page.FinalURL = directives.Canonical
	}

	followLinks := true
	if e.respectNofollow && directives.Nofollow {
		followLinks = false
	}

	if e.minContentLength > 0 && len(page.Body) < e.minContentLength {
		metadata["thin_content"] = "true"
		followLinks = false
	}

	var baseURL *url.URL
	if directives.Base != nil {
		baseURL = directives.Base
	} else if page.FinalURL != nil {
		baseURL = page.FinalURL
	} else {
		baseURL = page.URL
	}

	var images []types.ImageAsset
	if e.mediaEnabled && doc != nil && baseURL != nil && e.maxImagesPerPage != 0 {
		images = e.collectImages(ctx, doc, baseURL)
		if len(images) > 0 {
			metadata["image_count"] = strconv.Itoa(len(images))
			logger.Debug("images captured", "count", len(images))
		}
	}

	var links []*url.URL
	if followLinks && doc != nil && baseURL != nil {
		links = e.extractLinks(doc, baseURL)
	}
	result.Links = links
	result.Images = images

	shouldPersist := true
	if e.respectMetaRobots && directives.Noindex {
		shouldPersist = false
	}

	if shouldPersist && e.storage != nil {
		if err := e.storage.Persist(ctx, result); err != nil {
			logger.Error("persist failed", "error", err)
		}
	} else if !shouldPersist && e.storage != nil {
		logger.Debug("skipping persistence due to noindex")
	}

	logger.Info("page processed",
		"status", page.StatusCode,
		"rendered", page.Rendered,
		"latency_ms", page.ResponseLatency.Milliseconds(),
		"links", len(links),
	)

	if req.Depth >= req.MaxDepth {
		return
	}

	if len(links) == 0 {
		return
	}

	parent := page.FinalURL
	if parent == nil {
		parent = req.URL
	}

	for _, link := range links {
		child := types.CrawlRequest{
			URL:       link,
			Depth:     req.Depth + 1,
			Parent:    parent,
			Render:    e.cfg.Rendering.Enabled,
			SeedLabel: req.SeedLabel,
			MaxDepth:  req.MaxDepth,
		}
		e.enqueue(ctx, child)
	}
}

func (e *Engine) shouldVisit(req types.CrawlRequest) bool {
	if req.URL == nil {
		return false
	}
	scheme := strings.ToLower(req.URL.Scheme)
	if scheme != "http" && scheme != "https" {
		return false
	}
	host := strings.ToLower(req.URL.Hostname())

	if len(e.allowed) > 0 {
		if _, ok := e.allowed[host]; !ok {
			return false
		}
	}
	if _, denied := e.excluded[host]; denied {
		return false
	}

	if !e.cfg.Crawl.Discovery.FollowExternal {
		if req.Parent != nil {
			if !sameDomain(req.Parent, req.URL) {
				return false
			}
		}
	}

	if !e.footprint.ShouldVisit(req.URL, req.Depth, req.MaxDepth) {
		return false
	}
	return true
}

func (e *Engine) extractLinks(doc *goquery.Document, base *url.URL) []*url.URL {
	if doc == nil || base == nil {
		return nil
	}

	maxLinks := e.cfg.Crawl.Discovery.MaxLinksPerPage
	if maxLinks <= 0 {
		maxLinks = 200
	}
	seen := make(map[string]struct{})
	links := make([]*url.URL, 0, maxLinks)

	doc.Find("a[href]").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		href, ok := s.Attr("href")
		if !ok {
			return true
		}
		href = strings.TrimSpace(href)
		if href == "" || href == "#" {
			return true
		}
		lower := strings.ToLower(href)
		if strings.HasPrefix(lower, "javascript:") || strings.HasPrefix(lower, "mailto:") || strings.HasPrefix(lower, "tel:") || strings.HasPrefix(lower, "data:") {
			return true
		}
		if idx := strings.IndexRune(href, '#'); idx == 0 {
			return true
		} else if idx > 0 {
			href = href[:idx]
		}

		if e.respectNofollow {
			if rel, ok := s.Attr("rel"); ok && hasDirective(rel, "nofollow") {
				return true
			}
		}

		target, err := base.Parse(href)
		if err != nil || target == nil {
			return true
		}
		target.Fragment = ""
		if !e.acceptLink(base, target) {
			return true
		}
		key := target.String()
		if _, exists := seen[key]; exists {
			return true
		}
		seen[key] = struct{}{}
		links = append(links, target)
		if len(links) >= maxLinks {
			return false
		}
		return true
	})

	return links
}

func (e *Engine) collectImages(ctx context.Context, doc *goquery.Document, base *url.URL) []types.ImageAsset {
	if doc == nil || base == nil || e.imageClient == nil {
		return nil
	}
	if e.maxImagesPerPage == 0 {
		return nil
	}
	max := e.maxImagesPerPage
	assets := make([]types.ImageAsset, 0, max)
	seen := make(map[string]struct{})

	doc.Find("img[src]").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		if max > 0 && len(assets) >= max {
			return false
		}
		if ctx != nil && ctx.Err() != nil {
			return false
		}
		src, ok := s.Attr("src")
		if !ok {
			return true
		}
		src = strings.TrimSpace(src)
		if src == "" {
			return true
		}
		lower := strings.ToLower(src)
		if strings.HasPrefix(lower, "data:") {
			return true
		}
		target, err := base.Parse(src)
		if err != nil || target == nil {
			return true
		}
		if scheme := strings.ToLower(target.Scheme); scheme != "http" && scheme != "https" {
			return true
		}
		target.Fragment = ""
		resolved := target.String()
		if _, dup := seen[resolved]; dup {
			return true
		}
		seen[resolved] = struct{}{}

		alt, _ := s.Attr("alt")
		asset, err := e.fetchImageAsset(ctx, target, alt)
		if err != nil {
			e.logger.Debug("image fetch failed", "image", resolved, "error", err)
			return true
		}
		if asset != nil {
			assets = append(assets, *asset)
		}
		return true
	})

	return assets
}

func (e *Engine) fetchImageAsset(ctx context.Context, target *url.URL, alt string) (*types.ImageAsset, error) {
	if target == nil || e.imageClient == nil {
		return nil, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build image request: %w", err)
	}
	req.Header.Set("User-Agent", e.cfg.Crawl.UserAgent)
	req.Header.Set("Accept", "image/avif,image/webp,image/apng,image/*,*/*;q=0.8")
	if lang := strings.TrimSpace(e.imageAcceptLang); lang != "" {
		req.Header.Set("Accept-Language", lang)
	}

	resp, err := e.imageClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch image: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("image status %d", resp.StatusCode)
	}

	contentType := normaliseContentType(resp.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = inferImageContentType(target)
	}

	if contentType != "" && !e.isAllowedImageType(contentType) {
		return nil, nil
	}

	maxBytes := e.maxImageBytes
	if maxBytes <= 0 {
		maxBytes = 2 * 1024 * 1024
	}
	if resp.ContentLength > 0 && maxBytes > 0 && resp.ContentLength > maxBytes {
		return nil, fmt.Errorf("image exceeds max size (%d > %d)", resp.ContentLength, maxBytes)
	}

	limited := io.LimitReader(resp.Body, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read image: %w", err)
	}
	if maxBytes > 0 && int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("image exceeds max size (%d > %d)", len(data), maxBytes)
	}

	if contentType == "" {
		contentType = normaliseContentType(http.DetectContentType(data))
	}
	if !e.isAllowedImageType(contentType) {
		return nil, nil
	}

	width, height := 0, 0
	if cfg, _, err := image.DecodeConfig(bytes.NewReader(data)); err == nil {
		width = cfg.Width
		height = cfg.Height
	}

	asset := &types.ImageAsset{
		SourceURL:   target.String(),
		AltText:     strings.TrimSpace(alt),
		ContentType: contentType,
		Data:        data,
		SizeBytes:   int64(len(data)),
		Width:       width,
		Height:      height,
	}
	return asset, nil
}

func (e *Engine) isAllowedImageType(ct string) bool {
	if len(e.imageAllowedTypes) == 0 {
		return true
	}
	norm := normaliseContentType(ct)
	if norm == "" {
		return false
	}
	_, ok := e.imageAllowedTypes[norm]
	return ok
}

func inferImageContentType(u *url.URL) string {
	if u == nil {
		return ""
	}
	if ext := strings.ToLower(path.Ext(u.Path)); ext != "" {
		switch ext {
		case ".jpg", ".jpeg":
			return "image/jpeg"
		case ".png":
			return "image/png"
		case ".gif":
			return "image/gif"
		case ".webp":
			return "image/webp"
		case ".bmp":
			return "image/bmp"
		case ".svg":
			return "image/svg+xml"
		}
	}
	return ""
}

func (e *Engine) acceptLink(base, target *url.URL) bool {
	if target == nil {
		return false
	}
	scheme := strings.ToLower(target.Scheme)
	if scheme != "http" && scheme != "https" {
		return false
	}

	host := strings.ToLower(target.Hostname())

	if len(e.allowed) > 0 {
		if _, ok := e.allowed[host]; !ok {
			return false
		}
	}
	if _, denied := e.excluded[host]; denied {
		return false
	}

	if !e.cfg.Crawl.Discovery.FollowExternal && base != nil {
		if !sameDomain(base, target) {
			return false
		}
	}

	if len(e.includePatterns) > 0 {
		matched := false
		for _, pat := range e.includePatterns {
			if pat.MatchString(target.String()) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, pat := range e.excludePatterns {
		if pat.MatchString(target.String()) {
			return false
		}
	}

	return true
}

func (e *Engine) buildSeedRequests() ([]types.CrawlRequest, error) {
	maxDepth := e.cfg.Crawl.MaxDepth
	seeds := make([]types.CrawlRequest, 0, len(e.cfg.Crawl.Seeds))
	for _, seed := range e.cfg.Crawl.Seeds {
		parsed, err := url.Parse(seed.URL)
		if err != nil {
			return nil, fmt.Errorf("parse seed %q: %w", seed.URL, err)
		}
		if parsed.Scheme == "" {
			parsed.Scheme = "https"
		}
		if parsed.Host == "" {
			return nil, fmt.Errorf("seed %q missing host", seed.URL)
		}

		depthLimit := maxDepth
		if seed.MaxDepth > 0 && seed.MaxDepth < depthLimit {
			depthLimit = seed.MaxDepth
		}

		seeds = append(seeds, types.CrawlRequest{
			URL:       parsed,
			Depth:     0,
			Parent:    nil,
			Render:    e.cfg.Rendering.Enabled,
			SeedLabel: parsed.Hostname(),
			MaxDepth:  depthLimit,
		})
	}
	return seeds, nil
}

func compilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, raw := range patterns {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		pat, err := regexp.Compile(raw)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, pat)
	}
	return compiled, nil
}

func sameDomain(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(a.Hostname(), b.Hostname())
}

func (e *Engine) fetchWithRetry(ctx context.Context, req types.CrawlRequest) (*types.Page, error) {
	attempts := e.cfg.Worker.MaxRetries + 1
	if attempts <= 0 {
		attempts = 1
	}
	backoff := e.cfg.Worker.RetryBackoff.Duration
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	const maxBackoff = 30 * time.Second

	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		e.logger.Debug("fetch attempt", "url", req.URL.String(), "attempt", attempt+1)
		page, err := e.fetcher.Fetch(ctx, req)
		if err == nil {
			if page != nil && shouldRetryStatus(page.StatusCode) && attempt < attempts-1 {
				lastErr = fmt.Errorf("retryable status %d", page.StatusCode)
			} else {
				return page, err
			}
		} else {
			lastErr = err
		}

		if attempt < attempts-1 {
			delay := backoff * time.Duration(1<<attempt)
			if delay > maxBackoff {
				delay = maxBackoff
			}
			e.logger.Debug("backing off", "url", req.URL.String(), "delay", delay)
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			}
			timer.Stop()
		}
	}
	e.logger.Warn("exhausted retries", "url", req.URL.String(), "error", lastErr)
	return nil, lastErr
}

func shouldRetryStatus(code int) bool {
	if code == 0 {
		return true
	}
	if code == 429 {
		return true
	}
	return code >= 500 && code < 600
}

func (e *Engine) isContentTypeAllowed(ct string) bool {
	if len(e.allowedContentTypes) == 0 {
		return true
	}
	norm := normaliseContentType(ct)
	if norm == "" {
		return false
	}
	_, ok := e.allowedContentTypes[norm]
	return ok
}

func normaliseContentType(ct string) string {
	if ct == "" {
		return ""
	}
	parts := strings.Split(ct, ";")
	main := strings.TrimSpace(strings.ToLower(parts[0]))
	return main
}

type pageDirectives struct {
	Base        *url.URL
	Canonical   *url.URL
	Title       string
	Description string
	Language    string
	MetaRobots  string
	Noindex     bool
	Nofollow    bool
}

func (e *Engine) analyseDocument(page *types.Page) (*goquery.Document, pageDirectives, error) {
	directives := pageDirectives{}
	if page == nil || len(page.Body) == 0 {
		return nil, directives, fmt.Errorf("page body empty")
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page.Body))
	if err != nil {
		return nil, directives, fmt.Errorf("parse html: %w", err)
	}

	directives.Title = strings.TrimSpace(doc.Find("title").First().Text())
	if desc, ok := doc.Find("meta[name='description']").Attr("content"); ok {
		directives.Description = strings.TrimSpace(desc)
	}

	if html := doc.Find("html").First(); html.Length() > 0 {
		if lang, ok := html.Attr("lang"); ok {
			directives.Language = strings.TrimSpace(lang)
		}
	}
	if directives.Language == "" {
		if lang, ok := doc.Find("meta[http-equiv='content-language']").Attr("content"); ok {
			directives.Language = strings.TrimSpace(lang)
		}
	}
	if directives.Language == "" {
		if lang, ok := doc.Find("meta[name='language']").Attr("content"); ok {
			directives.Language = strings.TrimSpace(lang)
		}
	}

	baseRef := page.FinalURL
	if baseRef == nil {
		baseRef = page.URL
	}
	if href, ok := doc.Find("base[href]").First().Attr("href"); ok {
		if resolved := resolveURL(baseRef, strings.TrimSpace(href)); resolved != nil {
			directives.Base = resolved
		}
	}
	if directives.Base == nil {
		directives.Base = baseRef
	}

	if canonicalHref, ok := doc.Find("link[rel='canonical']").First().Attr("href"); ok {
		if resolved := resolveURL(directives.Base, strings.TrimSpace(canonicalHref)); resolved != nil {
			directives.Canonical = resolved
		}
	}

	robots := collectMetaRobots(doc)
	if len(robots) > 0 {
		directives.MetaRobots = strings.Join(robots, ",")
	}
	directives.Noindex = containsDirective(robots, "noindex") || containsDirective(robots, "none")
	directives.Nofollow = containsDirective(robots, "nofollow") || containsDirective(robots, "none")

	return doc, directives, nil
}

func resolveURL(base *url.URL, href string) *url.URL {
	if strings.TrimSpace(href) == "" {
		return nil
	}
	if base == nil {
		u, err := url.Parse(href)
		if err != nil {
			return nil
		}
		u.Fragment = ""
		return u
	}
	u, err := base.Parse(href)
	if err != nil {
		return nil
	}
	u.Fragment = ""
	return u
}

func collectMetaRobots(doc *goquery.Document) []string {
	if doc == nil {
		return nil
	}
	directives := make(map[string]struct{})
	doc.Find("meta[name]").Each(func(_ int, s *goquery.Selection) {
		name, ok := s.Attr("name")
		if !ok {
			return
		}
		if !strings.EqualFold(name, "robots") {
			return
		}
		content, ok := s.Attr("content")
		if !ok {
			return
		}
		for _, token := range splitDirectives(content) {
			directives[token] = struct{}{}
		}
	})
	if len(directives) == 0 {
		return nil
	}
	result := make([]string, 0, len(directives))
	for token := range directives {
		result = append(result, token)
	}
	sort.Strings(result)
	return result
}

func splitDirectives(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		if r == ',' || r == ';' {
			return true
		}
		return unicode.IsSpace(r)
	})
	if len(fields) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(fields))
	result := make([]string, 0, len(fields))
	for _, field := range fields {
		norm := strings.TrimSpace(strings.ToLower(field))
		if norm == "" {
			continue
		}
		if _, ok := seen[norm]; ok {
			continue
		}
		seen[norm] = struct{}{}
		result = append(result, norm)
	}
	return result
}

func containsDirective(tokens []string, target string) bool {
	for _, token := range tokens {
		if token == target {
			return true
		}
	}
	return false
}

func hasDirective(raw string, directive string) bool {
	for _, token := range splitDirectives(raw) {
		if token == directive {
			return true
		}
	}
	return false
}

func buildLogger(cfg config.LoggingConfig) (*slog.Logger, error) {
	level := slog.LevelInfo
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "info", "":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", cfg.Level)
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if cfg.Structured {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler), nil
}
