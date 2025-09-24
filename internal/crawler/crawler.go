package crawler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

	allowed  map[string]struct{}
	excluded map[string]struct{}

	includePatterns []*regexp.Regexp
	excludePatterns []*regexp.Regexp

	maxPages int64
	enqueued atomic.Int64

	pool *WorkerPool
	wg   sync.WaitGroup

	closers   []func() error
	closeOnce sync.Once
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
		MaxBodyBytes: 6 * 1024 * 1024,
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
				MaxBodyBytes:       6 * 1024 * 1024,
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
	pipeline := storage.NewPipeline(relational, vector)

	footprint := NewFootprint(cfg.Crawl.Footprint.MaxEntries)
	limiter := NewDomainLimiter(cfg.Crawl.PerDomainDelay.Duration)

	allowed := make(map[string]struct{}, len(cfg.Crawl.AllowedDomains))
	for _, v := range cfg.Crawl.AllowedDomains {
		allowed[v] = struct{}{}
	}
	excluded := make(map[string]struct{}, len(cfg.Crawl.ExcludedDomains))
	for _, v := range cfg.Crawl.ExcludedDomains {
		excluded[v] = struct{}{}
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
		cfg:             cfg,
		fetcher:         composite,
		processor:       proc,
		robots:          robots,
		storage:         pipeline,
		limiter:         limiter,
		footprint:       footprint,
		logger:          logger,
		allowed:         allowed,
		excluded:        excluded,
		includePatterns: include,
		excludePatterns: exclude,
		maxPages:        maxPages,
		closers:         closers,
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

	seeds, err := e.buildSeedRequests()
	if err != nil {
		return err
	}

	for _, req := range seeds {
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
	if ctx.Err() != nil {
		return
	}

	if !e.robots.Allowed(ctx, req.URL) {
		e.logger.Debug("blocked by robots", "url", req.URL.String())
		e.footprint.MarkExplored(req.URL, req.Depth)
		return
	}

	if err := e.limiter.Wait(ctx, req.URL.Hostname()); err != nil {
		e.logger.Warn("domain limiter interrupted", "url", req.URL.String(), "error", err)
		return
	}

	page, err := e.fetcher.Fetch(ctx, req)
	if err != nil {
		e.logger.Warn("fetch failed", "url", req.URL.String(), "error", err)
		return
	}

	result := types.CrawlResult{Request: req, Page: page}

	cleaned, err := e.processor.Process(ctx, page)
	if err != nil {
		e.logger.Debug("processor error", "url", req.URL.String(), "error", err)
	} else {
		result.Preprocessed = cleaned
	}

	links := e.extractLinks(page)
	result.Links = links

	if e.storage != nil {
		if err := e.storage.Persist(ctx, result); err != nil {
			e.logger.Error("persist failed", "url", req.URL.String(), "error", err)
		}
	}

	if len(links) == 0 || req.Depth >= req.MaxDepth {
		e.footprint.MarkExplored(req.URL, req.Depth)
	}

	if req.Depth >= req.MaxDepth {
		return
	}

	for _, link := range links {
		child := types.CrawlRequest{
			URL:       link,
			Depth:     req.Depth + 1,
			Parent:    req.URL,
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

func (e *Engine) extractLinks(page *types.Page) []*url.URL {
	if page == nil || len(page.Body) == 0 {
		return nil
	}

	base := page.FinalURL
	if base == nil {
		base = page.URL
	}
	if base == nil {
		return nil
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(page.Body))
	if err != nil {
		e.logger.Debug("link extraction failed", "error", err)
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
		if href == "" {
			return true
		}
		if strings.HasPrefix(href, "javascript:") || strings.HasPrefix(href, "mailto:") {
			return true
		}

		u, err := base.Parse(href)
		if err != nil {
			return true
		}
		u.Fragment = ""
		if !e.acceptLink(base, u) {
			return true
		}
		key := u.String()
		if _, exists := seen[key]; exists {
			return true
		}
		seen[key] = struct{}{}
		links = append(links, u)
		if len(links) >= maxLinks {
			return false
		}
		return true
	})

	return links
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
