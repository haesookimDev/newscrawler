package fetcher

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/chromedp/chromedp"

	"xgen-crawler/pkg/types"
)

// RenderOptions configures the JavaScript rendering pipeline.
type RenderOptions struct {
	Timeout            time.Duration
	WaitForSelector    string
	WaitForDOMReady    bool
	UserAgent          string
	MaxBodyBytes       int64
	DisableHeadless    bool
	ConcurrentSessions int
	CaptureDelay       time.Duration
}

// ChromedpRenderer executes headless Chrome sessions using chromedp.
type ChromedpRenderer struct {
	opts      RenderOptions
	semaphore chan struct{}
	logger    *slog.Logger
}

// NewChromedpRenderer constructs a renderer with bounded concurrency.
func NewChromedpRenderer(opts RenderOptions) *ChromedpRenderer {
	if opts.Timeout <= 0 {
		opts.Timeout = 60 * time.Second
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = 5 * 1024 * 1024
	}
	if opts.ConcurrentSessions <= 0 {
		opts.ConcurrentSessions = 1
	}
	return &ChromedpRenderer{
		opts:      opts,
		semaphore: make(chan struct{}, opts.ConcurrentSessions),
		logger:    slog.Default(),
	}
}

// Render navigates to the target URL and exports the final DOM outer HTML.
func (r *ChromedpRenderer) Render(parentCtx context.Context, req types.CrawlRequest) (*types.Page, error) {
	if req.URL == nil {
		return nil, fmt.Errorf("render request URL is nil")
	}

	logger := r.logger.With(
		"url", req.URL.String(),
		"timeout", r.opts.Timeout.String(),
		"wait_for_dom_ready", r.opts.WaitForDOMReady,
		"wait_for_selector", strings.TrimSpace(r.opts.WaitForSelector),
	)

	select {
	case r.semaphore <- struct{}{}:
		defer func() { <-r.semaphore }()
	case <-parentCtx.Done():
		return nil, parentCtx.Err()
	}

	ctx, cancel := context.WithTimeout(parentCtx, r.opts.Timeout)
	defer cancel()

	headless := !r.opts.DisableHeadless
	execOpts := []chromedp.ExecAllocatorOption{
		chromedp.Flag("headless", headless),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("no-sandbox", true),
	}

	if ua := strings.TrimSpace(selectUserAgent(r.opts.UserAgent)); ua != "" {
		execOpts = append(execOpts, chromedp.UserAgent(ua))
	}

	allocCtx, allocCancel := chromedp.NewExecAllocator(ctx, execOpts...)
	defer allocCancel()

	chromeCtx, chromeCancel := chromedp.NewContext(allocCtx)
	defer chromeCancel()

	start := time.Now()
	var html string
	var finalURL string

	actions := []chromedp.Action{
		chromedp.Navigate(req.URL.String()),
	}

	waitMode := "capture_delay"
	switch {
	case r.opts.WaitForDOMReady:
		waitMode = "dom_ready"
		actions = append(actions,
			waitForDocumentReady(logger),
			chromedp.Sleep(250*time.Millisecond),
		)
	default:
		waitSelector := strings.TrimSpace(r.opts.WaitForSelector)
		if waitSelector != "" {
			waitMode = "selector"
			actions = append(actions,
				chromedp.WaitReady(waitSelector, chromedp.ByQuery),
				chromedp.Sleep(250*time.Millisecond),
			)
		} else {
			delay := r.opts.CaptureDelay
			if delay <= 0 {
				delay = 1500 * time.Millisecond
			}
			waitMode = "delay"
			actions = append(actions, chromedp.Sleep(delay))
		}
	}
	logger.Debug("chromedp starting render", "wait_mode", waitMode)
	actions = append(actions,
		chromedp.OuterHTML("html", &html, chromedp.ByQuery),
		chromedp.Location(&finalURL),
	)

	if err := chromedp.Run(chromeCtx, actions...); err != nil {
		logger.Error("chromedp run failed", "error", err)
		return nil, fmt.Errorf("chromedp run: %w", err)
	}

	if int64(len(html)) > r.opts.MaxBodyBytes {
		html = html[:r.opts.MaxBodyBytes]
	}

	parsedFinal := req.URL
	if finalURL != "" {
		if u, err := url.Parse(finalURL); err == nil {
			parsedFinal = u
		}
	}

	latency := time.Since(start)
	page := &types.Page{
		URL:             req.URL,
		FinalURL:        parsedFinal,
		Body:            []byte(html),
		ContentType:     "text/html; charset=utf-8",
		StatusCode:      200,
		FetchedAt:       time.Now(),
		Rendered:        true,
		ResponseLatency: latency,
	}
	logger.Debug("chromedp render complete",
		"latency_ms", latency.Milliseconds(),
		"final_url", parsedFinal.String(),
		"html_bytes", len(html),
	)
	return page, nil
}

func selectUserAgent(base string) string {
	if strings.TrimSpace(base) != "" {
		return base
	}
	return "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
}

func waitForDocumentReady(logger *slog.Logger) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			var readyState string
			if err := chromedp.Evaluate(`document.readyState`, &readyState).Do(ctx); err != nil {
				if logger != nil {
					logger.Warn("waitForDocumentReady evaluate failed", "error", err)
				}
				return err
			}
			if readyState == "complete" {
				return nil
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				if logger != nil {
					logger.Warn("waitForDocumentReady cancelled", "error", ctx.Err())
				}
				return ctx.Err()
			}
		}
	})
}
