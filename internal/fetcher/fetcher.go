package fetcher

import (
	"compress/flate"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/andybalholm/brotli"

	"xgen-crawler/pkg/types"
)

// Fetcher retrieves a web page for the crawler.
type Fetcher interface {
	Fetch(ctx context.Context, req types.CrawlRequest) (*types.Page, error)
}

// Options controls HTTP fetching behaviour.
type Options struct {
	UserAgent    string
	Headers      map[string]string
	Timeout      time.Duration
	MaxBodyBytes int64
	ProxyURL     string
}

// HTTPFetcher implements Fetcher via the Go http.Client.
type HTTPFetcher struct {
	client       *http.Client
	userAgent    string
	extraHeaders map[string]string
	maxBodyBytes int64
}

// NewHTTPFetcher constructs an HTTP fetcher using the provided options.
func NewHTTPFetcher(opts Options) (*HTTPFetcher, error) {
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = 5 * 1024 * 1024 // 5MB cap
	}

	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if strings.TrimSpace(opts.ProxyURL) != "" {
		proxyURL, err := url.Parse(opts.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("parse proxy url: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}

	client := &http.Client{
		Timeout:   opts.Timeout,
		Transport: transport,
	}

	headers := make(map[string]string, len(opts.Headers))
	for k, v := range opts.Headers {
		headers[k] = v
	}

	return &HTTPFetcher{
		client:       client,
		userAgent:    opts.UserAgent,
		extraHeaders: headers,
		maxBodyBytes: opts.MaxBodyBytes,
	}, nil
}

// Fetch downloads a single URL using HTTP.
func (f *HTTPFetcher) Fetch(ctx context.Context, req types.CrawlRequest) (*types.Page, error) {
	if req.URL == nil {
		return nil, errors.New("request URL is nil")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	if f.userAgent != "" {
		httpReq.Header.Set("User-Agent", f.userAgent)
	}
	httpReq.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	httpReq.Header.Set("Accept-Language", "en-US,en;q=0.8")
	httpReq.Header.Set("Accept-Encoding", "gzip, deflate, br")

	for k, v := range f.extraHeaders {
		httpReq.Header.Set(k, v)
	}

	start := time.Now()
	resp, err := f.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http fetch failed: %w", err)
	}

	body, err := f.readBody(resp)
	if err != nil {
		return nil, err
	}

	var finalURL *url.URL
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL
	} else {
		finalURL = req.URL
	}

	page := &types.Page{
		URL:             req.URL,
		FinalURL:        finalURL,
		Body:            body,
		ContentType:     resp.Header.Get("Content-Type"),
		StatusCode:      resp.StatusCode,
		Headers:         resp.Header.Clone(),
		FetchedAt:       time.Now(),
		Rendered:        req.Render,
		ResponseLatency: time.Since(start),
	}

	return page, nil
}

func (f *HTTPFetcher) readBody(resp *http.Response) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("empty response body")
	}

	reader := io.Reader(resp.Body)
	closers := []io.Closer{resp.Body}

	encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
	switch encoding {
	case "gzip":
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("gzip decode: %w", err)
		}
		reader = gz
		closers = append(closers, gz)
	case "br":
		reader = brotli.NewReader(resp.Body)
	case "deflate":
		fl := flate.NewReader(resp.Body)
		reader = fl
		closers = append(closers, fl)
	}

	defer func() {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i].Close()
		}
	}()

	limited := io.LimitReader(reader, f.maxBodyBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if int64(len(body)) > f.maxBodyBytes {
		return nil, fmt.Errorf("response body exceeds limit of %d bytes", f.maxBodyBytes)
	}
	return body, nil
}

// Client exposes the underlying HTTP client for reuse (eg. robots.txt fetches).
func (f *HTTPFetcher) Client() *http.Client {
	if f == nil {
		return nil
	}
	return f.client
}

// Composite chooses between raw HTTP and a renderer per request.
type Composite struct {
	defaultFetcher Fetcher
	renderer       Renderer
}

// Renderer executes JavaScript and returns the rendered DOM.
type Renderer interface {
	Render(ctx context.Context, req types.CrawlRequest) (*types.Page, error)
}

// NewComposite builds a composite fetcher from HTTP and optional renderer components.
func NewComposite(httpFetcher Fetcher, renderer Renderer) *Composite {
	return &Composite{defaultFetcher: httpFetcher, renderer: renderer}
}

// Fetch delegates to either the renderer (if requested) or the HTTP fetcher.
func (c *Composite) Fetch(ctx context.Context, req types.CrawlRequest) (*types.Page, error) {
	if req.Render && c.renderer != nil {
		page, err := c.renderer.Render(ctx, req)
		if err == nil {
			return page, nil
		}
		// fall back to HTTP fetch on renderer errors.
		logger := slog.With("url", req.URL.String(), "error", err)
		logger.Warn("renderer failed, falling back to HTTP fetch")
	}
	if req.Render {
		req.Render = false
	}
	return c.defaultFetcher.Fetch(ctx, req)
}
