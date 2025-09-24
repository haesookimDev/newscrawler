package types

import (
	"net/http"
	"net/url"
	"time"
)

// CrawlRequest models a work item submitted to the crawler frontier.
type CrawlRequest struct {
	URL        *url.URL
	Depth      int
	Parent     *url.URL
	Render     bool
	SeedLabel  string
	MaxDepth   int
	EnqueuedAt time.Time
}

// Page represents the fetched content.
type Page struct {
	URL             *url.URL
	FinalURL        *url.URL
	Body            []byte
	ContentType     string
	StatusCode      int
	Headers         http.Header
	FetchedAt       time.Time
	Rendered        bool
	ResponseLatency time.Duration
}

// CrawlResult aggregates the outcome of processing a request.
type CrawlResult struct {
	Request       CrawlRequest
	Page          *Page
	Links         []*url.URL
	Error         error
	RobotsIgnored bool
	Preprocessed  []byte
	Metadata      map[string]string
}

// FootprintState tracks crawl completion for a URL at a specific depth.
type FootprintState struct {
	Depth         int
	FullyExplored bool
	LastVisited   time.Time
}
