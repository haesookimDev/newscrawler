package robots

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/temoto/robotstxt"

	"newscrawler/internal/config"
)

// Agent evaluates robots.txt rules with caching and domain overrides.
type Agent struct {
	client    *http.Client
	userAgent string
	ttl       time.Duration
	respect   bool

	mu        sync.RWMutex
	cache     map[string]cacheEntry
	overrides map[string]struct{}
}

type cacheEntry struct {
	fetched time.Time
	rules   *robotstxt.RobotsData
}

// NewAgent constructs a robots agent from configuration.
func NewAgent(cfg config.RobotsConfig, client *http.Client) *Agent {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	ttl := cfg.CacheTTL.Duration
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}

	overrides := make(map[string]struct{}, len(cfg.Overrides))
	for _, host := range cfg.Overrides {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" {
			continue
		}
		overrides[host] = struct{}{}
	}

	return &Agent{
		client:    client,
		userAgent: cfg.UserAgent,
		ttl:       ttl,
		respect:   cfg.Respect,
		cache:     make(map[string]cacheEntry),
		overrides: overrides,
	}
}

// Allowed reports whether the target URL is permitted.
func (a *Agent) Allowed(ctx context.Context, target *url.URL) bool {
	if target == nil {
		return false
	}
	if !target.IsAbs() {
		return false
	}

	if !a.respect {
		return true
	}

	host := strings.ToLower(target.Hostname())
	if _, ok := a.overrides[host]; ok {
		return true
	}

	rules, err := a.rules(ctx, target)
	if err != nil {
		// Fail-open on robots errors (common industry practice).
		return true
	}

	group := rules.FindGroup(a.userAgent)
	if group == nil {
		group = rules.FindGroup("*")
		if group == nil {
			return true
		}
	}
	return group.Test(target.Path)
}

func (a *Agent) rules(ctx context.Context, target *url.URL) (*robotstxt.RobotsData, error) {
	host := strings.ToLower(target.Host)

	a.mu.RLock()
	entry, ok := a.cache[host]
	if ok && time.Since(entry.fetched) < a.ttl {
		a.mu.RUnlock()
		return entry.rules, nil
	}
	a.mu.RUnlock()

	robotsURL := target.Scheme + "://" + target.Host + "/robots.txt"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build robots request: %w", err)
	}
	if a.userAgent != "" {
		req.Header.Set("User-Agent", a.userAgent)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch robots.txt: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("robots returned status %d", resp.StatusCode)
	}

	data, err := robotstxt.FromResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("parse robots.txt: %w", err)
	}

	a.mu.Lock()
	a.cache[host] = cacheEntry{fetched: time.Now(), rules: data}
	a.mu.Unlock()

	return data, nil
}

// Purge evicts cached robots rules for a host.
func (a *Agent) Purge(host string) {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return
	}
	a.mu.Lock()
	delete(a.cache, host)
	a.mu.Unlock()
}
