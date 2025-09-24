package crawler

import (
	"net/url"
	"strings"
	"sync"
	"time"

	"newscrawler/pkg/types"
)

// Footprint tracks visited URLs to prevent redundant crawls.
type Footprint struct {
	mu         sync.RWMutex
	entries    map[string]types.FootprintState
	maxEntries int
	ttl        time.Duration
	enabled    bool
}

// NewFootprint initialises a footprint store with an optional capacity hint.
func NewFootprint(maxEntries int, ttl time.Duration, enabled bool) *Footprint {
	if maxEntries <= 0 {
		maxEntries = 200000
	}
	fp := &Footprint{
		maxEntries: maxEntries,
		ttl:        ttl,
		enabled:    enabled,
	}
	if enabled {
		fp.entries = make(map[string]types.FootprintState, maxEntries)
	}
	return fp
}

// ShouldVisit reports whether a URL at the given depth should be crawled.
func (f *Footprint) ShouldVisit(u *url.URL, depth int, maxDepth int) bool {
	if !f.enabled {
		return true
	}
	if u == nil {
		return false
	}
	key := canonicalKey(u)
	now := time.Now()

	f.mu.RLock()
	state, ok := f.entries[key]
	f.mu.RUnlock()
	if !ok {
		return true
	}
	if f.ttl > 0 && now.Sub(state.LastVisited) > f.ttl {
		f.mu.Lock()
		if current, ok := f.entries[key]; ok && now.Sub(current.LastVisited) > f.ttl {
			delete(f.entries, key)
			f.mu.Unlock()
			return true
		}
		if updated, ok := f.entries[key]; ok {
			state = updated
		}
		f.mu.Unlock()
	}
	if depth < state.Depth {
		return true
	}
	if !state.FullyExplored {
		return false
	}
	if depth >= maxDepth && state.Depth >= maxDepth {
		return false
	}
	return false
}

// MarkVisited records a visit occurrence with optional exploration status.
func (f *Footprint) MarkVisited(u *url.URL, depth int, fullyExplored bool) {
	if !f.enabled {
		return
	}
	if u == nil {
		return
	}
	key := canonicalKey(u)
	f.mu.Lock()
	now := time.Now()
	current := f.entries[key]
	if current.Depth == 0 || depth < current.Depth {
		current.Depth = depth
	}
	current.LastVisited = now
	if fullyExplored {
		current.FullyExplored = true
	}
	f.entries[key] = current
	f.removeExpiredLocked(now)
	if len(f.entries) > f.maxEntries {
		f.evictOldestLocked(now)
	}
	f.mu.Unlock()
}

// MarkExplored marks a URL as fully explored to prevent further crawls.
func (f *Footprint) MarkExplored(u *url.URL, depth int) {
	if !f.enabled {
		return
	}
	if u == nil {
		return
	}
	key := canonicalKey(u)
	f.mu.Lock()
	now := time.Now()
	state := f.entries[key]
	if state.Depth == 0 || depth < state.Depth {
		state.Depth = depth
	}
	state.FullyExplored = true
	state.LastVisited = now
	f.entries[key] = state
	f.removeExpiredLocked(now)
	if len(f.entries) > f.maxEntries {
		f.evictOldestLocked(now)
	}
	f.mu.Unlock()
}

func (f *Footprint) evictOldestLocked(now time.Time) {
	var oldestKey string
	var oldestTime time.Time
	for key, state := range f.entries {
		if f.ttl > 0 && now.Sub(state.LastVisited) > f.ttl {
			delete(f.entries, key)
			continue
		}
		if oldestKey == "" || state.LastVisited.Before(oldestTime) {
			oldestKey = key
			oldestTime = state.LastVisited
		}
	}
	if oldestKey != "" {
		delete(f.entries, oldestKey)
	}
}
func (f *Footprint) removeExpiredLocked(now time.Time) {
	if f.ttl <= 0 {
		return
	}
	for key, state := range f.entries {
		if now.Sub(state.LastVisited) > f.ttl {
			delete(f.entries, key)
		}
	}
}

func canonicalKey(u *url.URL) string {
	if u == nil {
		return ""
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme == "" {
		scheme = "http"
	}
	host := strings.ToLower(u.Hostname())
	if port := u.Port(); port != "" && port != defaultPortForScheme(scheme) {
		host = host + ":" + port
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	key := scheme + "://" + host + path
	if q := u.RawQuery; q != "" {
		key += "?" + q
	}
	return key
}

func defaultPortForScheme(scheme string) string {
	switch scheme {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}
