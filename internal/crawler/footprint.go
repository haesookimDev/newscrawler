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
}

// NewFootprint initialises a footprint store with an optional capacity hint.
func NewFootprint(maxEntries int) *Footprint {
	if maxEntries <= 0 {
		maxEntries = 200000
	}
	return &Footprint{
		entries:    make(map[string]types.FootprintState, maxEntries),
		maxEntries: maxEntries,
	}
}

// ShouldVisit reports whether a URL at the given depth should be crawled.
func (f *Footprint) ShouldVisit(u *url.URL, depth int, maxDepth int) bool {
	if u == nil {
		return false
	}
	key := canonicalKey(u)
	f.mu.RLock()
	state, ok := f.entries[key]
	f.mu.RUnlock()
	if !ok {
		return true
	}
	if depth < state.Depth {
		return true
	}
	if depth >= maxDepth && state.Depth >= maxDepth {
		return false
	}
	if state.FullyExplored {
		return false
	}
	return false
}

// MarkVisited records a visit occurrence with optional exploration status.
func (f *Footprint) MarkVisited(u *url.URL, depth int, fullyExplored bool) {
	if u == nil {
		return
	}
	key := canonicalKey(u)
	f.mu.Lock()
	current, ok := f.entries[key]
	if !ok || depth < current.Depth {
		current.Depth = depth
	}
	current.LastVisited = time.Now()
	if fullyExplored {
		current.FullyExplored = true
	}
	f.entries[key] = current
	if len(f.entries) > f.maxEntries {
		f.evictOldestLocked()
	}
	f.mu.Unlock()
}

// MarkExplored marks a URL as fully explored to prevent further crawls.
func (f *Footprint) MarkExplored(u *url.URL, depth int) {
	if u == nil {
		return
	}
	key := canonicalKey(u)
	f.mu.Lock()
	state := f.entries[key]
	state.FullyExplored = true
	if depth < state.Depth || state.Depth == 0 {
		state.Depth = depth
	}
	state.LastVisited = time.Now()
	f.entries[key] = state
	if len(f.entries) > f.maxEntries {
		f.evictOldestLocked()
	}
	f.mu.Unlock()
}

func (f *Footprint) evictOldestLocked() {
	var oldestKey string
	var oldestTime time.Time
	for key, state := range f.entries {
		if oldestKey == "" || state.LastVisited.Before(oldestTime) {
			oldestKey = key
			oldestTime = state.LastVisited
		}
	}
	if oldestKey != "" {
		delete(f.entries, oldestKey)
	}
}

func canonicalKey(u *url.URL) string {
	if u == nil {
		return ""
	}
	host := strings.ToLower(u.Host)
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	return host + path + "?" + u.RawQuery
}
