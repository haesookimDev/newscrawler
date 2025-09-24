package crawler

import (
	"context"
	"strings"
	"sync"
	"time"
)

// DomainLimiter enforces a minimum delay between requests per host.
type DomainLimiter struct {
	delay time.Duration
	mu    sync.Mutex
	last  map[string]time.Time
}

// NewDomainLimiter creates a limiter with the provided delay.
func NewDomainLimiter(delay time.Duration) *DomainLimiter {
	if delay <= 0 {
		return &DomainLimiter{delay: 0}
	}
	return &DomainLimiter{
		delay: delay,
		last:  make(map[string]time.Time),
	}
}

// Wait sleeps as necessary to respect the per-domain delay.
func (d *DomainLimiter) Wait(ctx context.Context, host string) error {
	if d == nil || d.delay <= 0 || host == "" {
		return nil
	}
	host = strings.ToLower(host)

	d.mu.Lock()
	last := d.last[host]
	d.mu.Unlock()

	now := time.Now()
	wait := d.delay - now.Sub(last)
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	d.mu.Lock()
	d.last[host] = time.Now()
	d.mu.Unlock()
	return nil
}
