package crawler

import (
	"context"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiterSettings configures token-bucket style rate limiting per host.
type RateLimiterSettings struct {
	Requests int
	Window   time.Duration
}

// DomainLimiter enforces per-domain politeness rules combining delay and rate limits.
type DomainLimiter struct {
	delay       time.Duration
	rate        RateLimiterSettings
	rateEnabled bool

	mu       sync.Mutex
	last     map[string]time.Time
	limiters map[string]*rate.Limiter
}

// NewDomainLimiter creates a limiter with per-domain delay and optional rate limiting.
func NewDomainLimiter(delay time.Duration, rateCfg RateLimiterSettings) *DomainLimiter {
	limiter := &DomainLimiter{delay: delay}
	if delay > 0 {
		limiter.last = make(map[string]time.Time)
	}
	if rateCfg.Requests > 0 && rateCfg.Window > 0 {
		limiter.rateEnabled = true
		limiter.rate = rateCfg
		limiter.limiters = make(map[string]*rate.Limiter)
		if limiter.last == nil {
			limiter.last = make(map[string]time.Time)
		}
	}
	return limiter
}

// Wait blocks until politeness constraints for the host are satisfied.
func (d *DomainLimiter) Wait(ctx context.Context, host string) error {
	if d == nil || host == "" {
		return nil
	}
	host = strings.ToLower(host)

	if d.delay <= 0 && !d.rateEnabled {
		return nil
	}

	var sleep time.Duration
	var limiter *rate.Limiter
	now := time.Now()

	d.mu.Lock()
	if d.delay > 0 {
		if last, ok := d.last[host]; ok {
			rest := last.Add(d.delay).Sub(now)
			if rest > 0 {
				sleep = rest
			}
		}
	}
	if d.rateEnabled {
		limiter = d.ensureLimiterLocked(host)
	}
	d.mu.Unlock()

	if sleep > 0 {
		timer := time.NewTimer(sleep)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if limiter != nil {
		if err := limiter.Wait(ctx); err != nil {
			return err
		}
	}

	d.mu.Lock()
	if d.last != nil {
		d.last[host] = time.Now()
	}
	d.mu.Unlock()
	return nil
}

func (d *DomainLimiter) ensureLimiterLocked(host string) *rate.Limiter {
	limiter, ok := d.limiters[host]
	if ok {
		return limiter
	}
	interval := d.rate.Window / time.Duration(d.rate.Requests)
	if interval <= 0 {
		interval = time.Millisecond
	}
	burst := d.rate.Requests
	if burst <= 0 {
		burst = 1
	}
	limiter = rate.NewLimiter(rate.Every(interval), burst)
	d.limiters[host] = limiter
	return limiter
}
