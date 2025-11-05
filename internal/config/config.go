package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config captures the full configuration required to initialise the crawler engine.
type Config struct {
	DB         SQLConfig        `yaml:"db"`
	VectorDB   VectorDBConfig   `yaml:"vector_db"`
	Worker     WorkerConfig     `yaml:"worker"`
	Crawl      CrawlConfig      `yaml:"crawl"`
	Preprocess PreprocessConfig `yaml:"preprocess"`
	Robots     RobotsConfig     `yaml:"robots"`
	Rendering  RenderingConfig  `yaml:"rendering"`
	Logging    LoggingConfig    `yaml:"logging"`
	Media      MediaConfig      `yaml:"media"`
	Job        JobConfig        `yaml:"job"`
}

// SQLConfig describes a relational database connection used for persistence.
type SQLConfig struct {
	Driver          string   `yaml:"driver"`
	DSN             string   `yaml:"dsn"`
	MaxOpenConns    int      `yaml:"max_open_conns"`
	MaxIdleConns    int      `yaml:"max_idle_conns"`
	ConnMaxLifetime Duration `yaml:"conn_max_lifetime"`
	MigrationsPath  string   `yaml:"migrations_path"`
	CreateIfMissing bool     `yaml:"create_if_missing"`
	AutoMigrate     bool     `yaml:"auto_migrate"`
}

// VectorDBConfig describes a vector database used to store embeddings or semantic features.
type VectorDBConfig struct {
	Provider        string `yaml:"provider"`
	Endpoint        string `yaml:"endpoint"`
	APIKey          string `yaml:"api_key"`
	Index           string `yaml:"index"`
	Namespace       string `yaml:"namespace"`
	Dimension       int    `yaml:"dimension"`
	EmbeddingModel  string `yaml:"embedding_model"`
	UpsertBatchSize int    `yaml:"upsert_batch_size"`
}

// WorkerConfig controls concurrency, retry behaviour, and queue sizing.
type WorkerConfig struct {
	Concurrency  int      `yaml:"concurrency"`
	QueueSize    int      `yaml:"queue_size"`
	MaxRetries   int      `yaml:"max_retries"`
	RetryBackoff Duration `yaml:"retry_backoff"`
}

// CrawlConfig controls the crawl frontier, limits, and throttling.
type CrawlConfig struct {
	Seeds               []SeedConfig      `yaml:"seeds"`
	MaxDepth            int               `yaml:"max_depth"`
	MaxPages            int               `yaml:"max_pages"`
	UserAgent           string            `yaml:"user_agent"`
	Headers             map[string]string `yaml:"headers"`
	ProxyURL            string            `yaml:"proxy_url"`
	AllowedDomains      []string          `yaml:"allowed_domains"`
	ExcludedDomains     []string          `yaml:"excluded_domains"`
	PerDomainDelay      Duration          `yaml:"per_domain_delay"`
	RateLimitPerDomain  RateLimitConfig   `yaml:"rate_limit_per_domain"`
	RequestTimeout      Duration          `yaml:"request_timeout"`
	Discovery           DiscoveryConfig   `yaml:"discovery"`
	Footprint           FootprintConfig   `yaml:"footprint"`
	MaxBodyBytes        int64             `yaml:"max_body_bytes"`
	AllowedContentTypes []string          `yaml:"allowed_content_types"`
	RespectCanonical    bool              `yaml:"respect_canonical"`
	RespectMetaRobots   bool              `yaml:"respect_meta_robots"`
}

// SeedConfig declares an initial URL and optional depth override for the crawl frontier.
type SeedConfig struct {
	URL      string `yaml:"url"`
	MaxDepth int    `yaml:"max_depth"`
	Label    string `yaml:"label"`
}

// RateLimitConfig applies a token bucket per domain.
type RateLimitConfig struct {
	Requests int      `yaml:"requests"`
	Window   Duration `yaml:"window"`
}

// DiscoveryConfig tunes link extraction and filtering.
type DiscoveryConfig struct {
	FollowExternal   bool     `yaml:"follow_external"`
	MaxLinksPerPage  int      `yaml:"max_links_per_page"`
	IncludePatterns  []string `yaml:"include_patterns"`
	ExcludePatterns  []string `yaml:"exclude_patterns"`
	MinContentLength int      `yaml:"min_content_length"`
	RespectNofollow  bool     `yaml:"respect_nofollow"`
}

// FootprintConfig controls how aggressively the crawler remembers visited nodes.
type FootprintConfig struct {
	Enabled    bool     `yaml:"enabled"`
	TTL        Duration `yaml:"ttl"`
	MaxEntries int      `yaml:"max_entries"`
}

// PreprocessConfig configures HTML sanitisation.
type PreprocessConfig struct {
	RemoveAds        bool     `yaml:"remove_ads"`
	RemoveScripts    bool     `yaml:"remove_scripts"`
	RemoveStyles     bool     `yaml:"remove_styles"`
	TrimWhitespace   bool     `yaml:"trim_whitespace"`
	AdSelectors      []string `yaml:"ad_selectors"`
	ExtraDropClasses []string `yaml:"extra_drop_classes"`
}

// RobotsConfig configures robots.txt handling.
type RobotsConfig struct {
	Respect   bool     `yaml:"respect"`
	Overrides []string `yaml:"overrides"`
	UserAgent string   `yaml:"user_agent"`
	CacheTTL  Duration `yaml:"cache_ttl"`
}

// RenderingConfig controls optional JavaScript rendering.
type RenderingConfig struct {
	Enabled            bool     `yaml:"enabled"`
	Engine             string   `yaml:"engine"`
	Timeout            Duration `yaml:"timeout"`
	WaitForSelector    string   `yaml:"wait_for_selector"`
	ConcurrentSessions int      `yaml:"concurrent_sessions"`
	DisableHeadless    bool     `yaml:"disable_headless"`
}

// MediaConfig controls asset extraction and storage.
type MediaConfig struct {
	Enabled             bool     `yaml:"enabled"`
	Directory           string   `yaml:"directory"`
	MaxPerPage          int      `yaml:"max_per_page"`
	MaxSizeBytes        int64    `yaml:"max_size_bytes"`
	AllowedContentTypes []string `yaml:"allowed_content_types"`
}

// JobConfig identifies the scraper/run context provided by the orchestrator.
type JobConfig struct {
	ScraperID        string            `yaml:"scraper_id"`
	RunID            string            `yaml:"run_id"`
	TenantID         string            `yaml:"tenant_id"`
	DefaultSeedLabel string            `yaml:"default_seed_label"`
	Metadata         map[string]string `yaml:"metadata"`
}

// LoggingConfig selects log verbosity and format.
type LoggingConfig struct {
	Level      string `yaml:"level"`
	Structured bool   `yaml:"structured"`
}

// Default returns a Config populated with sensible defaults.
func Default() Config {
	return Config{
		Worker: WorkerConfig{
			Concurrency:  32,
			QueueSize:    2048,
			MaxRetries:   3,
			RetryBackoff: DurationFrom(500 * time.Millisecond),
		},
		Crawl: CrawlConfig{
			MaxDepth:       3,
			MaxPages:       1000,
			UserAgent:      "xgen-crawler-bot/1.0",
			Headers:        map[string]string{},
			PerDomainDelay: DurationFrom(250 * time.Millisecond),
			RequestTimeout: DurationFrom(10 * time.Second),
			MaxBodyBytes:   6 * 1024 * 1024,
			AllowedContentTypes: []string{
				"text/html",
				"application/xhtml+xml",
			},
			RespectCanonical:  true,
			RespectMetaRobots: true,
			Discovery: DiscoveryConfig{
				FollowExternal:  false,
				MaxLinksPerPage: 200,
				RespectNofollow: true,
			},
			Footprint: FootprintConfig{
				Enabled:    true,
				TTL:        DurationFrom(24 * time.Hour),
				MaxEntries: 100000,
			},
		},
		Preprocess: PreprocessConfig{
			RemoveAds:      true,
			RemoveScripts:  true,
			RemoveStyles:   false,
			TrimWhitespace: true,
			AdSelectors: []string{
				"[class*='advert']",
				"[class*='ad-']",
				"[id*='ad']",
				"script",
				"iframe[src*='ads']",
			},
		},
		Robots: RobotsConfig{
			Respect:   true,
			Overrides: []string{},
			UserAgent: "xgen-crawler-bot/1.0",
			CacheTTL:  DurationFrom(6 * time.Hour),
		},
		Rendering: RenderingConfig{
			Enabled:            false,
			Engine:             "chromedp",
			Timeout:            DurationFrom(15 * time.Second),
			ConcurrentSessions: 2,
		},
		Logging: LoggingConfig{
			Level:      "info",
			Structured: true,
		},
		DB: SQLConfig{
			AutoMigrate: true,
		},
		Media: MediaConfig{
			Enabled:      false,
			Directory:    "",
			MaxPerPage:   8,
			MaxSizeBytes: 2 * 1024 * 1024,
			AllowedContentTypes: []string{
				"image/jpeg",
				"image/png",
				"image/webp",
				"image/gif",
			},
		},
		Job: JobConfig{
			Metadata: make(map[string]string),
		},
	}
}

// Load reads, merges, and validates configuration from a YAML file.
func Load(path string) (*Config, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer fh.Close()

	cfg := Default()
	if err := decodeYAML(fh, &cfg); err != nil {
		return nil, err
	}
	cfg.normalise()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// LoadFromReader decodes configuration from an arbitrary reader.
func LoadFromReader(r io.Reader) (*Config, error) {
	cfg := Default()
	if err := decodeYAML(r, &cfg); err != nil {
		return nil, err
	}
	cfg.normalise()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func decodeYAML(r io.Reader, cfg *Config) error {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	return nil
}

// Validate enforces required invariants for the crawler configuration.
func (c Config) Validate() error {
	if len(c.Crawl.Seeds) == 0 {
		return errors.New("at least one crawl seed must be configured")
	}
	for i := range c.Crawl.Seeds {
		if c.Crawl.Seeds[i].URL == "" {
			return fmt.Errorf("seed %d has empty url", i)
		}
		if c.Crawl.Seeds[i].MaxDepth < 0 {
			return fmt.Errorf("seed %s has invalid max_depth %d", c.Crawl.Seeds[i].URL, c.Crawl.Seeds[i].MaxDepth)
		}
	}
	if c.Crawl.MaxDepth <= 0 {
		return fmt.Errorf("crawl.max_depth must be > 0 (got %d)", c.Crawl.MaxDepth)
	}
	if c.Worker.Concurrency <= 0 {
		return fmt.Errorf("worker.concurrency must be > 0 (got %d)", c.Worker.Concurrency)
	}
	if c.Worker.QueueSize <= 0 {
		return fmt.Errorf("worker.queue_size must be > 0 (got %d)", c.Worker.QueueSize)
	}
	if c.Worker.MaxRetries < 0 {
		return fmt.Errorf("worker.max_retries must be >= 0 (got %d)", c.Worker.MaxRetries)
	}
	if c.Crawl.MaxPages < 0 {
		return fmt.Errorf("crawl.max_pages must be >= 0 (got %d)", c.Crawl.MaxPages)
	}
	if rl := c.Crawl.RateLimitPerDomain; rl.Requests < 0 {
		return fmt.Errorf("crawl.rate_limit_per_domain.requests must be >= 0 (got %d)", rl.Requests)
	}
	if c.Crawl.MaxBodyBytes <= 0 {
		return fmt.Errorf("crawl.max_body_bytes must be > 0 (got %d)", c.Crawl.MaxBodyBytes)
	}
	if strings.TrimSpace(c.Crawl.UserAgent) == "" {
		return errors.New("crawl.user_agent must be set")
	}
	if strings.TrimSpace(c.Robots.UserAgent) == "" {
		return errors.New("robots.user_agent must be set")
	}
	if c.Media.Enabled {
		if strings.TrimSpace(c.Media.Directory) == "" {
			return errors.New("media.directory must be set when media.enabled is true")
		}
		if c.Media.MaxPerPage < 0 {
			return fmt.Errorf("media.max_per_page must be >= 0 (got %d)", c.Media.MaxPerPage)
		}
		if c.Media.MaxSizeBytes <= 0 {
			return fmt.Errorf("media.max_size_bytes must be > 0 (got %d)", c.Media.MaxSizeBytes)
		}
		if len(c.Media.AllowedContentTypes) == 0 {
			return errors.New("media.allowed_content_types must include at least one value")
		}
	}
	return nil
}

func (c *Config) normalise() {
	for i := range c.Crawl.Seeds {
		c.Crawl.Seeds[i].URL = strings.TrimSpace(c.Crawl.Seeds[i].URL)
		c.Crawl.Seeds[i].Label = strings.TrimSpace(c.Crawl.Seeds[i].Label)
	}
	c.Crawl.UserAgent = strings.TrimSpace(c.Crawl.UserAgent)
	c.Robots.UserAgent = strings.TrimSpace(c.Robots.UserAgent)

	c.Job.ScraperID = strings.TrimSpace(c.Job.ScraperID)
	c.Job.RunID = strings.TrimSpace(c.Job.RunID)
	c.Job.TenantID = strings.TrimSpace(c.Job.TenantID)
	c.Job.DefaultSeedLabel = strings.TrimSpace(c.Job.DefaultSeedLabel)
	if c.Job.Metadata == nil {
		c.Job.Metadata = make(map[string]string)
	}

	// Ensure overrides are de-duplicated and normalised to lower case.
	if len(c.Robots.Overrides) > 0 {
		unique := make(map[string]struct{}, len(c.Robots.Overrides))
		cleaned := make([]string, 0, len(c.Robots.Overrides))
		for _, raw := range c.Robots.Overrides {
			host := strings.ToLower(strings.TrimSpace(raw))
			if host == "" {
				continue
			}
			if _, exists := unique[host]; exists {
				continue
			}
			unique[host] = struct{}{}
			cleaned = append(cleaned, host)
		}
		sort.Strings(cleaned)
		c.Robots.Overrides = cleaned
	}
	if len(c.Crawl.AllowedDomains) > 0 {
		c.Crawl.AllowedDomains = dedupeLower(c.Crawl.AllowedDomains)
	}
	if len(c.Crawl.ExcludedDomains) > 0 {
		c.Crawl.ExcludedDomains = dedupeLower(c.Crawl.ExcludedDomains)
	}
	if len(c.Crawl.AllowedContentTypes) > 0 {
		c.Crawl.AllowedContentTypes = dedupeLower(c.Crawl.AllowedContentTypes)
	}
	c.Media.Directory = strings.TrimSpace(c.Media.Directory)
	if len(c.Media.AllowedContentTypes) > 0 {
		c.Media.AllowedContentTypes = dedupeLower(c.Media.AllowedContentTypes)
	}
}

func dedupeLower(values []string) []string {
	unique := make(map[string]struct{}, len(values))
	cleaned := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "" {
			continue
		}
		if _, ok := unique[v]; ok {
			continue
		}
		unique[v] = struct{}{}
		cleaned = append(cleaned, v)
	}
	sort.Strings(cleaned)
	return cleaned
}

// Enabled reports whether per-domain rate limiting is active.
func (r RateLimitConfig) Enabled() bool {
	return r.Requests > 0 && !r.Window.IsZero()
}
