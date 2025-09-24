# News Crawler Engine (Go)

This repository contains a modular Go implementation of a high-performance news crawling engine. Key design goals:

- Structured configuration describing relational DB, vector DB, crawling seeds, depth/limits, robots overrides, and preprocessing rules.
- Horizontally scalable worker-pool based crawler with per-domain throttling and footprint tracking to avoid redundant work at terminal depths.
- Pluggable fetch pipeline supporting HTTP fetching, optional JavaScript rendering, and HTML sanitisation that strips ads and other noisy elements before downstream processing.
- Robots-aware link discovery with domain overrides for portals that permit bypassing robots.txt.

## Module Layout

```
cmd/newscrawler       Application entry-point and CLI wiring
internal/config       Configuration loading/validation utilities
internal/crawler      Crawl coordinator, worker pool, footprint tracking
internal/fetcher      Fetch strategies (HTTP + optional JS rendering)
internal/processor    HTML preprocessing (ad removal, normalisation)
internal/robots       robots.txt client/cache with domain overrides
internal/storage      Interfaces to SQL + Vector DB sinks
pkg/types             Shared strongly-typed primitives (durations, queues)
```

Each module is documented inline. Extend the configuration to suit deployment environments (Kubernetes, serverless, etc.) by adding new sub-structs.

## Configuration

- Base configuration lives in YAML (see `configs/example.yaml`). It includes:
  - SQL database connection, pooling settings, and `create_if_missing` to auto-provision the target database when supported (PostgreSQL).
  - Vector database metadata for embedding upserts.
  - Crawl seeds, per-domain throttling, robots overrides, footprint sizing, and crawl politeness controls such as `max_body_bytes`, `allowed_content_types`, canonical/meta-robots respect flags, and `discovery.respect_nofollow`.
  - Preprocessing flags (ad removal selectors, script/style stripping).
  - Rendering options for headless Chromium via `chromedp` when JavaScript execution is required.
- Durations use Go's syntax (`500ms`, `15s`, `24h`). Lists such as `allowed_domains` drive host-level filtering.

## Running

```bash
go run ./cmd/newscrawler --config configs/example.yaml
```

```bash
go build ./cmd/newscrawler
./newscrawler --config <config-path>
```

- The crawler observes `robots.txt` unless a host is listed under `robots.overrides`.
- Worker concurrency, queue size, and per-domain delay protect against deadlocks and throttling.
- Run `go mod tidy` before building to fetch third-party modules (`chromedp`, `goquery`, `robotstxt`).
