# News Crawler Engine (Go)

This repository contains a modular Go implementation of a high-performance news crawling engine. Key design goals:

- Structured configuration describing relational DB, vector DB, crawling seeds, depth/limits, robots overrides, and preprocessing rules.
- Horizontally scalable worker-pool based crawler with per-domain throttling and footprint tracking to avoid redundant work at terminal depths.
- Pluggable fetch pipeline supporting HTTP fetching, optional JavaScript rendering, and HTML sanitisation that strips ads and other noisy elements before downstream processing.
- Robots-aware link discovery with domain overrides for portals that permit bypassing robots.txt.

## Module Layout

```
cmd/xgen-crawler      Application entry-point and CLI wiring
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
  - SQL database connection, pooling settings, `create_if_missing` to auto-provision the database, and `auto_migrate` to auto-create required tables when missing (PostgreSQL).
  - Media extraction controls (`media.*`) to download page images to a filesystem path while recording metadata in the relational store.
  - Vector database metadata for embedding upserts.
  - Crawl seeds, per-domain throttling, robots overrides, footprint sizing, and crawl politeness controls such as `max_body_bytes`, `allowed_content_types`, canonical/meta-robots respect flags, and `discovery.respect_nofollow`.
  - Preprocessing flags (ad removal selectors, script/style stripping).
- Rendering options for headless Chromium via `chromedp` when JavaScript execution is required.
- Durations use Go's syntax (`500ms`, `15s`, `24h`). Lists such as `allowed_domains` drive host-level filtering.

### Environment Overrides

- Set `XGEN_DB_DRIVER` and `XGEN_DB_DSN` to override the database connection supplied in YAML.
- Set `XGEN_DOC_DB_DRIVER` / `XGEN_DOC_DB_DSN` when the downstream document/vector database (used for `/documents` sync) lives in a separate Postgres instance.
- Set `XGEN_VECTOR_*` variables (`PROVIDER`, `ENDPOINT`, `API_KEY`, `INDEX`, `NAMESPACE`, `DIMENSION`, `EMBEDDING_MODEL`, `UPSERT_BATCH_SIZE`) to replace vector DB settings at runtime.
- Set `XGEN_EMBEDDING_BASE_URL` to point at the embedding microservice (defaults to `http://embedding-service:8000`).
- Set `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_PASSWORD` to enable Redis-backed session persistence (optional; when unset, in-memory session tracking is used).
- These overrides are applied after the YAML is loaded, allowing per-environment secrets without modifying config files. See `.env` for examples.

## Running

```bash
cp run configs/example.yaml configs/config.yaml
```

```bash
go run ./cmd/xgen-crawler --config configs/example.yaml
```

```bash
go build ./cmd/xgen-crawler
./xgen-crawler --config <config-path>
```

- The crawler observes `robots.txt` unless a host is listed under `robots.overrides`.
- Worker concurrency, queue size, and per-domain delay protect against deadlocks and throttling.
- Run `go mod tidy` before building to fetch third-party modules (`chromedp`, `goquery`, `robotstxt`).

## Recent Enhancements

- HTML post-processor now extracts both plain text (`extracted_text`) and Markdown (`markdown`) while preserving table/list structure. These are stored with the cleaned HTML for downstream indexing.
- Storage layer tracks content fingerprints and marks rows that need re-indexing when the cleaned payload changes.
- Session-aware crawler engine emits structured progress events and supports multiple parallel runs keyed by seed host.
- REST API (`cmd/api`) allows external control: create/cancel sessions, stream Server-Sent Events, and retrieve configuration snapshots.
- Embedded OpenAPI 3.1 spec (`/openapi.yaml`) and Swagger UI (`/docs`) document all endpoints.
- Qdrant vector store integration embeds each page’s Markdown via the embedding microservice and stores points in per-session collections.

## API Server

Start the HTTP control plane with:

```bash
GOCACHE=$(pwd)/.gocache go run ./cmd/api \
  --config configs/config.yaml \
  --addr 0.0.0.0:9010
```

- Override maximum concurrent sessions via `--max-concurrency` or `CRAWLER_MAX_CONCURRENCY`.
- Health check: `GET /health`
- OpenAPI spec: `GET /openapi.yaml`
- Interactive docs: `GET /docs`

### Launching a Crawl via API

```bash
curl -X POST http://localhost:9010/api/sessions \
  -H 'Content-Type: application/json' \
  -d '{
        "seed_url": "https://example.com",
        "depth": 2,
        "user_agent": "crawler-bot/1.0",
        "allowed_domains": ["example.com"],
        "remove_ads": true,
        "remove_scripts": true,
        "remove_styles": false,
        "trim_whitespace": true,
        "robots": { "respect": true },
        "media": { "enabled": false },
        "vector_db": {
          "provider": "qdrant",
          "endpoint": "http://qdrant:6333",
          "index": "news-pages",
          "namespace": "default",
          "dimension": 1024,
          "embedding_model": "Qwen/Qwen3-Embedding-0.6B",
          "upsert_batch_size": 64
        }
      }'
```

Monitor progress with Server-Sent Events:

```bash
curl http://localhost:9010/api/sessions/{session_id}/events
```

Cancel an active crawl:

```bash
curl -X POST http://localhost:9010/api/sessions/{session_id}/cancel
```

Sync indexed pages into the chat document database (requires prior indexing and the same identity headers):

```bash
curl -X POST http://localhost:9010/api/crawler/sessions/{session_id}/documents \
  -H 'X-User-ID: 42' \
  -H 'X-User-Name: crawler-admin'
```

### Notes

- `media.enabled` requires a reachable relational database (configured under `db.*`). Disable it if you are running without persistence.
- The API reuses the provided base configuration; only supplied fields are overridden per session.
- SSE payloads include processed/queued counts and the most recent URL/domain, enabling live progress indicators.
- Include `X-User-ID` and `X-User-Name` headers on `POST /api/sessions`; these values are forwarded to the embedding service for authorization and vector-dimension discovery.

## Docker

Build the container image locally:

```bash
docker build -t xgen-crawler-api .
```

Run with the provided environment defaults (override any value in `.env` as needed):

```bash
docker run --env-file .env \
  -p "${PORT:-9010}:9010" \
  xgen-crawler-api
```

Mount a custom configuration file if desired:

```bash
docker run --env-file .env \
  -v $(pwd)/configs/config.yaml:/app/configs/config.yaml:ro \
  -p 9010:9010 \
  xgen-crawler-api
```

The container invokes `/entrypoint.sh`, which reads `PORT`, `CONFIG_PATH`, and `MAX_CONCURRENCY` to launch the API binary.
