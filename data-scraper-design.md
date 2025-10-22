# Data Scraper Feature Design

## 1. Overview
- 목표: 관리자 UI에 구현된 스크래퍼/데이터 레이크 화면을 실제 동작 서비스로 연결.
- 구성요소: FastAPI 기반 백엔드, Go 기반 `newscrawler` 엔진, Postgres & MinIO & Redis 스토리지, 프론트엔드.
- 요구사항 핵심:
  - 스크래퍼 CRUD + robots 확인 + 실행/테스트 요청.
  - 대시보드 통계와 실행 로그.
  - 수집 데이터 브라우징 및 파싱.
  - 다중 데이터 소스 타입(web/api/db/email/document 등) 확장성.

## 2. High-Level Architecture
```
Admin UI → FastAPI (admin/data-scraper) → Job Scheduler / Queue → Go newscrawler
                                      ↓                               ↓
                                 Postgres (metadata)           MinIO (binaries)
                                      ↓                               ↓
                             Redis (job state/cache) ←───── Go engine footprint
                                      ↓
                          Data Lake & Vector pipeline (optional)
```

### 주요 서비스 모듈
1. **ScraperService**: 설정 CRUD, 로봇 검사, job 생성, 통계 집계.
2. **RunExecutor**: 실행 요청 수신 → Config 템플릿 생성 → 엔진 실행 → 상태 업데이트.
3. **IngestionWorker**: 엔진 결과 ingestion, MinIO 업로드, Postgres 기록, 집계 큐잉.
4. **StatsAggregator**: 실행/데이터 통계 계산 및 캐시.
5. **DataLakeService**: 목록 조회, 필터, 상세, 파싱/후처리.

## 3. Backend Integration
### 라우터 구조
- `controller/admin/data_scraper_controller.py` (신규)
  - `@router.get("/scrapers")`, `.post`, `.patch`, `.delete`.
  - `/scrapers/{id}/run`, `/scrapers/{id}/test`, `/scrapers/{id}/robots`.
  - `/stats`, `/runs`, `/runs/{id}`, `/datalake/items`, `/datalake/items/{id}` 등.
- 기존 `admin` 권한 체크에 `data-scraper` 섹션을 포함.

### 서비스 레이어
- `service/data_scraper` 패키지 신규: repository, service, scheduler, workers.
- 실행 상태 저장은 Redis hash (`scraper:run:{id}`) + Postgres `scraper_runs`.
- 스케줄/큐 관리: FastAPI 인스턴스에 APScheduler(필요 시 Redis JobStore) 연결해 interval 실행과 대기열을 제어한다. Redis가 이미 구축돼 있으므로 상태 캐시·락·Pub/Sub에 활용한다.
- 실행 파이프라인: APScheduler 또는 수동 호출이 RunExecutor에 작업을 enqueue → subprocess로 Go 바이너리 실행 → 완료 후 IngestionWorker.

## 4. Go Crawler Orchestration
- 실행 환경: CI 파이프라인에서 `newscrawler` 정적 바이너리를 미리 빌드해 아티팩트(S3/MinIO 또는 내부 패키지 리포지토리)에 업로드한다. 배포 시 FastAPI 백엔드와 동일 VM/컨테이너에 해당 버전을 내려받아 배치하고, 실행 시에는 해당 바이너리를 직접 subprocess로 호출한다(DoD 불필요, 롤백이 용이).
- 실행 시점에 ScraperConfig → `configs/runtime-{run_id}.yaml` 생성.
- 설정 매핑:
  - `endpoint` → `crawl.seeds[0].url`.
  - `maxDepth` → `crawl.seeds[0].max_depth` & `crawl.max_depth`.
  - `respectRobotsTxt` → `robots.respect`.
  - `followLinks` → `crawl.discovery.respect_nofollow` false 시 링크 추적.
  - `userAgent`, `headers`, `parsingMethod` → 각각 매핑.
  - `interval`은 scheduler 에 저장.
- 실행 명령: 배포된 바이너리를 직접 실행 (예: `./newscrawler --config <file>`).
- 결과 수집:
  - 엔진에서 pages/images 테이블에 저장되는 데이터를 run_id/scraper_id로 구분해야 하므로 저장 스키마 확장 필요(메타데이터 JSON에 `scraper_id`, `run_id` 삽입). Storage 공유 전략은 Go 엔진이 Postgres/MinIO에 직접 쓰고 Python IngestionWorker가 동일 저장소를 읽는 형태로 유지하며, 필요 시 Redis Pub/Sub 이벤트로 ingestion 트리거만 추가한다.
  - Go 엔진 storage 계층 수정 포인트:
    - `types.CrawlRequest`에 `ScraperID` 필드 추가.
    - `storage.Document.Metadata`에 `scraper_id`, `run_id`, `seed_label` 기록.
    - SQL schema (pages) 에 컬럼 추가(`scraper_id`, `run_id`).

## 5. Database Schema (Postgres 기준)

### `scraper_configs`
| 필드 | 타입 | 설명 |
| --- | --- | --- |
| id | UUID PK | 스크래퍼 ID |
| tenant_id | UUID nullable | 향후 멀티 테넌트 구분용 |
| name | VARCHAR | 표시 이름 |
| endpoint | TEXT | seed URL/API/db 연결 문자열 |
| data_source_type | VARCHAR | `web/api/database/document/email/image` |
| parsing_method | VARCHAR | `html/json/xml/csv/raw` |
| schedule_interval_minutes | INT | 주기 (분) |
| max_depth | INT | 웹 전용 깊이 |
| follow_links | BOOLEAN | 링크 추적 |
| respect_robots | BOOLEAN | robots 준수 |
| user_agent | TEXT | UA 문자열 |
| headers | JSONB | 추가 헤더 |
| authentication | JSONB | 인증 정보 (type, credentials) |
| filters | JSONB | include/exclude 패턴 등 |
| enable_vector_pipeline | BOOLEAN | Vector 파이프라인 연동 여부(기본 false) |
| status | VARCHAR | `idle/running/error/paused` |
| last_run_at | TIMESTAMPTZ | 최근 실행 시각 |
| created_at/updated_at | TIMESTAMPTZ | 타임스탬프 |

### `scraper_runs`
| 필드 | 타입 | 설명 |
| id | UUID PK |
| tenant_id | UUID nullable |
| scraper_id | FK → `scraper_configs` |
| trigger_type | VARCHAR (`manual/schedule/test`) |
| status | VARCHAR (`pending/running/completed/failed/cancelled`) |
| started_at / finished_at | TIMESTAMPTZ |
| duration_seconds | INT |
| items_collected | INT |
| items_failed | INT |
| config_snapshot | JSONB |
| error_log | JSONB |

### `scraped_items`
| 필드 | 타입 |
| id | UUID PK |
| tenant_id | UUID nullable |
| scraper_id | FK |
| run_id | FK |
| source_url | TEXT |
| final_url | TEXT |
| title | TEXT |
| content_type | VARCHAR |
| raw_path | TEXT (MinIO 경로) |
| clean_path | TEXT |
| preview_text | TEXT |
| metadata | JSONB (author, publishedAt 등) |
| parsing_method | VARCHAR |
| size_bytes | BIGINT |
| collected_at | TIMESTAMPTZ |

### `scraped_assets`
| 필드 | 타입 |
| id | UUID PK |
| tenant_id | UUID nullable |
| item_id | FK → scraped_items |
| asset_type | VARCHAR (`image`, `attachment`) |
| stored_path | TEXT |
| content_type | VARCHAR |
| size_bytes | BIGINT |
| width/height | INT |

### `scraper_stats` (materialized view or table)
| 필드 | 타입 |
| scraper_id | FK |
| total_runs | INT |
| successful_runs | INT |
| failed_runs | INT |
| total_items | BIGINT |
| total_size_bytes | BIGINT |
| avg_duration_seconds | NUMERIC |
| last_run_at | TIMESTAMPTZ |

### 인덱스 & 기타
- `scraper_runs(scraper_id, started_at DESC)` BTREE.
- `scraped_items(scraper_id, collected_at DESC)` BTREE + GIN(`metadata`).
- Full-text search 컬럼(ko/eng tokenizer) 고려.

## 6. API Contract

### Scraper CRUD
- `GET /api/admin/data-scraper/scrapers?page=1&page_size=20&sort=-updated_at`
  - 응답: `{ items: ScraperConfig[], total, stats? }`
- `POST /api/admin/data-scraper/scrapers`  
  Request body: Admin UI 입력 필드와 동일.
- `GET /api/admin/data-scraper/scrapers/{id}`
- `PATCH /api/admin/data-scraper/scrapers/{id}`
- `DELETE /api/admin/data-scraper/scrapers/{id}`

### Robots 검사
- `POST /api/admin/data-scraper/scrapers/robots-check` `{ endpoint, user_agent }`
  - 응답: `{ allowed: bool, crawlDelay, disallowedPaths[], message }`
  - 구현: Go `internal/robots` 라이브러리를 재사용하거나 Python `robotexclusionrulesparser` 사용.

### 실행 관련
- `POST /api/admin/data-scraper/scrapers/{id}/run` `{ trigger: manual|schedule }`
- `POST /api/admin/data-scraper/scrapers/{id}/test` (샘플 수집 후 삭제)
- `POST /api/admin/data-scraper/scrapers/{id}/cancel`
- `GET /api/admin/data-scraper/runs?scraper_id=&status=&page=1`
- `GET /api/admin/data-scraper/runs/{run_id}`

### 통계
- `GET /api/admin/data-scraper/stats/summary`
  - 반환: 총 스크래퍼 수, 활성 스크래퍼, 수집 항목 총합, 데이터 크기, 성공률 등 (프론트 통계 카드용).
- `GET /api/admin/data-scraper/stats/{scraper_id}`

### 데이터 레이크
- `GET /api/admin/data-scraper/datalake/items?scraper_id=&content_type=&parsing_method=&start=&end=&search=`
- `GET /api/admin/data-scraper/datalake/items/{item_id}`
- `POST /api/admin/data-scraper/datalake/items/{item_id}/parse` `{ method }`
  - 파싱 결과 반환: `{ success, data, error }`
- Vector 파이프라인: IngestionWorker가 스크래퍼 설정(`enable_vector_pipeline`)과 컨텐츠 타입 규칙을 확인해 필요 시 VectorManager 큐에 등록한다(기본값 비활성).

### 응답 DTO
- 공통: ISO8601 타임스탬프, 사이즈는 bytes, 다국어 스키마(ko-ui).
- 실패시: `{ detail, code, metadata }`

## 7. Execution Flow
1. **Create**: Admin UI → POST → DB insert → Redis scheduler 등록(optional).
2. **Run Trigger**:
   - Manual: Admin UI → POST `/run`.
   - Scheduled: APScheduler/Redis queue가 interval 스캔.
3. **Job Preparation**:
   - Scraper config 로드 → runtime YAML 생성 → subprocess 실행 → `scraper_runs` 레코드 `running`.
4. **Crawler Execution**:
   - Go 엔진이 페이지 수집 → Storage(writer) 확장 덕분에 scraper_id/run_id & metadata 저장 → Raw HTML은 MinIO 경로 기록.
5. **Ingestion**:
   - 실행 완료 후 Python worker가 pages 테이블 읽어 `scraped_items`/`scraped_assets`로 변환, 필터/파싱, 미디어 저장, 요약 생성.
   - `enable_vector_pipeline`이 활성화되고 컨텐츠 조건을 만족하면 VectorManager/DocumentProcessor 파이프라인에 큐잉.
6. **Completion**:
   - `scraper_runs` 상태 업데이트, Redis hash 상태 갱신, 통계 집계 업데이트, 알림 발송(optional).
7. **DataLake/UI**:
   - 프론트가 새로운 API에서 목록/통계/상세 데이터를 가져와 렌더.

## 8. Security & Ops Considerations
- Go 엔진 배포: CI에서 버전 태깅된 정적 바이너리를 빌드해 S3/MinIO 등에 저장하고, 배포 시 받아와 교체한다(롤백 시 바이너리 스왑).
- 인증 정보 암호화: `authentication` JSON 암호화 저장(예: Fernet) 및 반환 시 마스킹.
- 도메인 화이트리스트/블랙리스트 정책.
- Rate limiting / concurrency 제어: scraper 별 동시 실행 제한.
- 장기 실행 타임아웃, 실패 재시도 간격.
- Monitoring: run 로그를 Elastic/Prometheus로 전송, alert 구성. Redis Pub/Sub 또는 Stream으로 경량 알림 가능.
- MinIO/Redis/DB 연결 실패 시 graceful degrade.
- GDPR/개인정보: 수집 도메인별 규정 준수, robots.txt 및 법적 제한.

## 9. Implementation Roadmap
1. **모델 & 마이그레이션**
   - FastAPI 모델 정의 (`service/database/models/scraper.py` 등), Alembic or custom migration. 주요 테이블/MinIO 경로에 `tenant_id`/`project_id` nullable 컬럼을 함께 생성해 향후 멀티 테넌트 확장 준비.
2. **API 라우터 기본기능**
   - CRUD, stats stub, run stub.
   - 프론트 Mock 제거 → 실 API 호출 전환 (기능 flag 고려).
3. **엔진 어댑터**
   - Go engine config generator, subprocess wrapper, 결과 polling.
4. **데이터 적재**
   - Go storage schema 확장 → Python ingestion → DataLake API 구현.
5. **통계/집계 & UI 연동**
   - dashboard metrics, run history, DataLake parse.
6. **Scheduler & Multi-source 확장**
   - APScheduler + Redis 백엔드로 interval run/cancel/큐 관리, 실패 재시도/락 정교화.
   - API/DB/email/doc connector 설계 (추가 모듈).
7. **Observability & Hardening**
   - 로그 수집, 알림, 에러 handling, 보안 점검.

## 10. Open Questions
- APScheduler 인스턴스 다중 노드 운영 시 리더 선출/중복 실행 방지 전략.
- Go 엔진 실행 로그/러닝타임 메트릭을 어떤 방식으로 수집·보관할지(예: structured log → Loki/ELK).
- IngestionWorker의 재처리 전략(부분 실패, 중복 수집 방지) 및 모니터링.
- 다중 데이터 소스(API/DB/email 등) 커넥터 구현 우선순위와 공통 추상화.
- 향후 다중 테넌트 활성화 시 권한 모델 및 UI 전환 플랜.
