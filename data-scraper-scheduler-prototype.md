# APScheduler + Redis Prototype Plan

## 1. Objective
- FastAPI 인스턴스에서 APScheduler와 Redis를 이용한 스크래퍼 실행 큐를 검증.
- 주요 검증 항목:
  - 여러 프로세스/인스턴스 실행 시 중복 실행 방지(Distributed Lock).
  - 수동 실행과 스케줄 실행이 동시에 일어날 때의 상태 일관성.
  - 실행 중 서버 재시작 시 잡 재개/정리 전략.

## 2. Minimal Prototype Architecture
```
FastAPI (uvicorn) ── APScheduler (AsyncIOScheduler)
      │                 │
      │                 ├─ RedisJobStore (existing Redis cluster)
      │                 └─ RedisLock (custom using redis-py)
      │
      └─ RunExecutor (mock) → sleep + random result
```

### 주요 컴포넌트
1. **Scheduler 초기화**
   - 앱 스타트업 시 `AsyncIOScheduler` 생성.
   - JobStore: `apscheduler.jobstores.redis.RedisJobStore`.
   - Executor: 기본 ThreadPoolExecutor(테스트 환경).
   - API 서버 종료 시 graceful shutdown.

2. **Lock & 상태 저장**
   - 실행 전 redis `SETNX scraper:{id}:lock` + TTL.
   - 실행 종료 후 lock 삭제.
   - 상태 해시는 `scraper:run:{run_id}` 구조로 유지(설계 문서 기반).
   - TTL 만료 또는 작업 실패 시 자동 정리.

3. **실행 플로우**
   - 수동 실행 endpoint → `enqueue_run(scraper_id, trigger='manual')`.
   - APScheduler interval job → 동일 enqueue 함수 호출.
   - `enqueue_run` 내부에서 Redis lock 체크 후 job 등록.
   - 실제 작업은 `RunExecutor` mock(랜덤 sleep, 성공/실패 기록)으로 대체.

4. **중복 실행 방지 시나리오**
   - 동일 scraper_id에 대한 job이 실행 중일 때 재실행 요청 → lock 실패 경고 로그 + run 기록에 `skipped_due_to_lock`.
   - Distributed 테스트: 별도 스크립트로 동시에 실행 요청(다중 프로세스).

## 3. Prototype Deliverables
1. `/prototypes/scheduler/main.py`
   - FastAPI 앱, APScheduler 초기화, Redis 연결, health endpoint.
2. `/prototypes/scheduler/run_executor.py`
   - Mock executor: lock-acquire → sleep → 상태 업데이트 → lock release.
3. `/prototypes/scheduler/redis_utils.py`
   - Lock helper, hash update helper.
4. `/prototypes/scheduler/README.md`
   - 실행 방법 (poetry/venv), 환경변수 (`REDIS_URL`), 테스트 시나리오.

## 4. Test Plan
1. **단일 인스턴스 테스트**
   - 수동 실행 연속 호출 → 두 번째 호출이 `409 Conflict` 응답인지 확인.
   - APScheduler interval job 설정(예: 1분) → 상태 해시에 주기적 업데이트 확인.
2. **다중 프로세스 테스트**
   - 두 개의 uvicorn 프로세스를 띄우고 동일 scraper 실행 요청.
   - Redis lock이 중복 실행을 막는지 검증.
3. **장애 시나리오**
   - 실행 도중 프로세스 kill → lock TTL이 만료되면 다음 실행 가능 여부.
   - Redis 장애 시 fallback 전략(예: 즉시 실패).

## 5. Next Steps After Prototype
- lock/test 결과를 종합해 실제 서비스에 적용 가능한 util 모듈 추출.
- 상태 모델(`scraper_runs`)과 연결, FastAPI 실제 라우터에 통합하는 작업 진행.
- 필요 시 Celery/RQ 마이그레이션 대비 인터페이스 추상화 여부 검토.
