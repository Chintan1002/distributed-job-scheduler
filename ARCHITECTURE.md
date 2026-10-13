# Architecture — Distributed Job Scheduler (Go)

## 1. System context
```
   ┌─────────┐     REST      ┌──────────────┐
   │ Clients │ ───────────▶  │  API Server  │
   └─────────┘               └──────┬───────┘
                                    │ persist
                                    ▼
   ┌──────────────┐   read/write  ┌──────────────┐   push   ┌─────────────┐   pull   ┌──────────┐
   │  PostgreSQL  │◀────────────▶ │  Scheduler   │ ───────▶ │ Redis Queue │ ───────▶ │ Workers  │
   │ (truth)      │               │ (leader)     │          └─────────────┘          │ (N)      │
   └──────────────┘               └──────┬───────┘                                    └────┬─────┘
        ▲                                │ leader lease + fencing token                    │
        └────────────────────────────────┴──────────── write run result ──────────────────┘
```

## 2. Components and responsibilities

**API Server** — stateless HTTP. Validates requests, writes/reads job definitions in Postgres. Never executes jobs. Scales horizontally behind a load balancer.

**Scheduler** — the coordinator. Runs on ≥2 nodes but only the **leader** is active. Responsibilities:
- Hold the leader lease (Redis) and maintain a monotonic **fencing token**.
- Load jobs due in the next window from Postgres into an in-memory **hierarchical timing wheel**.
- On each tick, move expired jobs to the Redis **ready queue** and mark them `queued`.
- On job success, evaluate **DAG** children and schedule any whose parents are all done.

**Workers** — stateless executors. Pull from the ready queue, claim a job with the current fencing token, run the payload, and write the outcome to `job_runs` (conditional on the token). Handle retries with backoff. Scale horizontally.

**PostgreSQL** — durable source of truth: `jobs` (definitions/schedules/status) and `job_runs` (attempt history + fencing tokens). Chosen for durability and transactional conditional updates.

**Redis** — two roles: (1) the leader-election lease (`SET NX EX`), (2) the transient ready queue (list or stream). Chosen for speed and atomic primitives.

## 3. Key data flows

**Schedule a job**
1. `POST /jobs` → API validates, inserts row (`status=scheduled`) in Postgres, returns 201.
2. Leader scheduler later loads it into the timing wheel as its `run_at` approaches.

**Fire a due job (exactly-once path)**
1. Wheel tick expires the job → scheduler pushes `{job_id, fencing_token}` to Redis queue, sets `status=queued`.
2. A worker pops it, inserts a `job_runs` row and updates `jobs.status=running` **only if** `fencing_token >= last_seen_token` (conditional update). A stale token is rejected — this is the exactly-once guard.
3. Worker executes; writes `outcome=success|error`, sets `jobs.status` accordingly.

**Leader handover**
1. Leader stops renewing lease (crash/pause) → Redis key expires.
2. A standby wins `SET NX EX`, increments the fencing token, becomes leader, reloads due jobs into its wheel.
3. Any write from the old (zombie) leader now carries a lower token and is refused by Postgres.

**DAG completion**
1. Job A succeeds → scheduler finds jobs listing A in `depends_on`.
2. For each, if *all* parents are now `succeeded`, it's scheduled (put in the wheel). Cycles were rejected at creation.

## 4. Design decisions & tradeoffs
- **Timing wheel over min-heap/polling**: O(1) insert/tick vs O(log n)/O(n). Cost: bounded time horizon → we reload far-future jobs from Postgres periodically.
- **Single active leader over multi-writer**: simpler correctness (no distributed consensus needed beyond a lease). Cost: leader throughput ceiling → mitigated by workers doing the heavy lifting; sharding is a stretch goal.
- **Redis lease over Raft/etcd**: far simpler to build and explain. Cost: relies on Redis clock/TTL; fencing tokens cover the zombie-leader gap.
- **At-least-once + fencing over true exactly-once delivery**: the only achievable model in distributed systems. We make effects idempotent.
- **Postgres for truth, Redis for coordination**: durability where it matters, speed where it matters.

## 5. Failure modes and responses
| Failure | Effect | Mitigation |
|---------|--------|-----------|
| Worker dies mid-job | Job not completed | Job re-appears/retried; another worker runs it |
| Leader dies | No jobs fire briefly | Standby takes over within lease TTL |
| Zombie leader (paused then wakes) | Risk of double-fire | Fencing token rejects stale writes |
| Redis down | No queueing/election | System pauses scheduling; Postgres still safe; alert + restart |
| Postgres down | No truth store | Hard stop; do not fabricate state; alert |
| Downstream service failing | Jobs error | Exponential backoff + jitter, capped by max_retries |

## 6. Observability
- Metrics: `jobs_scheduled_total`, `jobs_run_total`, `jobs_failed_total`, `scheduler_lag_seconds` (now − expected fire time), queue depth, leader identity.
- Logs: one structured line per state transition (`scheduled→queued→running→succeeded/failed`).
- Alerts: scheduler_lag high, no active leader, queue depth growing unbounded.

## 7. Scaling notes
- API and workers scale horizontally with no coordination.
- The scheduler is the single-writer bottleneck; vertical scale first, then partition the job keyspace across multiple leaders (each owning a shard) as a stretch goal.
