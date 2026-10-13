# Distributed Job Scheduler (Go)

A fault-tolerant, distributed "cron-as-a-service" written in Go. Clients schedule jobs over a REST API, and the system runs them reliably across many worker processes, even when machines crash or the coordinating scheduler fails over to a standby.

It is built around three guarantees:

1. **A due job runs.** Schedules are committed to PostgreSQL before the API acknowledges them, so nothing is lost across restarts.
2. **A job runs at most once per due time.** Conditional database updates guarded by **fencing tokens** reject duplicate and stale executions, including writes from a "zombie" leader.
3. **The system survives node failure.** Schedulers elect a leader through a Redis lease, and a standby takes over within the lease window.

---

## Status

The project is built milestone by milestone. **M1–M6 are complete and tested. M7 and M8 are planned but not yet built.**

| Milestone | Feature | Status |
|---|---|---|
| M1 | REST API, Postgres persistence, one-shot scheduling | ✅ Done |
| M2 | Hierarchical timing wheel replacing database polling (O(1) per tick) | ✅ Done |
| M3 | Redis ready-queue and a separate horizontally-scalable worker binary | ✅ Done |
| M4 | Leader election through a Redis lease (`SET NX EX`) with renewal | ✅ Done |
| M5 | Exactly-once effect through fencing tokens and conditional writes | ✅ Done |
| M6 | Retries with exponential backoff and jitter | ✅ Done |
| M7 | DAG job dependencies with cycle detection | ⏳ Planned |
| M8 | Prometheus metrics, containerized services, 10k-job chaos test | ⏳ Planned |

---

## Architecture

```
                 ┌──────────────┐
   client  ───▶  │  API Server  │   REST: create / get / cancel jobs (stateless)
                 └──────┬───────┘
                        │ writes job (status = scheduled)
                        ▼
                 ┌──────────────┐        ┌────────────────────┐
                 │  PostgreSQL  │◀──────▶│  Scheduler × N     │  only the LEADER is active
                 │ (source of   │        │  (timing wheel)    │  (Redis lease + fencing token)
                 │  truth)      │        └─────────┬──────────┘
                 └──────────────┘                  │ pushes {job_id, fencing_token}
                        ▲                          ▼
                        │                 ┌────────────────────┐
                        │                 │  Redis ready queue │
                        │                 └─────────┬──────────┘
                        │                           │ blocking pull
                 ┌──────┴───────────────────────────┴──────┐
                 │   Worker 1     Worker 2     Worker 3     │  claim (fenced) → execute → record
                 └──────────────────────────────────────────┘
```

**Job lifecycle:** `scheduled → queued → running → succeeded`, or `failed` after retries run out. A failed attempt with retries left goes `running → scheduled` with a backed-off `run_at`. A scheduled job can also move to `cancelled`.

| Component | Role |
|---|---|
| **API server** (`cmd/api`) | Validates requests and persists jobs to Postgres. Never executes jobs. |
| **Scheduler** (`cmd/scheduler`) | Runs on several nodes; only the lease holder's timing wheel fires jobs. Periodically loads soon-due jobs from Postgres into the wheel and pushes due jobs to the Redis queue. |
| **Worker** (`cmd/worker`) | Pulls from the queue, claims the job in Postgres with the message's fencing token, executes it, records the run, and reschedules on failure. |
| **PostgreSQL** | Durable truth: job definitions, status, attempt counters, fencing high-water marks, and run history. |
| **Redis** | Leader lease, fencing-token counter, and the transient ready queue. |

---

## Engineering highlights

### 1. Hierarchical timing wheel (O(1) scheduling)
Instead of querying the database every tick, the leader loads jobs due in the next window into an in-memory **hierarchical timing wheel**. This is the structure used by the Linux kernel and Kafka. Inserting a job and advancing a tick are both O(1). Far-future jobs sit in coarser outer wheels and **cascade** into finer wheels as their time approaches, like clock hands.

- Implementation: 4 levels × 64 slots with a 100 ms tick, which covers about 19 days.
- Due times round **up** to the next tick, so a job can never fire early.
- Re-adding a pending job is ignored (dedup by job ID), so reloading an overlapping window from Postgres is safe.
- A `sync.Mutex` guards the wheel because the tick loop and the insert path share it.
- **Benchmark (Apple M4, 100k pending jobs):** one wheel tick takes **~14 ns with 0 allocations**. An in-memory polling scan over the same jobs takes **~22.8 µs**, about **1,600× slower**. Real polling is slower still, because each tick is a round trip to Postgres.

### 2. Leader election with a Redis lease
Only one scheduler may fire jobs at a time. A node becomes leader with an atomic `SET key id NX EX ttl` and renews the lease every `ttl/3`. Renewal and release use Lua scripts that check ownership, so a node can never extend or delete a lease it no longer holds. If renewals stop being confirmed, the leader **steps down on its own** before the key can expire underneath it. The entire timing wheel runs inside the leadership context, so losing the lease immediately stops all firing.

- **Measured:** after a hard crash with a 1.5 s lease, the standby took over in **1.52 s**. A graceful handover, where the leader releases the key on shutdown, took **~174 ms**.

### 3. Exactly-once effect with fencing tokens
A lease alone isn't enough. A leader can pause (GC, network partition), lose its lease, and wake up still believing it leads. To handle this, every new leadership mints a **monotonic fencing token** with Redis `INCR`, and every state-changing write in Postgres is conditional:

```sql
UPDATE jobs SET status = 'running', last_fencing_token = $token, attempt = attempt + 1
WHERE id = $id AND status = 'queued' AND last_fencing_token <= $token;
```

Job statuses cycle (a retry sends a job back to `scheduled`), so status checks alone would let a zombie's write through. The token's high-water mark only moves forward, so stale writes are rejected permanently.

- **Chaos test:** 20 jobs come due over a 2-second window, and the leader is killed in the middle of it. Every job ends `succeeded` with **exactly one** recorded run: no duplicates and no lost jobs.

### 4. Reliable hand-off to workers
- The scheduler marks a job `queued` in Postgres **before** pushing it to Redis. A crash between the two steps leaves the job stuck rather than double-fired. If the push fails, the scheduler reverts the job to `scheduled` (using `context.WithoutCancel`, so the revert survives the leader's own shutdown) and the next leader re-fires it.
- Workers claim with a conditional update, so a duplicate queue message loses the race and is dropped.
- **Measured:** 9 concurrent jobs across 3 workers split exactly **3 / 3 / 3**. After a worker was killed mid-run, the surviving workers finished every remaining job.

### 5. Retries with exponential backoff and jitter
When attempt *n* fails, the job is rescheduled after `base · 2^(n−1)`, plus random jitter of up to `base/4`, capped at a maximum. A job gets `1 + max_retries` attempts in total. Retries go back through the normal wheel → queue → worker pipeline, so they keep the same exactly-once and failover guarantees. Every attempt is recorded in `job_runs` with its attempt number, outcome, error, and fencing token.

- **Measured:** a job set to always fail, with `max_retries: 3`, retried after **1.99 s → 2.20 s → 4.10 s**, then settled in `failed` with 4 recorded attempts.

---

## Tech stack

- **Go** (goroutines, channels, `context` cancellation, `log/slog` structured logging, standard-library `net/http` routing)
- **PostgreSQL 16** via `pgx/v5`: `FOR UPDATE SKIP LOCKED`, partial indexes, conditional updates
- **Redis 7** via `go-redis/v9`: `SET NX EX` leases, Lua scripts, `LPUSH`/`BRPOP` queue, `INCR` tokens
- **Docker Compose** for local Postgres and Redis
- About 3,500 lines of Go, including tests

---

## Repository layout

```
cmd/
  api/            REST API server
  scheduler/      leader-elected timing-wheel scheduler
  worker/         queue consumer / job executor
internal/
  api/            HTTP handlers + validation
  config/         env-based configuration
  election/       Redis lease, renewal, fencing-token minting
  model/          Job / JobRun types and status constants
  queue/          Redis ready-queue (push / blocking pop)
  scheduler/      timing wheel, wheel scheduler, dispatchers, backoff, M1 poller baseline
  store/          Postgres queries (all fenced lifecycle writes live here)
  worker/         worker loop: pop → fenced claim → execute → record
migrations/       0001_init, 0002_fencing, 0003_retries
ARCHITECTURE.md   components, data flows, failure modes, tradeoffs
```

---

## Running locally

Prerequisites: Go 1.22+ and Docker Desktop.

```bash
make migrate   # start Postgres + Redis in Docker and apply migrations
make run       # API + 2 schedulers (leader + standby) + 3 workers
```

Schedule a job 10 seconds out:

```bash
curl -X POST localhost:8080/jobs -d '{
  "name": "hello",
  "schedule_type": "once",
  "run_at": "2026-10-02T12:00:10Z",
  "payload": {"task": "say_hello"},
  "max_retries": 3
}'
# → 201 {"id": "…"}

curl localhost:8080/jobs/<id>              # status, attempt count
curl -X DELETE localhost:8080/jobs/<id>    # cancel (409 if it already ran)
```

The demo executor understands `{"sleep_ms": N}` to simulate work and `{"fail": true}` to exercise retries.

### API

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Create a one-shot job → `201 {id}` |
| `GET` | `/jobs/{id}` | Job status, schedule, attempt count |
| `DELETE` | `/jobs/{id}` | Cancel a scheduled job (soft delete; run history is kept) |

---

## Testing

```bash
make test    # 31 tests (unit + integration against real Postgres/Redis)
make bench   # timing wheel vs polling benchmarks
```

- **Unit tests:** timing-wheel correctness. These cover firing at the exact due tick across cascade boundaries, sub-tick rounding, dedup, removal, large time jumps, and a 300-job randomized property test. Also backoff math and API validation.
- **Integration tests:** run against real Postgres and Redis, and skip cleanly when Docker isn't running. They cover end-to-end scheduling, cancellation, load spread across workers, worker death mid-run, lease mutual exclusion, crash failover within the TTL, zombie-leader write rejection, stale queue messages, the leader-kill chaos test, and retry/give-up behavior.
- Fencing tests were written **before** the implementation, test-first.

---

## Design decisions and tradeoffs

- **Timing wheel vs. min-heap vs. polling:** O(1) insert and tick, versus O(log n) for a heap and O(n) for polling. The cost is a bounded horizon, so the leader re-loads a lookahead window from Postgres periodically. A newly created job fires at most one reload interval late.
- **Redis lease vs. Raft/etcd:** much simpler to build and reason about. Fencing tokens cover the gap a lease leaves open (paused or zombie leaders).
- **At-least-once delivery + fencing = exactly-once *effect*:** true exactly-once delivery is impossible in a distributed system. Instead, the database acts as referee and accepts only the newest token.
- **Postgres for truth, Redis for coordination:** durability where it matters and speed where it matters. A Redis message is only a pointer; the job itself always lives in Postgres.

## Known limitations

These are planned or not yet handled:

- **Cron (recurring) schedules** and **DAG dependencies** are rejected by the API until M7.
- **No Prometheus metrics yet**, and the app services don't run in containers yet (Docker Compose currently runs only Postgres and Redis). Both are part of M8.
- A job whose worker is **hard-killed mid-execution** stays in `running`. It is visible but not automatically recovered; a lease/reaper for orphaned runs is future work.
- The fencing counter lives in Redis next to the lease. A production system would back it with a durable sequence.
- All times are UTC; clock skew between nodes is assumed small.
