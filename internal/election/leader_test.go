package election_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Chintan1002/distributed-job-scheduler/internal/election"
	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/queue"
	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
	"github.com/Chintan1002/distributed-job-scheduler/internal/worker"
)

// Integration tests against real Redis (and Postgres for the end-to-end
// failover test). Short TTLs keep them fast; each test uses its own lease
// key so runs can't interfere.

const (
	testRedisURL    = "redis://localhost:6379/0"
	testDatabaseURL = "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"
)

func redisURL() string {
	if v := os.Getenv("REDIS_URL"); v != "" {
		return v
	}
	return testRedisURL
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func newElector(t *testing.T, key, id string, ttl time.Duration) *election.Elector {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	e, err := election.New(ctx, redisURL(), key, id, ttl, testLogger())
	if err != nil {
		t.Skipf("redis unavailable, skipping integration test (run `make infra` first): %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// TestMutualExclusionAndGracefulHandover: two nodes compete; at no point are
// both leading. When the leader steps down (graceful release), the standby
// takes over well inside the TTL.
func TestMutualExclusionAndGracefulHandover(t *testing.T) {
	key := "test:leader:" + uuid.NewString()
	const ttl = time.Second

	var leading int32 // how many nodes think they lead right now
	var violations int32
	var aLed, bLed atomic.Bool

	var lastToken atomic.Int64
	lead := func(flag *atomic.Bool) func(context.Context, int64) {
		return func(ctx context.Context, token int64) {
			if atomic.AddInt32(&leading, 1) > 1 {
				atomic.AddInt32(&violations, 1)
			}
			// Tokens must be strictly monotonic across leaderships (M5).
			if prev := lastToken.Load(); token <= prev {
				t.Errorf("fencing token went backwards: %d after %d", token, prev)
			}
			lastToken.Store(token)
			flag.Store(true)
			<-ctx.Done()
			atomic.AddInt32(&leading, -1)
		}
	}

	a := newElector(t, key, "node-a", ttl)
	b := newElector(t, key, "node-b", ttl)

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	t.Cleanup(cancelA)
	t.Cleanup(cancelB)
	go func() { _ = a.Run(ctxA, lead(&aLed)) }()
	time.Sleep(100 * time.Millisecond) // let A win deterministically
	go func() { _ = b.Run(ctxB, lead(&bLed)) }()

	time.Sleep(1500 * time.Millisecond) // both compete for a while
	if !aLed.Load() {
		t.Fatal("node A never became leader")
	}
	if bLed.Load() {
		t.Fatal("node B led while A held the lease")
	}

	// Graceful handover: kill A, B must take over quickly (no TTL wait).
	start := time.Now()
	cancelA()
	deadline := time.Now().Add(ttl + 2*time.Second)
	for !bLed.Load() {
		if time.Now().After(deadline) {
			t.Fatal("node B never took over after A stepped down")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("graceful handover took %v", time.Since(start))

	cancelB()
	time.Sleep(100 * time.Millisecond)
	if v := atomic.LoadInt32(&violations); v != 0 {
		t.Fatalf("%d moments with two concurrent leaders", v)
	}
}

// TestCrashFailoverWithinTTL is the M4 exit criterion: the leader dies
// WITHOUT releasing (a crash — the key must expire on its own), and the
// standby takes over within the lease window.
func TestCrashFailoverWithinTTL(t *testing.T) {
	key := "test:leader:" + uuid.NewString()
	const ttl = 1500 * time.Millisecond

	// "Leader" A grabs the lease and immediately crashes: no renewals, no
	// release. The raw TryAcquire models this — nothing will clean up its key.
	a := newElector(t, key, "crashed-leader", ttl)
	ok, err := a.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("A could not acquire: ok=%v err=%v", ok, err)
	}

	var bLed atomic.Bool
	b := newElector(t, key, "standby", ttl)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	start := time.Now()
	go func() {
		_ = b.Run(ctx, func(leadCtx context.Context, _ int64) {
			bLed.Store(true)
			<-leadCtx.Done()
		})
	}()

	deadline := time.Now().Add(ttl + 3*time.Second)
	for !bLed.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("standby did not take over within TTL + slack")
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	t.Logf("crash failover took %v (ttl=%v)", elapsed, ttl)

	// It must NOT have led while the crashed leader's lease was still live.
	if elapsed < ttl-(ttl/renewDiv)-100*time.Millisecond {
		t.Fatalf("standby led after only %v — before the lease could expire (ttl %v)", elapsed, ttl)
	}
}

const renewDiv = 3 // mirrors the elector's campaign interval for the bound above

// TestSchedulerFailoverEndToEnd: two full schedulers behind electors, one
// worker. Jobs run under leader A; after A dies, new jobs still run — fired
// by standby B. This is the M4 invariant end to end: firing never needs a
// particular node, only *a* leader.
func TestSchedulerFailoverEndToEnd(t *testing.T) {
	ctx := context.Background()
	log := testLogger()
	leaderKey := "test:leader:" + uuid.NewString()
	const ttl = time.Second

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = testDatabaseURL
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Skipf("postgres unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(st.Close)

	q, err := queue.New(ctx, redisURL(), "jobs:ready:test:"+uuid.NewString())
	if err != nil {
		t.Skipf("redis unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	newWheel := func(token int64) *scheduler.WheelScheduler {
		return &scheduler.WheelScheduler{
			Loader:    st,
			Dispatch:  &scheduler.QueueDispatcher{Store: st, Queue: q, Token: token, Log: log},
			Tick:      50 * time.Millisecond,
			Lookahead: 10 * time.Second,
			Reload:    300 * time.Millisecond,
			Log:       log,
		}
	}
	startNode := func(id string, led *atomic.Bool) context.CancelFunc {
		e := newElector(t, leaderKey, id, ttl)
		nodeCtx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go func() {
			_ = e.Run(nodeCtx, func(leadCtx context.Context, token int64) {
				led.Store(true)
				_ = newWheel(token).Run(leadCtx)
			})
		}()
		return cancel
	}

	var aLed, bLed atomic.Bool
	cancelA := startNode("sched-a", &aLed)
	time.Sleep(150 * time.Millisecond) // A wins first
	startNode("sched-b", &bLed)

	// One worker executes whatever any leader enqueues.
	wctx, wcancel := context.WithCancel(context.Background())
	t.Cleanup(wcancel)
	w := &worker.Worker{ID: "w1", Store: st, Queue: q, Exec: &scheduler.PayloadLogger{Log: log}, Log: log}
	go func() { _ = w.Run(wctx) }()

	makeJob := func(name string) *model.Job {
		runAt := time.Now().UTC().Add(-time.Second)
		job := &model.Job{
			ID: uuid.NewString(), Name: name, Payload: json.RawMessage(`{"task":"noop"}`),
			ScheduleType: model.ScheduleOnce, RunAt: &runAt, DependsOn: []string{},
			MaxRetries: 3, Status: model.StatusScheduled,
		}
		if err := st.CreateJob(ctx, job); err != nil {
			t.Fatalf("create job: %v", err)
		}
		return job
	}
	waitSucceeded := func(job *model.Job, timeout time.Duration) {
		deadline := time.Now().Add(timeout)
		for {
			got, err := st.GetJob(ctx, job.ID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if got.Status == model.StatusSucceeded {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %s (%s) still %q after %v", job.Name, job.ID, got.Status, timeout)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Under leader A.
	waitSucceeded(makeJob("m4-under-leader-a"), 10*time.Second)
	if !aLed.Load() || bLed.Load() {
		t.Fatalf("expected A leading, B standing by (aLed=%v bLed=%v)", aLed.Load(), bLed.Load())
	}

	// Kill A. B must take over and fire new jobs.
	cancelA()
	waitSucceeded(makeJob("m4-after-failover"), ttl+15*time.Second)
	if !bLed.Load() {
		t.Fatal("standby B never became leader")
	}
}

// TestHandoverChaosNoDoubleRuns is the M5 exit criterion: a batch of jobs
// comes due exactly while leadership changes hands, and every single one
// runs at most once (in fact exactly once — nothing may be lost either).
func TestHandoverChaosNoDoubleRuns(t *testing.T) {
	ctx := context.Background()
	log := testLogger()
	leaderKey := "test:leader:" + uuid.NewString()
	const ttl = time.Second

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = testDatabaseURL
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Skipf("postgres unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(st.Close)

	q, err := queue.New(ctx, redisURL(), "jobs:ready:test:"+uuid.NewString())
	if err != nil {
		t.Skipf("redis unavailable, skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	startNode := func(id string) context.CancelFunc {
		e := newElector(t, leaderKey, id, ttl)
		nodeCtx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go func() {
			_ = e.Run(nodeCtx, func(leadCtx context.Context, token int64) {
				s := &scheduler.WheelScheduler{
					Loader:    st,
					Dispatch:  &scheduler.QueueDispatcher{Store: st, Queue: q, Token: token, Log: log},
					Tick:      50 * time.Millisecond,
					Lookahead: 10 * time.Second,
					Reload:    200 * time.Millisecond,
					Log:       log,
				}
				_ = s.Run(leadCtx)
			})
		}()
		return cancel
	}

	// 20 jobs coming due across the handover window.
	const nJobs = 20
	jobs := make([]*model.Job, nJobs)
	base := time.Now().UTC()
	for i := range jobs {
		runAt := base.Add(time.Duration(i) * 100 * time.Millisecond) // 0–1.9s from now
		jobs[i] = &model.Job{
			ID: uuid.NewString(), Name: "m5-chaos", Payload: json.RawMessage(`{"task":"noop"}`),
			ScheduleType: model.ScheduleOnce, RunAt: &runAt, DependsOn: []string{},
			MaxRetries: 3, Status: model.StatusScheduled,
		}
		if err := st.CreateJob(ctx, jobs[i]); err != nil {
			t.Fatalf("create job: %v", err)
		}
	}

	cancelA := startNode("chaos-a")
	time.Sleep(150 * time.Millisecond)
	startNode("chaos-b")

	// Two workers drain the queue throughout.
	for i := 1; i <= 2; i++ {
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		w := &worker.Worker{ID: "w" + uuid.NewString()[:4], Store: st, Queue: q,
			Exec: &scheduler.PayloadLogger{Log: log}, Log: log}
		go func() { _ = w.Run(wctx) }()
	}

	// Kill the leader right in the middle of the due window.
	time.Sleep(800 * time.Millisecond)
	cancelA()
	t.Log("killed leader A mid-window")

	// Every job must end up succeeded...
	deadline := time.Now().Add(25 * time.Second)
	for _, job := range jobs {
		for {
			got, err := st.GetJob(ctx, job.ID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if got.Status == model.StatusSucceeded {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %s still %q — lost across the handover", job.ID, got.Status)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	// ...with exactly one run each: at-most-once even during the handover.
	for _, job := range jobs {
		runs, err := st.RunsForJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("list runs: %v", err)
		}
		if len(runs) != 1 {
			t.Fatalf("job %s ran %d times during handover, want exactly 1", job.ID, len(runs))
		}
	}
}
