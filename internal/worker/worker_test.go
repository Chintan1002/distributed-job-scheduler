package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/queue"
	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
	"github.com/Chintan1002/distributed-job-scheduler/internal/worker"
)

// M3 integration tests: scheduler → Redis ready queue → workers. They need
// Postgres AND Redis up (make infra) and skip otherwise.

const (
	testDatabaseURL = "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"
	testRedisURL    = "redis://localhost:6379/0"
)

func testDeps(t *testing.T) (*store.Store, *queue.Queue) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = testDatabaseURL
	}
	st, err := store.New(ctx, dbURL)
	if err != nil {
		t.Skipf("postgres unavailable, skipping integration test (run `make migrate` first): %v", err)
	}
	t.Cleanup(st.Close)

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = testRedisURL
	}
	// Each test gets its own queue key so runs can't interfere.
	q, err := queue.New(ctx, redisURL, "jobs:ready:test:"+uuid.NewString())
	if err != nil {
		t.Skipf("redis unavailable, skipping integration test (run `make infra` first): %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	return st, q
}

// execCounter is shared by all workers in a test: it records which worker
// executed which job, behind one mutex.
type execCounter struct {
	mu     sync.Mutex
	perJob map[string]int // job id → times executed
	perWkr map[string]int // worker id → jobs executed
}

func newExecCounter() *execCounter {
	return &execCounter{perJob: map[string]int{}, perWkr: map[string]int{}}
}

func (c *execCounter) record(workerID, jobID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.perJob[jobID]++
	c.perWkr[workerID]++
}

func (c *execCounter) jobCount(jobID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.perJob[jobID]
}

func (c *execCounter) workerCounts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for k, v := range c.perWkr {
		out[k] = v
	}
	return out
}

// countingExecutor reports executions to the shared counter, optionally
// signals starts, and simulates work.
type countingExecutor struct {
	workerID string
	counter  *execCounter
	started  chan string   // receives job id when an execution starts (optional)
	delay    time.Duration // simulated work
}

func (e *countingExecutor) Execute(ctx context.Context, job *model.Job) error {
	e.counter.record(e.workerID, job.ID)
	if e.started != nil {
		e.started <- job.ID
	}
	if e.delay > 0 {
		select {
		case <-time.After(e.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func startWorker(t *testing.T, st *store.Store, q *queue.Queue, id string, exec scheduler.Executor) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &worker.Worker{ID: id, Store: st, Queue: q, Exec: exec, Log: newTestLogger()}
	go func() { _ = w.Run(ctx) }()
	return cancel
}

func createDueJob(t *testing.T, st *store.Store, name string) *model.Job {
	t.Helper()
	runAt := time.Now().UTC().Add(-time.Second) // already due
	job := &model.Job{
		ID:           uuid.NewString(),
		Name:         name,
		Payload:      json.RawMessage(`{"task":"noop"}`),
		ScheduleType: model.ScheduleOnce,
		RunAt:        &runAt,
		DependsOn:    []string{},
		MaxRetries:   3,
		Status:       model.StatusScheduled,
	}
	if err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job
}

func waitAllSucceeded(t *testing.T, st *store.Store, jobs []*model.Job, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
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
				t.Fatalf("job %s status still %q after %v", job.ID, got.Status, timeout)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestQueuePushPopRoundTrip(t *testing.T) {
	_, q := testDeps(t)
	ctx := context.Background()

	want := queue.Message{JobID: uuid.NewString(), FencingToken: 7}
	if err := q.Push(ctx, want); err != nil {
		t.Fatalf("push: %v", err)
	}
	if n, _ := q.Len(ctx); n != 1 {
		t.Fatalf("queue depth = %d, want 1", n)
	}
	got, ok, err := q.Pop(ctx, time.Second)
	if err != nil || !ok {
		t.Fatalf("pop: ok=%v err=%v", ok, err)
	}
	if got != want {
		t.Fatalf("pop = %+v, want %+v", got, want)
	}
	// Empty queue: pop times out with ok=false, no error.
	if _, ok, err := q.Pop(ctx, 100*time.Millisecond); ok || err != nil {
		t.Fatalf("empty pop: ok=%v err=%v, want false,nil", ok, err)
	}
}

// TestLoadSpreadsAcrossWorkers is M3's core check: the full scheduler →
// queue → worker pipeline runs every job exactly once, spread over 3 workers.
func TestLoadSpreadsAcrossWorkers(t *testing.T) {
	st, q := testDeps(t)
	log := newTestLogger()
	counter := newExecCounter()

	const nJobs = 9
	jobs := make([]*model.Job, nJobs)
	for i := range jobs {
		jobs[i] = createDueJob(t, st, "spread-test")
	}

	// Real wheel scheduler with the real queue dispatcher.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &scheduler.WheelScheduler{
		Loader:    st,
		Dispatch:  &scheduler.QueueDispatcher{Store: st, Queue: q, Log: log},
		Tick:      50 * time.Millisecond,
		Lookahead: 10 * time.Second,
		Reload:    300 * time.Millisecond,
		Log:       log,
	}
	go func() { _ = s.Run(ctx) }()

	for i := 1; i <= 3; i++ {
		id := "w" + strconv.Itoa(i)
		startWorker(t, st, q, id, &countingExecutor{workerID: id, counter: counter, delay: 150 * time.Millisecond})
	}

	waitAllSucceeded(t, st, jobs, 15*time.Second)

	// Exactly-once per job.
	for _, job := range jobs {
		runs, err := st.RunsForJob(context.Background(), job.ID)
		if err != nil {
			t.Fatalf("list runs: %v", err)
		}
		if len(runs) != 1 {
			t.Fatalf("job %s has %d runs, want 1", job.ID, len(runs))
		}
		if got := counter.jobCount(job.ID); got != 1 {
			t.Fatalf("job %s executed %d times, want 1", job.ID, got)
		}
	}
	// Load spread: with 9 slow jobs and blocking pops, every worker must get
	// at least one.
	total := 0
	for id, n := range counter.workerCounts() {
		t.Logf("worker %s executed %d jobs", id, n)
		if n == 0 {
			t.Errorf("worker %s executed 0 jobs — no load spread", id)
		}
		total += n
	}
	if total != nJobs {
		t.Fatalf("workers executed %d jobs total, want %d", total, nJobs)
	}
}

// TestWorkerDiesMidRunOthersTakeOver is the M3 exit criterion: kill a worker
// while it's running a job; the jobs still in the queue are picked up and
// finished by another worker.
func TestWorkerDiesMidRunOthersTakeOver(t *testing.T) {
	st, q := testDeps(t)
	ctx := context.Background()

	const nJobs = 5
	jobs := make([]*model.Job, nJobs)
	for i := range jobs {
		jobs[i] = createDueJob(t, st, "takeover-test")
		if ok, err := st.MarkQueued(ctx, jobs[i].ID, 1); err != nil || !ok {
			t.Fatalf("mark queued: ok=%v err=%v", ok, err)
		}
		if err := q.Push(ctx, queue.Message{JobID: jobs[i].ID, FencingToken: 1}); err != nil {
			t.Fatalf("push: %v", err)
		}
	}

	// Worker A is slow and reports when it starts a job.
	started := make(chan string, nJobs)
	execA := &countingExecutor{workerID: "victim", counter: newExecCounter(), started: started, delay: 10 * time.Second}
	cancelA := startWorker(t, st, q, "victim", execA)

	// Wait until A is mid-run, then kill it.
	select {
	case victimJob := <-started:
		cancelA()
		t.Logf("killed worker A mid-run of job %s", victimJob)
	case <-time.After(10 * time.Second):
		t.Fatal("worker A never started a job")
	}

	// Worker B takes over the remaining queue.
	counterB := newExecCounter()
	execB := &countingExecutor{workerID: "survivor", counter: counterB}
	startWorker(t, st, q, "survivor", execB)

	// All jobs except A's in-flight one must succeed via B.
	deadline := time.Now().Add(15 * time.Second)
	for {
		succeeded := 0
		for _, job := range jobs {
			got, err := st.GetJob(ctx, job.ID)
			if err != nil {
				t.Fatalf("get job: %v", err)
			}
			if got.Status == model.StatusSucceeded {
				succeeded++
			}
		}
		if succeeded == nJobs-1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d remaining jobs succeeded after takeover", succeeded, nJobs-1)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := counterB.workerCounts()["survivor"]; got != nJobs-1 {
		t.Fatalf("survivor executed %d jobs, want %d", got, nJobs-1)
	}
	// A's in-flight job is failed (its context died mid-execution) — visible,
	// not silently lost. Automatic retry of such jobs arrives in M6.
	if n, _ := q.Len(ctx); n != 0 {
		t.Fatalf("queue not drained: %d messages left", n)
	}
}

// TestStaleQueueMessageDropped (M5): the same job instance appears twice in
// the queue — once from a zombie leadership (token 1), once from the current
// one (token 2). Exactly one run must happen, and it must be the fenced one.
// The stale message arrives FIRST, the nastier ordering: status alone would
// have let it claim.
func TestStaleQueueMessageDropped(t *testing.T) {
	st, q := testDeps(t)
	ctx := context.Background()

	job := createDueJob(t, st, "zombie-test")
	// Current leadership (token 2) owns the enqueue...
	if ok, err := st.MarkQueued(ctx, job.ID, 2); err != nil || !ok {
		t.Fatalf("mark queued: ok=%v err=%v", ok, err)
	}
	// ...but the zombie's message (token 1) is already sitting ahead of it.
	if err := q.Push(ctx, queue.Message{JobID: job.ID, FencingToken: 1}); err != nil {
		t.Fatalf("push stale: %v", err)
	}
	if err := q.Push(ctx, queue.Message{JobID: job.ID, FencingToken: 2}); err != nil {
		t.Fatalf("push current: %v", err)
	}

	counter := newExecCounter()
	startWorker(t, st, q, "w1", &countingExecutor{workerID: "w1", counter: counter})

	waitAllSucceeded(t, st, []*model.Job{job}, 10*time.Second)

	if got := counter.jobCount(job.ID); got != 1 {
		t.Fatalf("job executed %d times, want exactly 1", got)
	}
	runs, err := st.RunsForJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("job has %d runs, want 1", len(runs))
	}
	if runs[0].FencingToken != 2 {
		t.Fatalf("run recorded token %d, want 2 (the current leadership's)", runs[0].FencingToken)
	}
}

// failNTimesExecutor fails the first n executions of each job, then succeeds.
type failNTimesExecutor struct {
	mu    sync.Mutex
	n     int
	seen  map[string]int
	times map[string][]time.Time // execution start times per job
}

func newFailNTimes(n int) *failNTimesExecutor {
	return &failNTimesExecutor{n: n, seen: map[string]int{}, times: map[string][]time.Time{}}
}

func (e *failNTimesExecutor) Execute(_ context.Context, job *model.Job) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seen[job.ID]++
	e.times[job.ID] = append(e.times[job.ID], time.Now())
	if e.seen[job.ID] <= e.n {
		return errors.New("simulated failure")
	}
	return nil
}

func (e *failNTimesExecutor) startTimes(jobID string) []time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]time.Time(nil), e.times[jobID]...)
}

// startPipeline runs the full wheel scheduler → queue → one worker pipeline
// with a short retry base, so retry tests exercise the real machinery.
func startPipeline(t *testing.T, st *store.Store, q *queue.Queue, exec scheduler.Executor, retry scheduler.Backoff) {
	t.Helper()
	log := newTestLogger()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	s := &scheduler.WheelScheduler{
		Loader:    st,
		Dispatch:  &scheduler.QueueDispatcher{Store: st, Queue: q, Token: 1, Log: log},
		Tick:      50 * time.Millisecond,
		Lookahead: 10 * time.Second,
		Reload:    150 * time.Millisecond,
		Log:       log,
	}
	go func() { _ = s.Run(ctx) }()

	w := &worker.Worker{ID: "retry-w", Store: st, Queue: q, Exec: exec, Retry: retry, Log: log}
	go func() { _ = w.Run(ctx) }()
}

// TestFailingJobRetriesThenSucceeds (M6): attempts 1 and 2 fail, attempt 3
// succeeds. The gaps between attempts must show exponential backoff
// (>= base, then >= 2·base).
func TestFailingJobRetriesThenSucceeds(t *testing.T) {
	st, q := testDeps(t)
	ctx := context.Background()

	const base = 400 * time.Millisecond
	exec := newFailNTimes(2)
	startPipeline(t, st, q, exec, scheduler.Backoff{Base: base, Cap: 10 * time.Second})

	job := createDueJob(t, st, "retry-then-succeed")
	waitAllSucceeded(t, st, []*model.Job{job}, 20*time.Second)

	runs, err := st.RunsForJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("got %d runs, want 3 (fail, fail, success)", len(runs))
	}
	for i, run := range runs {
		if run.Attempt != i+1 {
			t.Errorf("run %d has attempt %d, want %d", i, run.Attempt, i+1)
		}
	}
	for i, wantOutcome := range []string{model.OutcomeError, model.OutcomeError, model.OutcomeSuccess} {
		if runs[i].Outcome == nil || *runs[i].Outcome != wantOutcome {
			t.Errorf("run %d outcome = %v, want %s", i, runs[i].Outcome, wantOutcome)
		}
	}

	// Exponential spacing: gap1 >= base, gap2 >= 2·base (jitter and queue
	// latency only ever push the gaps later, never earlier).
	times := exec.startTimes(job.ID)
	if len(times) != 3 {
		t.Fatalf("executor saw %d starts, want 3", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < base {
		t.Errorf("first retry after %v, want >= %v", gap, base)
	}
	if gap := times[2].Sub(times[1]); gap < 2*base {
		t.Errorf("second retry after %v, want >= %v", gap, 2*base)
	}

	got, err := st.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.Attempt != 3 {
		t.Errorf("job.attempt = %d, want 3", got.Attempt)
	}
}

// TestFailingJobGivesUpAtMaxRetries (M6): with max_retries=1 an always-
// failing job runs twice (initial + 1 retry) and lands in 'failed' for good.
func TestFailingJobGivesUpAtMaxRetries(t *testing.T) {
	st, q := testDeps(t)
	ctx := context.Background()

	exec := newFailNTimes(1 << 30) // always fails
	startPipeline(t, st, q, exec, scheduler.Backoff{Base: 200 * time.Millisecond, Cap: 5 * time.Second})

	runAt := time.Now().UTC().Add(-time.Second)
	job := &model.Job{
		ID: uuid.NewString(), Name: "gives-up", ScheduleType: model.ScheduleOnce,
		RunAt: &runAt, DependsOn: []string{}, MaxRetries: 1, Status: model.StatusScheduled,
	}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		got, err := st.GetJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if got.Status == model.StatusFailed {
			break
		}
		if got.Status == model.StatusSucceeded {
			t.Fatal("always-failing job succeeded")
		}
		if time.Now().After(deadline) {
			t.Fatalf("job still %q, want failed", got.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	runs, err := st.RunsForJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("got %d runs, want exactly 2 (initial + 1 retry)", len(runs))
	}
	for _, run := range runs {
		if run.Outcome == nil || *run.Outcome != model.OutcomeError {
			t.Fatalf("run outcome = %v, want error", run.Outcome)
		}
	}
}
