package scheduler_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
)

// Integration tests against real Postgres (docker-compose). They skip with a
// clear message when the database is unreachable.

const defaultTestDatabaseURL = "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"

func testStore(t testing.TB) *store.Store {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = defaultTestDatabaseURL
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	st, err := store.New(ctx, url)
	if err != nil {
		t.Skipf("postgres unavailable, skipping integration test (run `make migrate` first): %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func startPoller(t *testing.T, st *store.Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	p := &scheduler.Poller{
		Store:    st,
		Exec:     &scheduler.PayloadLogger{Log: log},
		Interval: 100 * time.Millisecond,
		Log:      log,
	}
	go func() { _ = p.Run(ctx) }()
}

func newOneShotJob(runAt time.Time) *model.Job {
	return &model.Job{
		ID:           uuid.NewString(),
		Name:         "m1-integration-test",
		Payload:      json.RawMessage(`{"task":"noop"}`),
		ScheduleType: model.ScheduleOnce,
		RunAt:        &runAt,
		DependsOn:    []string{},
		MaxRetries:   3,
		Status:       model.StatusScheduled,
	}
}

// TestScheduledJobRuns is the M1 exit criterion: a job scheduled for the near
// future actually runs (scheduled → succeeded, with a job_runs row).
// It uses +2s instead of the spec's illustrative +10s to keep the suite fast —
// the proof (a future fire time honored by the poll loop) is identical.
func TestScheduledJobRuns(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	runAt := time.Now().UTC().Add(2 * time.Second)
	job := newOneShotJob(runAt)
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	startPoller(t, st)

	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := st.GetJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if got.Status == model.StatusSucceeded {
			break
		}
		if got.Status == model.StatusFailed {
			t.Fatalf("job failed, want succeeded")
		}
		if time.Now().After(deadline) {
			t.Fatalf("job never ran: status still %q after deadline", got.Status)
		}
		time.Sleep(100 * time.Millisecond)
	}

	runs, err := st.RunsForJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("got %d runs, want exactly 1", len(runs))
	}
	run := runs[0]
	if run.Outcome == nil || *run.Outcome != model.OutcomeSuccess {
		t.Fatalf("run outcome = %v, want success", run.Outcome)
	}
	if run.StartedAt == nil || run.FinishedAt == nil {
		t.Fatalf("run missing timestamps: started=%v finished=%v", run.StartedAt, run.FinishedAt)
	}
	// The job must not fire before its due time. 1s tolerance covers clock
	// drift between the host (runAt) and the Postgres container (started_at).
	if run.StartedAt.Before(runAt.Add(-1 * time.Second)) {
		t.Fatalf("job ran early: started %v, due %v", run.StartedAt, runAt)
	}
}

// TestCancelledJobDoesNotRun: cancelling before the due time means the job
// never fires and no run is recorded.
func TestCancelledJobDoesNotRun(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	runAt := time.Now().UTC().Add(1 * time.Second)
	job := newOneShotJob(runAt)
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := st.CancelJob(ctx, job.ID); err != nil {
		t.Fatalf("cancel job: %v", err)
	}

	startPoller(t, st)

	// Wait until well past the due time, then confirm nothing happened.
	time.Sleep(2500 * time.Millisecond)

	got, err := st.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.Status != model.StatusCancelled {
		t.Fatalf("status = %q, want cancelled", got.Status)
	}
	runs, err := st.RunsForJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("cancelled job has %d runs, want 0", len(runs))
	}
}

// TestClaimDueJobsClaimsOnce: the conditional UPDATE means a job can only be
// claimed once even when claimed repeatedly — the M1 seed of the
// at-most-once invariant.
func TestClaimDueJobsClaimsOnce(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	runAt := time.Now().UTC().Add(-1 * time.Second) // already due
	job := newOneShotJob(runAt)
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed := 0
	for i := 0; i < 3; i++ {
		jobs, err := st.ClaimDueJobs(ctx, 1000)
		if err != nil {
			t.Fatalf("claim due jobs: %v", err)
		}
		for _, j := range jobs {
			if j.ID == job.ID {
				claimed++
			}
		}
	}
	if claimed != 1 {
		t.Fatalf("job claimed %d times, want exactly 1", claimed)
	}
}
