package scheduler_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
)

// Integration tests proving M2 preserves M1 behavior, with the timing wheel
// in place of the polling loop. Same skip rule: need Postgres up.

func startWheelScheduler(t *testing.T, st *store.Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &scheduler.WheelScheduler{
		Loader:    st,
		Dispatch:  &scheduler.InlineDispatcher{Store: st, Exec: &scheduler.PayloadLogger{Log: log}, Log: log},
		Tick:      50 * time.Millisecond,
		Lookahead: 10 * time.Second,
		Reload:    500 * time.Millisecond,
		Log:       log,
	}
	go func() { _ = s.Run(ctx) }()
}

func waitForStatus(t *testing.T, st *store.Store, jobID, want string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for {
		got, err := st.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if got.Status == want {
			return
		}
		if got.Status == model.StatusFailed && want != model.StatusFailed {
			t.Fatalf("job failed, want %s", want)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job status still %q after %v, want %q", got.Status, timeout, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestWheelSchedulerRunsDueJob is the M1 exit criterion re-proven on the
// wheel: a job scheduled for the near future runs on time, exactly once.
func TestWheelSchedulerRunsDueJob(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	runAt := time.Now().UTC().Add(2 * time.Second)
	job := newOneShotJob(runAt)
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	startWheelScheduler(t, st)
	waitForStatus(t, st, job.ID, model.StatusSucceeded, 10*time.Second)

	runs, err := st.RunsForJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("got %d runs, want exactly 1", len(runs))
	}
	if runs[0].Outcome == nil || *runs[0].Outcome != model.OutcomeSuccess {
		t.Fatalf("run outcome = %v, want success", runs[0].Outcome)
	}
	if runs[0].StartedAt.Before(runAt.Add(-1 * time.Second)) {
		t.Fatalf("job ran early: started %v, due %v", runs[0].StartedAt, runAt)
	}
}

// TestWheelSchedulerPicksUpNewJob: a job created AFTER the scheduler started
// is picked up by a periodic reload — the bounded-horizon tradeoff from
// ARCHITECTURE.md §4 in action.
func TestWheelSchedulerPicksUpNewJob(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	startWheelScheduler(t, st)
	time.Sleep(200 * time.Millisecond) // scheduler is up before the job exists

	runAt := time.Now().UTC().Add(1500 * time.Millisecond)
	job := newOneShotJob(runAt)
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	waitForStatus(t, st, job.ID, model.StatusSucceeded, 10*time.Second)
}

// TestWheelSchedulerCancelledJobDoesNotRun: the job is already loaded in the
// wheel when it gets cancelled; the conditional claim at fire time must
// reject it. No run row may exist.
func TestWheelSchedulerCancelledJobDoesNotRun(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	runAt := time.Now().UTC().Add(1500 * time.Millisecond)
	job := newOneShotJob(runAt)
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	startWheelScheduler(t, st)
	time.Sleep(700 * time.Millisecond) // past a reload: job is in the wheel now
	if err := st.CancelJob(ctx, job.ID); err != nil {
		t.Fatalf("cancel job: %v", err)
	}

	time.Sleep(1500 * time.Millisecond) // well past the due time

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
