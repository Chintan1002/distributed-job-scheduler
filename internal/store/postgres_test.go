package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
)

const defaultTestDatabaseURL = "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"

func testStore(t *testing.T) *store.Store {
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

func createScheduledJob(t *testing.T, st *store.Store) *model.Job {
	t.Helper()
	runAt := time.Now().UTC().Add(-time.Second)
	job := &model.Job{
		ID: uuid.NewString(), Name: "fencing-test",
		ScheduleType: model.ScheduleOnce, RunAt: &runAt,
		DependsOn: []string{}, MaxRetries: 3, Status: model.StatusScheduled,
	}
	if err := st.CreateJob(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job
}

// TestFencingRejectsStaleClaim: the worker-side fence. A message minted under
// an old leadership cannot claim a job whose high-water mark has moved on.
func TestFencingRejectsStaleClaim(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	job := createScheduledJob(t, st)

	if ok, err := st.MarkQueued(ctx, job.ID, 5); err != nil || !ok {
		t.Fatalf("MarkQueued(token=5): ok=%v err=%v", ok, err)
	}
	// Stale token 4 < high-water 5: rejected even though status is 'queued'.
	if ok, err := st.ClaimQueuedJob(ctx, job.ID, 4); err != nil || ok {
		t.Fatalf("ClaimQueuedJob(token=4) = %v, %v; want rejected", ok, err)
	}
	// Equal token (same leadership that enqueued it): accepted.
	if ok, err := st.ClaimQueuedJob(ctx, job.ID, 5); err != nil || !ok {
		t.Fatalf("ClaimQueuedJob(token=5): ok=%v err=%v", ok, err)
	}
}

// TestFencingRejectsZombieLeaderWrite is the zombie-leader scenario at the database level:
// the "two leaders" scenario. Statuses can cycle back to 'scheduled' (e.g. a
// requeue after a failed push), so a status condition alone would let the
// zombie's write through — the monotonic token is what actually fences it.
func TestFencingRejectsZombieLeaderWrite(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	job := createScheduledJob(t, st)

	// Leadership #2 queued the job, then it went back to 'scheduled'
	// (requeue path) — status now looks claimable again.
	if ok, err := st.MarkQueued(ctx, job.ID, 2); err != nil || !ok {
		t.Fatalf("MarkQueued(token=2): ok=%v err=%v", ok, err)
	}
	if err := st.SetJobStatus(ctx, job.ID, model.StatusScheduled); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	// The zombie (old leadership #1) wakes up and tries to enqueue: status
	// matches, but its token is below the high-water mark → rejected.
	if ok, err := st.MarkQueued(ctx, job.ID, 1); err != nil || ok {
		t.Fatalf("zombie MarkQueued(token=1) = %v, %v; want rejected", ok, err)
	}
	// The current leadership (#3) succeeds.
	if ok, err := st.MarkQueued(ctx, job.ID, 3); err != nil || !ok {
		t.Fatalf("MarkQueued(token=3): ok=%v err=%v", ok, err)
	}
}
