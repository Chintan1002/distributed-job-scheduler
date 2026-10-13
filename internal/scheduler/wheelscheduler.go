package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/queue"
)

// loadBatchSize caps how many jobs one reload pulls into the wheel.
const loadBatchSize = 1000

// LoadStore is what the wheel scheduler needs to refill its window.
type LoadStore interface {
	LoadDueJobs(ctx context.Context, until time.Time, limit int) ([]*model.Job, error)
}

// Dispatcher decides what "fire" means for a due job. M2 runs it inline in
// this process; M3 hands it to workers through the Redis ready queue.
type Dispatcher interface {
	Dispatch(ctx context.Context, job *model.Job)
}

// WheelScheduler is the M2+ scheduler: instead of asking Postgres "anything
// due?" on every tick (the M1 poller), it periodically loads the jobs due
// within the next Lookahead window into an in-memory hierarchical timing
// wheel and fires them from there. Postgres is touched once per Reload
// interval, not once per tick, and each tick is O(1).
type WheelScheduler struct {
	Loader    LoadStore
	Dispatch  Dispatcher
	Tick      time.Duration // wheel granularity, e.g. 100ms
	Lookahead time.Duration // how far ahead reloads look, e.g. 60s
	Reload    time.Duration // how often to refill the wheel from Postgres
	Log       *slog.Logger
}

// Run drives the wheel until ctx is cancelled. The tradeoff (ARCHITECTURE.md
// §4): the wheel's horizon is bounded, so far-future jobs are reloaded from
// Postgres periodically. A job created inside the current window fires at
// most one Reload interval late.
func (s *WheelScheduler) Run(ctx context.Context) error {
	now := time.Now().UTC()
	wheel := NewTimingWheel(s.Tick, defaultWheelSize, defaultWheelLevels, now)
	s.Log.Info("wheel scheduler started",
		"tick", s.Tick, "lookahead", s.Lookahead, "reload", s.Reload)

	if err := s.load(ctx, wheel); err != nil {
		s.Log.Error("initial load failed", "err", err)
	}

	ticker := time.NewTicker(s.Tick)
	defer ticker.Stop()
	reload := time.NewTicker(s.Reload)
	defer reload.Stop()

	for {
		select {
		case <-ctx.Done():
			s.Log.Info("wheel scheduler stopped")
			return ctx.Err()
		case <-reload.C:
			if err := s.load(ctx, wheel); err != nil && ctx.Err() == nil {
				s.Log.Error("reload failed", "err", err)
			}
		case tickTime := <-ticker.C:
			for _, job := range wheel.Advance(tickTime.UTC()) {
				s.Dispatch.Dispatch(ctx, job)
			}
		}
	}
}

// load pulls every job due before now+Lookahead into the wheel. The wheel
// dedups by job id, so reloading an overlapping window is harmless. Jobs
// already past due (e.g. created due-immediately) fire right away.
func (s *WheelScheduler) load(ctx context.Context, wheel *TimingWheel) error {
	jobs, err := s.Loader.LoadDueJobs(ctx, time.Now().UTC().Add(s.Lookahead), loadBatchSize)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.RunAt == nil {
			continue
		}
		if due := wheel.Add(job, *job.RunAt); due {
			s.Dispatch.Dispatch(ctx, job)
		}
	}
	return nil
}

// InlineStore is what the inline dispatcher needs: claim + record.
type InlineStore interface {
	RunStore
	ClaimJob(ctx context.Context, jobID string) (bool, error)
}

// InlineDispatcher (M2): claim and execute the job in this process. Kept for
// single-process runs and as the pre-distribution baseline.
type InlineDispatcher struct {
	Store InlineStore
	Exec  Executor
	Retry Backoff // zero value → defaults
	Log   *slog.Logger
}

// Dispatch claims and runs one due job. The conditional claim (scheduled →
// running) is the at-most-once guard: a job cancelled after it was loaded
// into the wheel, or already claimed elsewhere, fails the claim and is
// skipped.
func (d *InlineDispatcher) Dispatch(ctx context.Context, job *model.Job) {
	claimed, err := d.Store.ClaimJob(ctx, job.ID)
	if err != nil {
		if ctx.Err() == nil {
			d.Log.Error("claim failed", "job_id", job.ID, "err", err)
		}
		return
	}
	if !claimed {
		d.Log.Info("skipping unclaimable job (cancelled or already claimed)", "job_id", job.ID)
		return
	}
	d.Log.Info("job state", "job_id", job.ID, "from", model.StatusScheduled, "to", model.StatusRunning)
	ExecuteAndRecord(ctx, d.Store, d.Exec, d.Retry, d.Log, job, 0)
}

// QueueStore is what the queue dispatcher needs.
type QueueStore interface {
	MarkQueued(ctx context.Context, jobID string, fencingToken int64) (bool, error)
	SetJobStatus(ctx context.Context, jobID, status string) error
}

// QueueDispatcher (M3): hand the due job to the workers via the Redis ready
// queue. The durable state transition (scheduled → queued in Postgres)
// happens BEFORE the push: if we crash in between, the job is stuck 'queued'
// rather than double-fired — never lost from Postgres, and recoverable.
type QueueDispatcher struct {
	Store QueueStore
	Queue *queue.Queue
	Token int64 // fencing token of the leadership this dispatcher belongs to (M5)
	Log   *slog.Logger
}

func (d *QueueDispatcher) Dispatch(ctx context.Context, job *model.Job) {
	queued, err := d.Store.MarkQueued(ctx, job.ID, d.Token)
	if err != nil {
		if ctx.Err() == nil {
			d.Log.Error("mark queued failed", "job_id", job.ID, "err", err)
		}
		return
	}
	if !queued {
		d.Log.Info("skipping unqueueable job (cancelled, already handed off, or fenced)", "job_id", job.ID)
		return
	}
	if err := d.Queue.Push(ctx, queue.Message{JobID: job.ID, FencingToken: d.Token}); err != nil {
		// Push failed after the DB flip (often because our leadership just
		// ended and ctx died): put the job back so the next leader's reload
		// re-fires it. The revert must survive our own cancellation, hence
		// WithoutCancel. Worst case the revert also fails and the job sits
		// 'queued' until reconciliation (later milestone) — but it is never
		// lost (it's in Postgres) and never double-fired.
		d.Log.Error("queue push failed, reverting to scheduled", "job_id", job.ID, "err", err)
		rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer rcancel()
		if revertErr := d.Store.SetJobStatus(rctx, job.ID, model.StatusScheduled); revertErr != nil {
			d.Log.Error("revert to scheduled failed", "job_id", job.ID, "err", revertErr)
		}
		return
	}
	d.Log.Info("job state", "job_id", job.ID, "from", model.StatusScheduled, "to", model.StatusQueued, "fencing_token", d.Token)
}
