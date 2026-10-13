// Package scheduler fires due jobs.
//
// The M1 design in this file is a simple Postgres polling loop that claims
// due jobs and runs them inline. It is kept as the baseline the timing wheel
// (timingwheel.go, M2) is benchmarked against.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
)

// claimBatchSize caps how many due jobs one tick claims.
const claimBatchSize = 100

// Store is the slice of the persistence layer the poller needs.
type Store interface {
	RunStore
	ClaimDueJobs(ctx context.Context, limit int) ([]*model.Job, error)
}

type Poller struct {
	Store    Store
	Exec     Executor
	Retry    Backoff // zero value → defaults (1s base, 1min cap)
	Interval time.Duration
	Log      *slog.Logger
}

// Run polls until ctx is cancelled. A failed tick is logged and retried on
// the next tick — a transient Postgres blip must not kill the scheduler.
func (p *Poller) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	p.Log.Info("scheduler poll loop started", "interval", p.Interval)
	for {
		select {
		case <-ctx.Done():
			p.Log.Info("scheduler poll loop stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := p.tick(ctx); err != nil && ctx.Err() == nil {
				p.Log.Error("tick failed", "err", err)
			}
		}
	}
}

func (p *Poller) tick(ctx context.Context) error {
	jobs, err := p.Store.ClaimDueJobs(ctx, claimBatchSize)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		p.Log.Info("job state", "job_id", job.ID, "from", model.StatusScheduled, "to", model.StatusRunning)
		ExecuteAndRecord(ctx, p.Store, p.Exec, p.Retry, p.Log, job, 0)
	}
	return nil
}
