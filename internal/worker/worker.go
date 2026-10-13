// Package worker pulls ready jobs from the Redis queue and executes them.
// Workers are stateless and scale horizontally: N workers share one queue,
// and the conditional claim in Postgres guarantees each queue message runs a
// job at most once even if a message is somehow duplicated.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/queue"
	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
)

// popTimeout bounds each blocking pop so the loop re-checks ctx regularly.
const popTimeout = 2 * time.Second

// Store is the slice of the persistence layer a worker needs.
type Store interface {
	scheduler.RunStore
	ClaimQueuedJob(ctx context.Context, jobID string, fencingToken int64) (bool, error)
	GetJob(ctx context.Context, jobID string) (*model.Job, error)
}

type Worker struct {
	ID    string
	Store Store
	Queue *queue.Queue
	Exec  scheduler.Executor
	Retry scheduler.Backoff // zero value → defaults (1s base, 1min cap)
	Log   *slog.Logger
}

// Run pulls and executes jobs until ctx is cancelled. One job at a time per
// worker — throughput scales by adding worker processes, not by hidden
// concurrency inside one.
func (w *Worker) Run(ctx context.Context) error {
	log := w.Log.With("worker_id", w.ID)
	log.Info("worker started")
	for {
		if ctx.Err() != nil {
			log.Info("worker stopped")
			return ctx.Err()
		}
		msg, ok, err := w.Queue.Pop(ctx, popTimeout)
		if err != nil {
			if ctx.Err() != nil {
				log.Info("worker stopped")
				return ctx.Err()
			}
			log.Error("queue pop failed", "err", err)
			time.Sleep(time.Second) // don't hot-loop against a sick Redis
			continue
		}
		if !ok {
			continue // timed out empty; loop re-checks ctx
		}
		w.process(ctx, log, msg)
	}
}

// process claims and runs one queue message. The conditional claim
// (queued → running) is the worker-side at-most-once guard, and the claim
// carries the message's fencing token (M5): a message minted by a stale
// leadership is rejected by Postgres here, no matter when it surfaces.
func (w *Worker) process(ctx context.Context, log *slog.Logger, msg queue.Message) {
	claimed, err := w.Store.ClaimQueuedJob(ctx, msg.JobID, msg.FencingToken)
	if err != nil {
		log.Error("claim failed", "job_id", msg.JobID, "err", err)
		return
	}
	if !claimed {
		log.Info("skipping unclaimable message (duplicate, stale token, or job no longer queued)",
			"job_id", msg.JobID, "fencing_token", msg.FencingToken)
		return
	}
	log.Info("job state", "job_id", msg.JobID, "from", model.StatusQueued, "to", model.StatusRunning)

	job, err := w.Store.GetJob(ctx, msg.JobID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.Error("claimed job vanished", "job_id", msg.JobID)
			return
		}
		log.Error("load claimed job failed", "job_id", msg.JobID, "err", err)
		return
	}
	scheduler.ExecuteAndRecord(ctx, w.Store, w.Exec, w.Retry, log, job, msg.FencingToken)
}
