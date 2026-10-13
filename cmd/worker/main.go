// The worker: pulls ready jobs from the Redis queue, claims them in
// Postgres, executes, and records the outcome. Stateless — run as many as
// you want.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Chintan1002/distributed-job-scheduler/internal/config"
	"github.com/Chintan1002/distributed-job-scheduler/internal/queue"
	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
	"github.com/Chintan1002/distributed-job-scheduler/internal/worker"
)

func main() {
	cfg := config.Load()
	workerID := cfg.WorkerID
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("service", "worker", "worker_id", workerID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect to postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	q, err := queue.New(ctx, cfg.RedisURL, cfg.QueueKey)
	if err != nil {
		log.Error("connect to redis", "err", err)
		os.Exit(1)
	}
	defer q.Close()

	w := &worker.Worker{
		ID:    workerID,
		Store: st,
		Queue: q,
		Exec:  &scheduler.PayloadLogger{Log: log},
		Retry: scheduler.Backoff{Base: cfg.RetryBase, Cap: cfg.RetryCap},
		Log:   log,
	}
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker exited", "err", err)
		os.Exit(1)
	}
	log.Info("worker shut down cleanly")
}
