// The scheduler: fires due jobs from an in-memory hierarchical timing wheel
// (M2), reloading the lookahead window from Postgres periodically. Due jobs
// are handed to workers through the Redis ready queue (M3). Run several of
// these: only the elected leader's wheel is active (M4) — the invariant that
// jobs fire only under the leader lease is enforced by running the whole
// wheel inside the elector's lead callback.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Chintan1002/distributed-job-scheduler/internal/config"
	"github.com/Chintan1002/distributed-job-scheduler/internal/election"
	"github.com/Chintan1002/distributed-job-scheduler/internal/queue"
	"github.com/Chintan1002/distributed-job-scheduler/internal/scheduler"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
)

func main() {
	cfg := config.Load()
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("service", "scheduler", "node_id", cfg.NodeID)

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

	elector, err := election.New(ctx, cfg.RedisURL, cfg.LeaderKey, cfg.NodeID, cfg.LeaderTTL, log)
	if err != nil {
		log.Error("connect elector to redis", "err", err)
		os.Exit(1)
	}
	defer elector.Close()

	// The wheel only ever runs inside the lead callback: no lease, no firing.
	// Every dispatch this leadership makes carries its fencing token.
	err = elector.Run(ctx, func(leaderCtx context.Context, token int64) {
		s := &scheduler.WheelScheduler{
			Loader:    st,
			Dispatch:  &scheduler.QueueDispatcher{Store: st, Queue: q, Token: token, Log: log},
			Tick:      cfg.WheelTick,
			Lookahead: cfg.WheelLookahead,
			Reload:    cfg.WheelReload,
			Log:       log,
		}
		_ = s.Run(leaderCtx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("scheduler exited", "err", err)
		os.Exit(1)
	}
	log.Info("scheduler shut down cleanly")
}
