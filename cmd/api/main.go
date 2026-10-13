// The API server: stateless REST front door. Validates requests and persists
// jobs to Postgres. Never executes jobs.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/api"
	"github.com/Chintan1002/distributed-job-scheduler/internal/config"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("service", "api")
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect to postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	srv := &http.Server{
		Addr:    cfg.APIAddr,
		Handler: api.NewServer(st, log).Handler(),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("api listening", "addr", cfg.APIAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
	log.Info("api shut down cleanly")
}
