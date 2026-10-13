// Package api is the HTTP layer: request validation and JSON in/out.
// It never executes jobs — it only reads and writes the store.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
	"github.com/Chintan1002/distributed-job-scheduler/internal/store"
)

// Store is the slice of the persistence layer the API needs.
type Store interface {
	CreateJob(ctx context.Context, j *model.Job) error
	GetJob(ctx context.Context, id string) (*model.Job, error)
	CancelJob(ctx context.Context, id string) error
}

type Server struct {
	store Store
	log   *slog.Logger
}

func NewServer(st Store, log *slog.Logger) *Server {
	return &Server{store: st, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.handleCreateJob)
	mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	mux.HandleFunc("DELETE /jobs/{id}", s.handleCancelJob)
	return mux
}

type createJobRequest struct {
	Name         string          `json:"name"`
	Payload      json.RawMessage `json:"payload"`
	ScheduleType string          `json:"schedule_type"`
	CronExpr     string          `json:"cron_expr"`
	RunAt        *time.Time      `json:"run_at"`
	DependsOn    []string        `json:"depends_on"`
	MaxRetries   *int            `json:"max_retries"`
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	switch req.ScheduleType {
	case model.ScheduleOnce:
		if req.RunAt == nil {
			writeError(w, http.StatusBadRequest, "run_at is required for schedule_type 'once' (RFC 3339, e.g. 2026-07-14T22:00:00Z)")
			return
		}
	case model.ScheduleCron:
		// M1 is one-shot only; cron schedules arrive with the timing wheel work.
		writeError(w, http.StatusBadRequest, "schedule_type 'cron' is not supported yet (M1 supports 'once' only)")
		return
	default:
		writeError(w, http.StatusBadRequest, "schedule_type must be 'once' or 'cron'")
		return
	}
	if len(req.DependsOn) > 0 {
		// DAG dependencies arrive in M7.
		writeError(w, http.StatusBadRequest, "depends_on is not supported yet (job dependencies arrive in a later milestone)")
		return
	}
	maxRetries := 3
	if req.MaxRetries != nil {
		if *req.MaxRetries < 0 {
			writeError(w, http.StatusBadRequest, "max_retries must be >= 0")
			return
		}
		maxRetries = *req.MaxRetries
	}

	runAt := req.RunAt.UTC()
	job := &model.Job{
		ID:           uuid.NewString(),
		Name:         req.Name,
		Payload:      req.Payload,
		ScheduleType: req.ScheduleType,
		RunAt:        &runAt,
		DependsOn:    []string{},
		MaxRetries:   maxRetries,
		Status:       model.StatusScheduled,
	}
	if err := s.store.CreateJob(r.Context(), job); err != nil {
		s.log.Error("create job failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to persist job")
		return
	}
	s.log.Info("job state", "job_id", job.ID, "name", job.Name, "to", model.StatusScheduled, "run_at", runAt)
	writeJSON(w, http.StatusCreated, map[string]string{"id": job.ID})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "id must be a UUID")
		return
	}
	job, err := s.store.GetJob(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		s.log.Error("get job failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to load job")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeError(w, http.StatusBadRequest, "id must be a UUID")
		return
	}
	err := s.store.CancelJob(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, store.ErrNotCancellable):
		writeError(w, http.StatusConflict, "job is not in a cancellable state")
	case err != nil:
		s.log.Error("cancel job failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
	default:
		s.log.Info("job state", "job_id", id, "to", model.StatusCancelled)
		w.WriteHeader(http.StatusNoContent)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
