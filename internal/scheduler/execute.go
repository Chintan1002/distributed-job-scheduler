package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
)

// Executor runs a job's payload.
type Executor interface {
	Execute(ctx context.Context, job *model.Job) error
}

// PayloadLogger is the stand-in executor until jobs do real work: it logs
// the payload; {"sleep_ms": N} simulates N ms of work, and {"fail": true}
// simulates a failing job (exercises the retry path end to end).
type PayloadLogger struct {
	Log *slog.Logger
}

func (e *PayloadLogger) Execute(ctx context.Context, job *model.Job) error {
	e.Log.Info("executing job", "job_id", job.ID, "name", job.Name, "payload", string(job.Payload), "attempt", job.Attempt)
	var p struct {
		SleepMs int  `json:"sleep_ms"`
		Fail    bool `json:"fail"`
	}
	if len(job.Payload) > 0 {
		_ = json.Unmarshal(job.Payload, &p) // payloads without these keys are fine
	}
	if p.SleepMs > 0 {
		select {
		case <-time.After(time.Duration(p.SleepMs) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.Fail {
		return errors.New("payload requested failure")
	}
	return nil
}

// Backoff is the retry policy. After the n-th failed
// attempt the next try waits Base·2^(n−1) plus a little jitter (up to
// Base/4) so a burst of failures doesn't retry in lockstep.
type Backoff struct {
	Base time.Duration // first retry delay; 0 → 1s
	Cap  time.Duration // upper bound on the delay; 0 → 1min
}

func (b Backoff) base() time.Duration {
	if b.Base <= 0 {
		return time.Second
	}
	return b.Base
}

func (b Backoff) cap() time.Duration {
	if b.Cap <= 0 {
		return time.Minute
	}
	return b.Cap
}

// Delay returns the wait before the next attempt, given that attempt
// `attempt` (1-based) just failed: Base, 2·Base, 4·Base, … capped at Cap,
// plus jitter in [0, Base/4).
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.base()
	// Shift with overflow guard: beyond the cap the exact power is moot.
	for i := 1; i < attempt && d < b.cap(); i++ {
		d *= 2
	}
	if d > b.cap() {
		d = b.cap()
	}
	if j := int64(b.base() / 4); j > 0 {
		d += time.Duration(rand.Int63n(j))
	}
	return d
}

// RunStore is the slice of the persistence layer needed to record an
// execution attempt and schedule retries. Shared by the M1 poller, the M2
// inline dispatcher, and the M3 workers.
type RunStore interface {
	StartRun(ctx context.Context, jobID string, attempt int, fencingToken int64) (string, error)
	FinishRun(ctx context.Context, runID, outcome string, errMsg *string) error
	SetJobStatus(ctx context.Context, jobID, status string) error
	RescheduleForRetry(ctx context.Context, jobID string, runAt time.Time, fencingToken int64) (bool, error)
}

// ExecuteAndRecord runs an already-claimed job (status=running, attempt
// already bumped by the claim) and records the attempt in job_runs. On
// failure it either reschedules the job with exponential backoff (M6) or,
// once MaxRetries retries are spent, marks it failed for good. The caller
// is responsible for the claim (and for logging its own claim transition).
func ExecuteAndRecord(ctx context.Context, st RunStore, exec Executor, retry Backoff, log *slog.Logger, job *model.Job, fencingToken int64) {
	attempt := job.Attempt
	if attempt < 1 {
		attempt = 1 // claims bump this; guard for rows claimed by older code
	}
	runID, err := st.StartRun(ctx, job.ID, attempt, fencingToken)
	if err != nil {
		log.Error("start run failed", "job_id", job.ID, "err", err)
		return
	}

	if execErr := exec.Execute(ctx, job); execErr != nil {
		msg := execErr.Error()
		if err := st.FinishRun(ctx, runID, model.OutcomeError, &msg); err != nil {
			log.Error("finish run failed", "job_id", job.ID, "err", err)
		}
		// Attempt n failed; retries remain while n <= MaxRetries (so a job
		// gets 1 + MaxRetries attempts in total).
		if attempt <= job.MaxRetries {
			delay := retry.Delay(attempt)
			runAt := time.Now().UTC().Add(delay)
			ok, err := st.RescheduleForRetry(ctx, job.ID, runAt, fencingToken)
			switch {
			case err != nil:
				log.Error("reschedule for retry failed", "job_id", job.ID, "err", err)
			case !ok:
				log.Warn("retry fenced off (job no longer running or newer leadership)", "job_id", job.ID)
			default:
				log.Info("job state", "job_id", job.ID, "from", model.StatusRunning, "to", model.StatusScheduled,
					"retry", attempt, "of", job.MaxRetries, "delay", delay, "next_run_at", runAt, "err", msg)
			}
			return
		}
		if err := st.SetJobStatus(ctx, job.ID, model.StatusFailed); err != nil {
			log.Error("set job status failed", "job_id", job.ID, "err", err)
		}
		log.Info("job state", "job_id", job.ID, "from", model.StatusRunning, "to", model.StatusFailed,
			"attempts", attempt, "err", msg)
		return
	}

	if err := st.FinishRun(ctx, runID, model.OutcomeSuccess, nil); err != nil {
		log.Error("finish run failed", "job_id", job.ID, "err", err)
	}
	if err := st.SetJobStatus(ctx, job.ID, model.StatusSucceeded); err != nil {
		log.Error("set job status failed", "job_id", job.ID, "err", err)
	}
	log.Info("job state", "job_id", job.ID, "from", model.StatusRunning, "to", model.StatusSucceeded, "attempt", attempt)
}
