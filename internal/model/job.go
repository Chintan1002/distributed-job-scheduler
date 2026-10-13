package model

import (
	"encoding/json"
	"time"
)

// Job statuses. Lifecycle: scheduled → queued → running → succeeded|failed,
// or scheduled → cancelled. (M1 runs jobs inline, so 'queued' is unused until M3.)
const (
	StatusScheduled = "scheduled"
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// Schedule types.
const (
	ScheduleOnce = "once"
	ScheduleCron = "cron"
)

// Job is a row in the jobs table — the durable source of truth for a schedule.
type Job struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	ScheduleType string          `json:"schedule_type"`
	CronExpr     *string         `json:"cron_expr,omitempty"`
	RunAt        *time.Time      `json:"run_at,omitempty"`
	DependsOn    []string        `json:"depends_on"`
	MaxRetries   int             `json:"max_retries"`
	Attempt      int             `json:"attempt"` // execution attempts started so far
	Status       string          `json:"status"`
	CreatedAt    time.Time       `json:"created_at"`
}

// JobRun is one execution attempt of a job.
type JobRun struct {
	ID           string     `json:"id"`
	JobID        string     `json:"job_id"`
	Attempt      int        `json:"attempt"`
	FencingToken int64      `json:"fencing_token"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	Outcome      *string    `json:"outcome,omitempty"`
	ErrorMsg     *string    `json:"error_msg,omitempty"`
}

// Run outcomes.
const (
	OutcomeSuccess = "success"
	OutcomeError   = "error"
)
