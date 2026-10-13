// Package store is the Postgres persistence layer for jobs and job_runs.
// Postgres is the durable source of truth: a schedule is never acknowledged
// to the client before it is committed here.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Chintan1002/distributed-job-scheduler/internal/model"
)

var (
	// ErrNotFound means no job exists with the given id.
	ErrNotFound = errors.New("job not found")
	// ErrNotCancellable means the job exists but is past the point of cancelling.
	ErrNotCancellable = errors.New("job is not in a cancellable state")
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

const jobColumns = `id::text, name, payload, schedule_type, cron_expr, run_at, depends_on::text[], max_retries, attempt, status, created_at`

func (s *Store) CreateJob(ctx context.Context, j *model.Job) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobs (id, name, payload, schedule_type, cron_expr, run_at, depends_on, max_retries, status)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, COALESCE($7::uuid[], '{}'::uuid[]), $8, $9)`,
		j.ID, j.Name, j.Payload, j.ScheduleType, j.CronExpr, j.RunAt, j.DependsOn, j.MaxRetries, j.Status)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

func (s *Store) GetJob(ctx context.Context, id string) (*model.Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1::uuid`, id)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// CancelJob soft-deletes: run history must survive, so the row stays and only
// the status flips. Only still-scheduled jobs can be cancelled.
func (s *Store) CancelJob(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = $2 WHERE id = $1::uuid AND status = $3`,
		id, model.StatusCancelled, model.StatusScheduled)
	if err != nil {
		return fmt.Errorf("cancel job: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// Nothing updated: either the job doesn't exist or it's past cancelling.
	if _, err := s.GetJob(ctx, id); err != nil {
		return err
	}
	return ErrNotCancellable
}

// ClaimDueJobs atomically flips due jobs from 'scheduled' to 'running' and
// returns them. The single conditional UPDATE means a job can only be claimed
// once — the same shape the distributed claim builds on in M3+.
// FOR UPDATE SKIP LOCKED keeps concurrent claimers from blocking each other.
func (s *Store) ClaimDueJobs(ctx context.Context, limit int) ([]*model.Job, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE jobs SET status = $2, attempt = attempt + 1
		WHERE id IN (
			SELECT id FROM jobs
			WHERE status = $3 AND run_at <= now()
			ORDER BY run_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+jobColumns,
		limit, model.StatusRunning, model.StatusScheduled)
	if err != nil {
		return nil, fmt.Errorf("claim due jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan claimed job: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim due jobs rows: %w", err)
	}
	return jobs, nil
}

// LoadDueJobs returns scheduled jobs due before `until`, without changing
// their status — they stay claimable. The wheel scheduler (M2+) uses this to
// fill its lookahead window; the partial index idx_jobs_due covers it.
func (s *Store) LoadDueJobs(ctx context.Context, until time.Time, limit int) ([]*model.Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobColumns+` FROM jobs
		WHERE status = $2 AND run_at <= $1
		ORDER BY run_at
		LIMIT $3`,
		until, model.StatusScheduled, limit)
	if err != nil {
		return nil, fmt.Errorf("load due jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan due job: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load due jobs rows: %w", err)
	}
	return jobs, nil
}

// ClaimJob conditionally flips one job from 'scheduled' to 'running'.
// Returns false when the job was already claimed, cancelled, or finished —
// the at-most-once guard for a single firing (fencing tokens extend this
// across leaders in M5).
func (s *Store) ClaimJob(ctx context.Context, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = $2, attempt = attempt + 1 WHERE id = $1::uuid AND status = $3`,
		id, model.StatusRunning, model.StatusScheduled)
	if err != nil {
		return false, fmt.Errorf("claim job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkQueued conditionally flips one job from 'scheduled' to 'queued' —
// the scheduler's at-most-once enqueue guard (M3+). The token condition is
// the fence (M5): a zombie leader carries a token below the
// job's high-water mark and is rejected regardless of status. `<=` because
// one leadership legitimately writes its own token more than once.
func (s *Store) MarkQueued(ctx context.Context, id string, token int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = $2, last_fencing_token = $4
		WHERE id = $1::uuid AND status = $3 AND last_fencing_token <= $4`,
		id, model.StatusQueued, model.StatusScheduled, token)
	if err != nil {
		return false, fmt.Errorf("mark job queued: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ClaimQueuedJob conditionally flips one job from 'queued' to 'running' —
// the worker's at-most-once claim (M3+). The token condition (M5) drops
// queue messages minted by a stale leadership: only messages carrying a
// token at or above the job's high-water mark may claim it.
func (s *Store) ClaimQueuedJob(ctx context.Context, id string, token int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = $2, last_fencing_token = $4, attempt = attempt + 1
		WHERE id = $1::uuid AND status = $3 AND last_fencing_token <= $4`,
		id, model.StatusRunning, model.StatusQueued, token)
	if err != nil {
		return false, fmt.Errorf("claim queued job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RescheduleForRetry puts a failed running job back on the schedule for its
// next attempt (M6): status returns to 'scheduled' with the backed-off
// run_at, and the normal wheel → queue → claim machinery re-runs it. Fenced
// like every write that moves a job's lifecycle.
func (s *Store) RescheduleForRetry(ctx context.Context, id string, runAt time.Time, token int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = $2, run_at = $3
		WHERE id = $1::uuid AND status = $4 AND last_fencing_token <= $5`,
		id, model.StatusScheduled, runAt, model.StatusRunning, token)
	if err != nil {
		return false, fmt.Errorf("reschedule for retry: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) SetJobStatus(ctx context.Context, id, status string) error {
	_, err := s.pool.Exec(ctx, `UPDATE jobs SET status = $2 WHERE id = $1::uuid`, id, status)
	if err != nil {
		return fmt.Errorf("set job status: %w", err)
	}
	return nil
}

// StartRun records the beginning of an execution attempt and returns the run id.
// fencingToken is 0 until M5 introduces real tokens.
func (s *Store) StartRun(ctx context.Context, jobID string, attempt int, fencingToken int64) (string, error) {
	runID := uuid.NewString()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO job_runs (id, job_id, attempt, fencing_token, started_at)
		VALUES ($1::uuid, $2::uuid, $3, $4, now())`,
		runID, jobID, attempt, fencingToken)
	if err != nil {
		return "", fmt.Errorf("insert job run: %w", err)
	}
	return runID, nil
}

func (s *Store) FinishRun(ctx context.Context, runID, outcome string, errMsg *string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE job_runs SET finished_at = now(), outcome = $2, error_msg = $3
		WHERE id = $1::uuid`,
		runID, outcome, errMsg)
	if err != nil {
		return fmt.Errorf("finish job run: %w", err)
	}
	return nil
}

func (s *Store) RunsForJob(ctx context.Context, jobID string) ([]*model.JobRun, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, job_id::text, attempt, fencing_token, started_at, finished_at, outcome, error_msg
		FROM job_runs WHERE job_id = $1::uuid ORDER BY started_at`,
		jobID)
	if err != nil {
		return nil, fmt.Errorf("list job runs: %w", err)
	}
	defer rows.Close()

	var runs []*model.JobRun
	for rows.Next() {
		var r model.JobRun
		if err := rows.Scan(&r.ID, &r.JobID, &r.Attempt, &r.FencingToken, &r.StartedAt, &r.FinishedAt, &r.Outcome, &r.ErrorMsg); err != nil {
			return nil, fmt.Errorf("scan job run: %w", err)
		}
		runs = append(runs, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list job runs rows: %w", err)
	}
	return runs, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (*model.Job, error) {
	var j model.Job
	err := row.Scan(&j.ID, &j.Name, &j.Payload, &j.ScheduleType, &j.CronExpr, &j.RunAt,
		&j.DependsOn, &j.MaxRetries, &j.Attempt, &j.Status, &j.CreatedAt)
	if err != nil {
		return nil, err
	}
	// Convention: all times UTC.
	if j.RunAt != nil {
		t := j.RunAt.UTC()
		j.RunAt = &t
	}
	j.CreatedAt = j.CreatedAt.UTC()
	if j.DependsOn == nil {
		j.DependsOn = []string{}
	}
	return &j, nil
}
