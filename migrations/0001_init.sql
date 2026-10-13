-- 0001_init.sql — jobs + job_runs.
-- IF NOT EXISTS added so `make migrate` is safely re-runnable.

CREATE TABLE IF NOT EXISTS jobs (
    id            UUID PRIMARY KEY,
    name          TEXT NOT NULL,
    payload       JSONB,                 -- what the job does
    schedule_type TEXT NOT NULL,         -- 'once' | 'cron'
    cron_expr     TEXT,                  -- if cron
    run_at        TIMESTAMPTZ,           -- next fire time
    depends_on    UUID[] DEFAULT '{}',   -- DAG parents
    max_retries   INT DEFAULT 3,
    status        TEXT NOT NULL,         -- 'scheduled'|'queued'|'running'|'succeeded'|'failed'|'cancelled'
    created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs (run_at) WHERE status = 'scheduled';

CREATE TABLE IF NOT EXISTS job_runs (
    id            UUID PRIMARY KEY,
    job_id        UUID REFERENCES jobs(id),
    attempt       INT NOT NULL,
    fencing_token BIGINT NOT NULL,       -- monotonic, prevents double-run (real values arrive in M5)
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    outcome       TEXT,                  -- 'success'|'error'
    error_msg     TEXT
);
