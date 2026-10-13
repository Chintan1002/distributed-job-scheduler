-- 0002_fencing.sql — fencing-token high-water mark (M5).
-- Statuses cycle (scheduled → queued → ... and back on requeue), so they are
-- not monotonic; the token is. Every fenced write must carry a token >= this
-- column, which makes a zombie leader's stale writes rejectable forever.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_fencing_token BIGINT NOT NULL DEFAULT 0;
