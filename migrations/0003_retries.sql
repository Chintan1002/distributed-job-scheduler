-- 0003_retries.sql — attempt counter for retries (M6).
-- Incremented atomically by every successful claim (one bump per execution
-- start), so "which attempt is this?" never needs a separate count query.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS attempt INT NOT NULL DEFAULT 0;
