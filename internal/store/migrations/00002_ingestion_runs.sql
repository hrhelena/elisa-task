-- Ingestion run state, shared by every replica (ADR 0010). At most one run is
-- 'running': the session advisory lock enforces it in the application, and the
-- partial unique index enforces it in the database.
--
-- Invariant at the end of a run: files_seen = messages_processed + messages_skipped (ADR 0007).

-- +goose Up
CREATE TABLE ingestion_runs (
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state              TEXT        NOT NULL CHECK (state IN ('running', 'completed', 'failed')),
    started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at        TIMESTAMPTZ,
    files_seen         BIGINT      NOT NULL DEFAULT 0,
    messages_processed BIGINT      NOT NULL DEFAULT 0, -- parsed OK and stored (inserted now or already present)
    messages_inserted  BIGINT      NOT NULL DEFAULT 0, -- new rows written by this run
    messages_skipped   BIGINT      NOT NULL DEFAULT 0, -- file-level problems, not counted as messages
    skip_reasons       JSONB       NOT NULL DEFAULT '{}', -- {"empty_file": 2, "not_an_email": 1}
    field_errors       JSONB       NOT NULL DEFAULT '{}', -- {"invalid_sender": 1, "invalid_date": 4}
    error              TEXT,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A run has a finish time exactly when it is no longer running.
    CONSTRAINT ingestion_runs_finished_at_matches_state CHECK ((state = 'running') = (finished_at IS NULL))
);

CREATE UNIQUE INDEX ingestion_runs_one_running ON ingestion_runs ((true)) WHERE state = 'running';

ALTER TABLE messages ADD CONSTRAINT messages_run_fk FOREIGN KEY (run_id) REFERENCES ingestion_runs (id);

-- +goose Down
ALTER TABLE messages DROP CONSTRAINT messages_run_fk;
DROP TABLE ingestion_runs;
