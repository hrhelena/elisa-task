package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// IngestLockKey is the session advisory lock key that serializes ingestion
// across the cluster (ADR 0010): "enron" in ASCII. With the single-bigint
// form, pg_locks shows it as classid = key>>32, objid = key&0xffffffff,
// objsubid = 1.
const IngestLockKey int64 = 0x656E726F6E

// discardTimeout bounds closing a connection that is being thrown away.
const discardTimeout = 5 * time.Second

const (
	runColumnList = "id, state, started_at, finished_at, files_seen, messages_processed, messages_inserted, " +
		"messages_skipped, skip_reasons, field_errors, error, updated_at"

	tryLockSQL = "SELECT pg_try_advisory_lock($1)"
	unlockSQL  = "SELECT pg_advisory_unlock($1)"
	// Advisory locks are per database, so the check is restricted to the
	// current one (several databases may share a server).
	lockHeldSQL = `SELECT EXISTS (SELECT 1 FROM pg_locks
WHERE locktype = 'advisory' AND granted
  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1)`

	interruptRunningSQL = `UPDATE ingestion_runs
SET state = 'failed', error = $1, finished_at = now(), updated_at = now()
WHERE state = 'running' RETURNING id`
	insertRunSQL = "INSERT INTO ingestion_runs (state) VALUES ('running') RETURNING " + runColumnList
	// Only a running run accepts progress: a run that was finished, or marked
	// interrupted by another replica, must not be resurrected.
	updateRunSQL = `UPDATE ingestion_runs
SET files_seen = $2, messages_processed = $3, messages_inserted = $4, messages_skipped = $5,
    skip_reasons = $6, field_errors = $7, updated_at = now()
WHERE id = $1 AND state = 'running'`
	finishRunSQL = `UPDATE ingestion_runs
SET files_seen = $2, messages_processed = $3, messages_inserted = $4, messages_skipped = $5,
    skip_reasons = $6, field_errors = $7, state = $8, error = $9, finished_at = now(), updated_at = now()
WHERE id = $1 AND state = 'running'`
	latestRunSQL = "SELECT " + runColumnList + " FROM ingestion_runs ORDER BY id DESC LIMIT 1"
)

// AcquireIngestLock takes pg_try_advisory_lock(IngestLockKey) on a connection
// taken from the pool, which stays checked out until release is called. It
// returns ErrLockHeld if another session holds the lock. If the process dies,
// Postgres releases the lock when the session ends: that is how a crashed run
// is detected (ADR 0010).
//
// release unlocks and returns the connection to the pool; it is safe to call
// more than once. If unlocking fails (for example ctx is already canceled at
// shutdown), the connection is closed instead, which also releases the lock.
// release must be called before Close, which waits for checked-out
// connections. The pool needs at least one connection beyond the lock for the
// run's own queries.
func (p *Postgres) AcquireIngestLock(ctx context.Context) (release func(context.Context) error, err error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring connection for ingest lock: %w", err)
	}
	var locked bool
	if err := conn.QueryRow(ctx, tryLockSQL, IngestLockKey).Scan(&locked); err != nil {
		// The outcome is unknown (the lock may have been granted before the
		// error), so the session must not go back to the pool.
		discardConn(ctx, conn)
		return nil, fmt.Errorf("trying ingest lock: %w", err)
	}
	if !locked {
		conn.Release()
		return nil, ErrLockHeld
	}

	var (
		once       sync.Once
		releaseErr error
	)
	return func(ctx context.Context) error {
		once.Do(func() { releaseErr = p.unlock(ctx, conn) })
		return releaseErr
	}, nil
}

// unlock releases the ingest lock held by conn and returns conn to the pool.
func (p *Postgres) unlock(ctx context.Context, conn *pgxpool.Conn) error {
	var unlocked bool
	if err := conn.QueryRow(ctx, unlockSQL, IngestLockKey).Scan(&unlocked); err != nil {
		// Ending the session releases the lock just as well.
		discardConn(ctx, conn)
		p.logger.WarnContext(ctx, "ingest lock released by closing its connection", slog.Any("error", err))
		return nil
	}
	if !unlocked {
		// The session no longer held the lock: another replica may have run
		// concurrently. Do not reuse a session in an unexpected state.
		discardConn(ctx, conn)
		return fmt.Errorf("releasing ingest lock: %w by this session", ErrLockNotHeld)
	}
	conn.Release()
	return nil
}

// discardConn removes conn from the pool and closes it.
func discardConn(ctx context.Context, conn *pgxpool.Conn) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), discardTimeout)
	defer cancel()
	_ = conn.Hijack().Close(closeCtx) // the server ends the session either way
}

// IngestLockHeld reports whether any session holds the ingest lock, by reading
// pg_locks. It does not take the lock.
func (p *Postgres) IngestLockHeld(ctx context.Context) (bool, error) {
	held, err := queryLockHeld(ctx, p.pool)
	if err != nil {
		return false, err
	}
	return held, nil
}

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func queryLockHeld(ctx context.Context, q rowQuerier) (bool, error) {
	var held bool
	if err := q.QueryRow(ctx, lockHeldSQL, IngestLockKey>>32, IngestLockKey&0xffffffff).Scan(&held); err != nil {
		return false, fmt.Errorf("checking ingest lock: %w", err)
	}
	return held, nil
}

// BeginRun must be called while holding the ingest lock; it returns
// ErrLockNotHeld if no session holds it. In one transaction it marks every
// run still 'running' as failed with error "interrupted" (their owner died,
// since the lock was free), then inserts and returns a new running run with
// all counters zero.
func (p *Postgres) BeginRun(ctx context.Context) (run Run, err error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Run{}, fmt.Errorf("beginning run transaction: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rolling back run transaction: %w", rbErr))
		}
	}()

	held, err := queryLockHeld(ctx, tx)
	if err != nil {
		return Run{}, err
	}
	if !held {
		return Run{}, fmt.Errorf("beginning run: %w", ErrLockNotHeld)
	}

	rows, err := tx.Query(ctx, interruptRunningSQL, ErrorInterrupted)
	if err != nil {
		return Run{}, fmt.Errorf("marking interrupted runs: %w", err)
	}
	interrupted, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return Run{}, fmt.Errorf("marking interrupted runs: %w", err)
	}

	run, err = scanRun(tx.QueryRow(ctx, insertRunSQL))
	if err != nil {
		return Run{}, fmt.Errorf("inserting run: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Run{}, fmt.Errorf("committing run start: %w", err)
	}
	for _, id := range interrupted {
		p.logger.WarnContext(ctx, "ingestion run was interrupted by a crash, marked failed",
			slog.Int64("run_id", id), slog.Int64("new_run_id", run.ID))
	}
	return run, nil
}

// UpdateRunProgress stores the current counters of a running run. It returns
// ErrRunNotRunning if run id does not exist or is no longer running.
func (p *Postgres) UpdateRunProgress(ctx context.Context, id int64, c Counters) error {
	args, err := counterArgs(id, c)
	if err != nil {
		return err
	}
	tag, err := p.pool.Exec(ctx, updateRunSQL, args...)
	if err != nil {
		return fmt.Errorf("updating run %d progress: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("updating run %d progress: %w", id, ErrRunNotRunning)
	}
	return nil
}

// FinishRun stores the final counters and state (completed or failed, else
// ErrInvalidRunState) of a running run, with an optional error message. A run
// can be finished only once: if it is not running, or does not exist, it
// returns ErrRunNotRunning.
func (p *Postgres) FinishRun(ctx context.Context, id int64, state RunState, c Counters, errMsg *string) error {
	if state != RunCompleted && state != RunFailed {
		return fmt.Errorf("finishing run %d as %q: %w", id, state, ErrInvalidRunState)
	}
	args, err := counterArgs(id, c)
	if err != nil {
		return err
	}
	args = append(args, string(state), sanitizeOptional(errMsg))
	tag, err := p.pool.Exec(ctx, finishRunSQL, args...)
	if err != nil {
		return fmt.Errorf("finishing run %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("finishing run %d: %w", id, ErrRunNotRunning)
	}
	return nil
}

// LatestRun returns the run with the highest id, or (nil, nil) if there has
// never been one.
//
// A run stored as running while nobody holds the ingest lock has lost its
// owner (crash, ADR 0010). It is reported as failed with error
// "interrupted" and a nil FinishedAt, without changing the stored row; the
// next BeginRun records that. The lock is checked before and after reading
// the row, so a run that is just starting or just finishing is never
// misreported: the lock is taken before a run is inserted and released only
// after it is finished.
func (p *Postgres) LatestRun(ctx context.Context) (*Run, error) {
	heldBefore, err := p.IngestLockHeld(ctx)
	if err != nil {
		return nil, err
	}
	run, err := scanRun(p.pool.QueryRow(ctx, latestRunSQL))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // no run yet (idle) is not an error
	}
	if err != nil {
		return nil, fmt.Errorf("reading latest run: %w", err)
	}
	if run.State != RunRunning || heldBefore {
		return &run, nil
	}
	heldAfter, err := p.IngestLockHeld(ctx)
	if err != nil {
		return nil, err
	}
	if !heldAfter {
		run.State = RunFailed
		msg := ErrorInterrupted
		run.Error = &msg
	}
	return &run, nil
}

// counterArgs returns the query arguments $1..$7 of updateRunSQL and
// finishRunSQL.
func counterArgs(id int64, c Counters) ([]any, error) {
	skipReasons, err := encodeCounts(c.SkipReasons)
	if err != nil {
		return nil, fmt.Errorf("encoding skip reasons of run %d: %w", id, err)
	}
	fieldErrors, err := encodeCounts(c.FieldErrors)
	if err != nil {
		return nil, fmt.Errorf("encoding field errors of run %d: %w", id, err)
	}
	return []any{id, c.FilesSeen, c.MessagesProcessed, c.MessagesInserted, c.MessagesSkipped,
		skipReasons, fieldErrors}, nil
}

// encodeCounts returns m as a JSON object for a JSONB column; nil is "{}".
func encodeCounts(m map[string]int64) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeCounts parses a JSONB counter object. The result is never nil.
func decodeCounts(b []byte) (map[string]int64, error) {
	m := map[string]int64{}
	if len(b) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m == nil { // the JSON literal null
		m = map[string]int64{}
	}
	return m, nil
}

// scanRun reads one row of runColumnList.
func scanRun(row pgx.Row) (Run, error) {
	var (
		r                        Run
		state                    string
		skipReasons, fieldErrors []byte
	)
	if err := row.Scan(&r.ID, &state, &r.StartedAt, &r.FinishedAt, &r.FilesSeen, &r.MessagesProcessed,
		&r.MessagesInserted, &r.MessagesSkipped, &skipReasons, &fieldErrors, &r.Error, &r.UpdatedAt); err != nil {
		return Run{}, err
	}
	r.State = RunState(state)
	var err error
	if r.SkipReasons, err = decodeCounts(skipReasons); err != nil {
		return Run{}, fmt.Errorf("decoding skip reasons of run %d: %w", r.ID, err)
	}
	if r.FieldErrors, err = decodeCounts(fieldErrors); err != nil {
		return Run{}, fmt.Errorf("decoding field errors of run %d: %w", r.ID, err)
	}
	return r, nil
}
