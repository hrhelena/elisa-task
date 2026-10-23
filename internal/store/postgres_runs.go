package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// IngestLockKey is the session advisory lock key that serializes ingestion
// across the cluster (ADR 0010): "enron" in ASCII. With the single-bigint
// form, pg_locks shows it as classid = key>>32, objid = key&0xffffffff,
// objsubid = 1.
const IngestLockKey int64 = 0x656E726F6E

// closeTimeout bounds closing a lock connection.
const closeTimeout = 5 * time.Second

// LatestRun test hook stages.
const (
	hookAfterRead      = "after-read"
	hookAfterLockCheck = "after-lock-check"
)

const (
	runColumnList = "id, state, started_at, finished_at, files_seen, messages_processed, messages_inserted, " +
		"messages_skipped, skip_reasons, field_errors, error, updated_at"

	tryLockSQL = "SELECT pg_try_advisory_lock($1)"
	unlockSQL  = "SELECT pg_advisory_unlock($1)"
	// Advisory locks are per database, so the filter is restricted to the
	// current one (several databases may share a server).
	lockFilter = `locktype = 'advisory' AND granted
  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1`
	lockHeldSQL       = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE " + lockFilter + ")"
	lockHeldBySelfSQL = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE " + lockFilter + " AND pid = pg_backend_pid())"

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

// pgIngestLock is a held ingest lock. It owns its connection: the connection
// is taken from the pool and hijacked, so the pool never waits for it and it
// is never handed back while it may hold the lock. The run writes go through
// this connection, so they prove that this session holds the lock, and they
// keep the connection busy during a run.
type pgIngestLock struct {
	p    *Postgres
	conn *pgx.Conn

	mu       sync.Mutex // serializes use of conn; guards released
	released bool
	aborted  atomic.Bool // the store closed the connection
}

// AcquireIngestLock takes pg_try_advisory_lock(IngestLockKey) on a dedicated
// connection taken from the pool, or returns ErrLockHeld if another session
// holds it. The lock is held until Release, until Close (which closes the
// connection), or until the process dies: Postgres releases it when the
// session ends, which is how a crashed run is detected (ADR 0010).
//
// Session advisory locks need a session: DATABASE_URL must point to Postgres
// or to a session-mode pooler, never to a transaction-mode pooler such as
// pgbouncer in pool_mode=transaction.
func (p *Postgres) AcquireIngestLock(ctx context.Context) (IngestLock, error) {
	pc, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquiring connection for ingest lock: %w", err)
	}
	var locked bool
	if err := pc.QueryRow(ctx, tryLockSQL, IngestLockKey).Scan(&locked); err != nil {
		// The outcome is unknown (the lock may have been granted before the
		// error), so the session must not go back to the pool.
		closeConn(ctx, pc.Hijack())
		return nil, fmt.Errorf("trying ingest lock: %w", err)
	}
	if !locked {
		pc.Release()
		return nil, ErrLockHeld
	}

	l := &pgIngestLock{p: p, conn: pc.Hijack()}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		closeConn(ctx, l.conn)
		return nil, errors.New("acquiring ingest lock: store is closed")
	}
	p.locks[l] = struct{}{}
	return l, nil
}

// Release unlocks and closes the lock's connection. Later calls return nil.
// If unlocking fails (for example ctx is already canceled at shutdown), the
// connection is closed anyway, which releases the lock too. It returns an
// error wrapping ErrLockNotHeld only if the session turns out not to have
// held the lock.
func (l *pgIngestLock) Release(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	l.released = true
	l.p.forget(l)
	if l.aborted.Load() {
		closeConn(ctx, l.conn)
		return nil
	}

	var unlocked bool
	err := l.conn.QueryRow(ctx, unlockSQL, IngestLockKey).Scan(&unlocked)
	closeConn(ctx, l.conn)
	if err != nil {
		l.p.logger.WarnContext(ctx, "ingest lock released by closing its connection", slog.Any("error", err))
		return nil
	}
	if !unlocked {
		return fmt.Errorf("releasing ingest lock: %w by this session", ErrLockNotHeld)
	}
	return nil
}

// abort closes the lock's network connection without waiting for an
// in-flight query, which ends the session and releases the lock. Used by
// Close; a later Release only cleans up.
func (l *pgIngestLock) abort() {
	l.aborted.Store(true)
	_ = l.conn.PgConn().Conn().Close() // net.Conn.Close is safe during a read
}

func (p *Postgres) forget(l *pgIngestLock) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.locks, l) // no-op once Close has taken the set
}

// closeConn closes conn, bounded by closeTimeout even if ctx is canceled.
func closeConn(ctx context.Context, conn *pgx.Conn) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()
	_ = conn.Close(closeCtx) // the server ends the session either way
}

// withLock runs fn on the lock's own connection. It returns ErrLockNotHeld
// if lock is not a held lock of this store, and wraps ErrLockNotHeld into
// fn's error if the connection, and with it the lock, was lost.
func (p *Postgres) withLock(lock IngestLock, fn func(*pgx.Conn) error) error {
	l, ok := lock.(*pgIngestLock)
	if !ok || l == nil || l.p != p {
		return fmt.Errorf("%w: not an ingest lock of this store", ErrLockNotHeld)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.aborted.Load() {
		return fmt.Errorf("%w: released", ErrLockNotHeld)
	}
	err := fn(l.conn)
	if err != nil && (l.aborted.Load() || l.conn.IsClosed()) {
		return fmt.Errorf("%w: lock connection lost: %w", ErrLockNotHeld, err)
	}
	return err
}

// IngestLockHeld reports whether any session holds the ingest lock, by reading
// pg_locks. It does not take the lock.
func (p *Postgres) IngestLockHeld(ctx context.Context) (bool, error) {
	return queryLock(ctx, p.pool, lockHeldSQL)
}

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func queryLock(ctx context.Context, q rowQuerier, sql string) (bool, error) {
	var held bool
	if err := q.QueryRow(ctx, sql, IngestLockKey>>32, IngestLockKey&0xffffffff).Scan(&held); err != nil {
		return false, fmt.Errorf("checking ingest lock: %w", err)
	}
	return held, nil
}

// BeginRun starts a run under lock. In one transaction on the lock's own
// connection it checks that this session holds the lock (else
// ErrLockNotHeld), marks every run still 'running' as failed with error
// "interrupted" (their owner died, since the lock was free), and inserts and
// returns a new running run with all counters zero.
func (p *Postgres) BeginRun(ctx context.Context, lock IngestLock) (Run, error) {
	var (
		run         Run
		interrupted []int64
	)
	err := p.withLock(lock, func(conn *pgx.Conn) error {
		return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			held, err := queryLock(ctx, tx, lockHeldBySelfSQL)
			if err != nil {
				return err
			}
			if !held {
				return fmt.Errorf("beginning run: %w by this session", ErrLockNotHeld)
			}
			rows, err := tx.Query(ctx, interruptRunningSQL, ErrorInterrupted)
			if err != nil {
				return fmt.Errorf("marking interrupted runs: %w", err)
			}
			if interrupted, err = pgx.CollectRows(rows, pgx.RowTo[int64]); err != nil {
				return fmt.Errorf("marking interrupted runs: %w", err)
			}
			if run, err = scanRun(tx.QueryRow(ctx, insertRunSQL)); err != nil {
				return fmt.Errorf("inserting run: %w", err)
			}
			return nil
		})
	})
	if err != nil {
		return Run{}, err
	}
	for _, id := range interrupted {
		p.logger.WarnContext(ctx, "ingestion run was interrupted by a crash, marked failed",
			slog.Int64("run_id", id), slog.Int64("new_run_id", run.ID))
	}
	return run, nil
}

// UpdateRunProgress stores the current counters of a running run, through
// the lock's connection. It returns ErrRunNotRunning if run id does not exist
// or is no longer running, and ErrLockNotHeld if the lock is not held.
func (p *Postgres) UpdateRunProgress(ctx context.Context, lock IngestLock, id int64, c Counters) error {
	args, err := counterArgs(id, c)
	if err != nil {
		return err
	}
	return p.withLock(lock, func(conn *pgx.Conn) error {
		tag, err := conn.Exec(ctx, updateRunSQL, args...)
		if err != nil {
			return fmt.Errorf("updating run %d progress: %w", id, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("updating run %d progress: %w", id, ErrRunNotRunning)
		}
		return nil
	})
}

// FinishRun stores the final counters and state (completed or failed, else
// ErrInvalidRunState) of a running run, with an optional error message,
// through the lock's connection. A run can be finished only once: if it is
// not running, or does not exist, it returns ErrRunNotRunning. It returns
// ErrLockNotHeld if the lock is not held.
func (p *Postgres) FinishRun(ctx context.Context, lock IngestLock, id int64, state RunState, c Counters,
	errMsg *string,
) error {
	if state != RunCompleted && state != RunFailed {
		return fmt.Errorf("finishing run %d as %q: %w", id, state, ErrInvalidRunState)
	}
	args, err := counterArgs(id, c)
	if err != nil {
		return err
	}
	args = append(args, string(state), sanitizeOptional(errMsg))
	return p.withLock(lock, func(conn *pgx.Conn) error {
		tag, err := conn.Exec(ctx, finishRunSQL, args...)
		if err != nil {
			return fmt.Errorf("finishing run %d: %w", id, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("finishing run %d: %w", id, ErrRunNotRunning)
		}
		return nil
	})
}

// LatestRun returns the run with the highest id, or (nil, nil) if there has
// never been one.
//
// A run stored as running while nobody holds the ingest lock has lost its
// owner (crash, ADR 0010). It is reported as failed with error "interrupted"
// and a nil FinishedAt, without changing the stored row; the next BeginRun
// records that. The check is: read run R as running, find the lock free,
// then read the latest run again and find the same R still running. A live
// owner holds the lock from before R is inserted until after R is finished,
// so it would have been seen holding the lock. In any other outcome the
// fresh read is returned as stored.
func (p *Postgres) LatestRun(ctx context.Context) (*Run, error) {
	first, err := p.readLatestRun(ctx)
	if first == nil || err != nil || first.State != RunRunning {
		return first, err
	}
	p.hook(hookAfterRead)
	held, err := p.IngestLockHeld(ctx)
	if err != nil {
		return nil, err
	}
	p.hook(hookAfterLockCheck)
	again, err := p.readLatestRun(ctx)
	if again == nil || err != nil {
		return again, err
	}
	if !held && again.ID == first.ID && again.State == RunRunning {
		again.State = RunFailed
		msg := ErrorInterrupted
		again.Error = &msg
	}
	return again, nil
}

func (p *Postgres) readLatestRun(ctx context.Context) (*Run, error) {
	run, err := scanRun(p.pool.QueryRow(ctx, latestRunSQL))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // no run yet (idle) is not an error
	}
	if err != nil {
		return nil, fmt.Errorf("reading latest run: %w", err)
	}
	return &run, nil
}

func (p *Postgres) hook(stage string) {
	if p.testHook != nil {
		p.testHook(stage)
	}
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
