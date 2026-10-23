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

const (
	// closeTimeout bounds closing a lock connection.
	closeTimeout = 5 * time.Second
	// txControlTimeout bounds BEGIN, COMMIT and ROLLBACK on the lock
	// connection, which ignore the caller's cancellation.
	txControlTimeout = 5 * time.Second
	// lockCheckEvery is how often a held lock checks its connection while no
	// run write is in flight, and lockCheckTimeout bounds one check.
	lockCheckEvery   = 5 * time.Second
	lockCheckTimeout = 5 * time.Second
)

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

// pgIngestLock is a held ingest lock. It owns a dedicated connection, dialed
// with the pool's settings but outside the pool, so the pool never waits for
// it and it is never handed back while it may hold the lock. Only this
// connection cancels a canceled query on the server rather than closing (see
// withServerCancel), so a canceled run write keeps the lock. The run writes go through
// this connection, so they prove that this session holds the lock, and they
// keep the connection busy during a run. A watcher goroutine checks the
// connection every p.lockCheckInterval while no write is in flight.
type pgIngestLock struct {
	p    *Postgres
	conn *pgx.Conn

	sem       chan struct{} // capacity 1: serializes use of conn, waitable with a context
	released  atomic.Bool
	aborted   atomic.Bool   // the store closed the connection
	closing   chan struct{} // closed with aborted; cuts short a wait for a cancel request
	closeOnce sync.Once

	done          chan struct{} // closed by end
	endOnce       sync.Once
	endErr        error         // written before done is closed
	watcherExited chan struct{} // closed when the watcher returns
}

func newIngestLock(p *Postgres, conn *pgx.Conn, closing chan struct{}) *pgIngestLock {
	return &pgIngestLock{p: p, conn: conn, sem: make(chan struct{}, 1), closing: closing,
		done: make(chan struct{}), watcherExited: make(chan struct{})}
}

// acquireConn waits for exclusive use of the connection, or for ctx.
func (l *pgIngestLock) acquireConn(ctx context.Context) error {
	select {
	case l.sem <- struct{}{}:
		return nil
	default:
	}
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tryAcquireConn takes the connection only if it is free.
func (l *pgIngestLock) tryAcquireConn() bool {
	select {
	case l.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *pgIngestLock) releaseConn() { <-l.sem }

// Done implements IngestLock.
func (l *pgIngestLock) Done() <-chan struct{} { return l.done }

// Err implements IngestLock.
func (l *pgIngestLock) Err() error {
	select {
	case <-l.done:
		return l.endErr
	default:
		return nil
	}
}

// end records why the lock is no longer held (err wraps ErrLockNotHeld) and
// closes Done. Only the first call has an effect; it reports whether this
// call was it.
func (l *pgIngestLock) end(err error) (first bool) {
	l.endOnce.Do(func() {
		l.endErr = err
		close(l.done)
		first = true
	})
	return first
}

// lost ends the lock because its connection, and so its session, is gone.
// It logs at ERROR only when that is news: when Release or Close ended the
// lock first (and so closed the connection), the error is theirs.
func (l *pgIngestLock) lost(ctx context.Context, cause error) error {
	if l.end(fmt.Errorf("%w: lock connection lost: %w", ErrLockNotHeld, cause)) {
		l.p.logger.ErrorContext(ctx, "ingest lock lost", slog.Any("error", cause))
	} else {
		l.p.logger.DebugContext(ctx, "ingest lock connection closed after the lock ended",
			slog.Any("error", cause), slog.Any("reason", l.Err()))
	}
	return l.Err()
}

// watch checks the lock's connection periodically until the lock ends. A
// non-positive interval uses lockCheckEvery.
func (l *pgIngestLock) watch(ctx context.Context, every time.Duration) {
	defer close(l.watcherExited)
	if every <= 0 {
		every = lockCheckEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-t.C:
			l.check(ctx)
		}
	}
}

// check asks the server whether this session still holds the lock. It skips
// the round when a run write is in flight: that write checks the connection
// itself. A slow answer is canceled on the server and is not a loss; only a
// dead connection or a session without the lock ends the lock.
func (l *pgIngestLock) check(ctx context.Context) {
	if !l.tryAcquireConn() {
		return
	}
	defer l.releaseConn()
	if l.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, lockCheckTimeout)
	defer cancel()
	held, err := queryLock(ctx, l.conn, lockHeldBySelfSQL)
	switch {
	case err != nil && (l.aborted.Load() || l.conn.IsClosed()):
		_ = l.lost(ctx, err)
	case err == nil && !held:
		if l.end(fmt.Errorf("%w: no longer held by its session", ErrLockNotHeld)) {
			l.p.logger.ErrorContext(ctx, "ingest lock lost: no longer held by its session")
		}
	}
}

// AcquireIngestLock takes pg_try_advisory_lock(IngestLockKey) on a dedicated
// connection (the pool's settings, outside the pool), or returns ErrLockHeld
// if another session holds it. The lock is held until Release, until Close
// (which closes the connection), or until the process dies: Postgres
// releases it when the session ends, which is how a crashed run is detected
// (ADR 0010).
//
// Session advisory locks need a session: DATABASE_URL must point to Postgres
// or to a session-mode pooler, never to a transaction-mode pooler such as
// pgbouncer in pool_mode=transaction.
func (p *Postgres) AcquireIngestLock(ctx context.Context) (IngestLock, error) {
	if p.lockConnConfig == nil {
		return nil, errors.New("acquiring ingest lock: store not opened with Open")
	}
	if p.isClosed() {
		return nil, fmt.Errorf("acquiring ingest lock: %w", ErrStoreClosed)
	}
	closing := make(chan struct{})
	conn, err := pgx.ConnectConfig(ctx, withServerCancel(p.lockConnConfig.Copy(), closing, p.logger))
	if err != nil {
		return nil, fmt.Errorf("connecting for ingest lock: %w", err)
	}
	var locked bool
	if err := conn.QueryRow(ctx, tryLockSQL, IngestLockKey).Scan(&locked); err != nil {
		// The outcome is unknown (the lock may have been granted before the
		// error); closing the session releases it in any case.
		closeConn(ctx, conn)
		return nil, fmt.Errorf("trying ingest lock: %w", err)
	}
	if !locked {
		closeConn(ctx, conn)
		return nil, ErrLockHeld
	}

	l := newIngestLock(p, conn, closing)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		closeConn(ctx, l.conn)
		return nil, fmt.Errorf("acquiring ingest lock: %w", ErrStoreClosed)
	}
	p.locks[l] = struct{}{}
	go l.watch(context.WithoutCancel(ctx), p.lockCheckInterval) // outlives the acquiring request
	return l, nil
}

// Release unlocks and closes the lock's connection, and closes Done (Err then
// wraps ErrLockReleased). Later calls return nil. It waits for an in-flight
// run write on the lock, but not beyond ctx: then it closes the connection
// under the write, which releases the lock as well. If unlocking fails (for
// example ctx is already canceled at shutdown), the connection is closed
// anyway. It returns an error wrapping ErrLockNotHeld only if the session
// turns out not to have held the lock.
func (l *pgIngestLock) Release(ctx context.Context) error {
	err := l.release(ctx)
	<-l.watcherExited
	return err
}

func (l *pgIngestLock) release(ctx context.Context) error {
	if !l.released.CompareAndSwap(false, true) {
		return nil
	}
	l.p.forget(l)
	l.end(fmt.Errorf("%w: %w", ErrLockNotHeld, ErrLockReleased))
	if err := l.acquireConn(ctx); err != nil {
		// A write is still running and ctx is done: end the session under it.
		l.closeSocket()
		l.p.logger.WarnContext(ctx, "ingest lock released by closing its connection during a write",
			slog.Any("error", err))
		return nil
	}
	defer l.releaseConn()
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
	l.end(fmt.Errorf("%w: %w", ErrLockNotHeld, ErrStoreClosed))
	l.closeSocket()
}

// closeSocket closes the network connection without waiting for an in-flight
// query (net.Conn.Close is safe during a read); the server ends the session.
// A canceled query waiting for its cancel request to be acknowledged stops
// waiting too (serverCancelHandler).
func (l *pgIngestLock) closeSocket() {
	l.aborted.Store(true)
	l.closeOnce.Do(func() { close(l.closing) })
	_ = l.conn.PgConn().Conn().Close()
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

// withLock runs fn on the lock's own connection. It returns an error
// wrapping ErrLockNotHeld if lock is not a held lock of this store, and ends
// the lock (closing Done) if fn finds the connection, and with it the lock,
// lost. Waiting for the connection (behind another write or a liveness
// check) is bounded by ctx. A done ctx is returned as is, without touching
// the connection; cancellation during fn cancels the query on the server and
// keeps the session (see Open).
func (p *Postgres) withLock(ctx context.Context, lock IngestLock, fn func(*pgx.Conn) error) error {
	l, ok := lock.(*pgIngestLock)
	if !ok || l == nil || l.p != p {
		return fmt.Errorf("%w: not an ingest lock of this store", ErrLockNotHeld)
	}
	if err := l.Err(); err != nil {
		return err
	}
	if err := l.acquireConn(ctx); err != nil {
		return err
	}
	defer l.releaseConn()
	if err := l.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := fn(l.conn)
	switch {
	case err == nil:
		return nil
	case l.Err() != nil: // Release or Close ended the lock meanwhile
		return l.Err()
	case l.aborted.Load() || l.conn.IsClosed():
		return l.lost(ctx, err)
	case errors.Is(err, ErrLockNotHeld):
		l.end(err)
	}
	return err
}

// txControlContext bounds BEGIN, COMMIT and ROLLBACK on the lock connection
// without the caller's cancellation: pgx closes the connection when one of
// them fails for a non-server reason, which would drop the lock.
func txControlContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), txControlTimeout)
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
	err := p.withLock(ctx, lock, func(conn *pgx.Conn) (err error) {
		beginCtx, cancel := txControlContext(ctx)
		defer cancel()
		tx, err := conn.Begin(beginCtx)
		if err != nil {
			return fmt.Errorf("beginning run transaction: %w", err)
		}
		defer func() {
			rbCtx, cancel := txControlContext(ctx)
			defer cancel()
			if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("rolling back run transaction: %w", rbErr))
			}
		}()

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
		commitCtx, cancelCommit := txControlContext(ctx)
		defer cancelCommit()
		if err := tx.Commit(commitCtx); err != nil {
			return fmt.Errorf("committing run start: %w", err)
		}
		return nil
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
	return p.withLock(ctx, lock, func(conn *pgx.Conn) error {
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
	return p.withLock(ctx, lock, func(conn *pgx.Conn) error {
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
