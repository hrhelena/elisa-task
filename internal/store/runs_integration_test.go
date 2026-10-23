//go:build integration

package store

import (
	"context"
	"errors"
	"maps"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Each test runs in its own database. Advisory locks are scoped to a
// database, so parallel tests never contend for the ingest lock.

func mustAcquire(t *testing.T, s *Postgres) IngestLock {
	t.Helper()
	lock, err := s.AcquireIngestLock(t.Context())
	if err != nil {
		t.Fatalf("AcquireIngestLock: %v", err)
	}
	t.Cleanup(func() { _ = lock.Release(context.Background()) })
	return lock
}

func mustBeginRun(t *testing.T, s *Postgres, lock IngestLock) Run {
	t.Helper()
	run, err := s.BeginRun(t.Context(), lock)
	if err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	return run
}

func mustLatestRun(t *testing.T, s *Postgres) *Run {
	t.Helper()
	run, err := s.LatestRun(t.Context())
	if err != nil {
		t.Fatalf("LatestRun: %v", err)
	}
	return run
}

func mustLockHeld(t *testing.T, s *Postgres) bool {
	t.Helper()
	held, err := s.IngestLockHeld(t.Context())
	if err != nil {
		t.Fatalf("IngestLockHeld: %v", err)
	}
	return held
}

// waitFor polls cond until it is true or a deadline passes. The server
// releases a dead session's locks asynchronously.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// killLockHolder terminates the backend holding the ingest lock, as a crashed
// pod would, and waits until the lock is free.
func killLockHolder(t *testing.T, via *Postgres) {
	t.Helper()
	var terminated bool
	if err := via.pool.QueryRow(t.Context(), "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE "+lockFilter,
		IngestLockKey>>32, IngestLockKey&0xffffffff).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("pg_terminate_backend on the lock holder = %v, %v", terminated, err)
	}
	waitFor(t, "the lock to be released", func() bool { return !mustLockHeld(t, via) })
}

// storedRun reads a run row as stored, bypassing LatestRun's reporting.
func storedRun(t *testing.T, s *Postgres, id int64) Run {
	t.Helper()
	run, err := scanRun(s.pool.QueryRow(t.Context(), "SELECT "+runColumnList+" FROM ingestion_runs WHERE id = $1", id))
	if err != nil {
		t.Fatalf("reading run %d: %v", id, err)
	}
	return run
}

// onceAt returns a LatestRun test hook that runs fn the first time stage is
// reached.
func onceAt(stage string, fn func()) func(string) {
	done := false
	return func(s string) {
		if s == stage && !done {
			done = true
			fn()
		}
	}
}

func requireZeroCounters(t *testing.T, run *Run) {
	t.Helper()
	c := run.Counters
	if c.FilesSeen != 0 || c.MessagesProcessed != 0 || c.MessagesInserted != 0 || c.MessagesSkipped != 0 {
		t.Errorf("counters = %+v, want all zero", c)
	}
	if c.SkipReasons == nil || len(c.SkipReasons) != 0 || c.FieldErrors == nil || len(c.FieldErrors) != 0 {
		t.Errorf("maps = %v, %v; want empty and non-nil", c.SkipReasons, c.FieldErrors)
	}
}

func requireRun(t *testing.T, what string, got *Run, id int64, state RunState) {
	t.Helper()
	if got == nil || got.ID != id || got.State != state {
		t.Fatalf("%s = %+v, want run %d %s", what, got, id, state)
	}
}

func TestMigrationCreatesIngestionRuns(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	for _, name := range []string{"public.ingestion_runs", "public.ingestion_runs_one_running",
		"public.messages_run_id_idx"} {
		if !regclassExists(t, s, name) {
			t.Errorf("%s does not exist after Open", name)
		}
	}
}

func TestOpenRejectsPoolMaxConnsBelowTwo(t *testing.T) {
	t.Parallel()
	u, err := url.Parse(newDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("pool_max_conns", "1")
	u.RawQuery = q.Encode()
	s, err := Open(t.Context(), u.String(), discardLogger())
	if err == nil {
		s.Close()
		t.Fatal("Open with pool_max_conns=1 succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "pool_max_conns") {
		t.Errorf("error %q does not name pool_max_conns", err)
	}

	q.Set("pool_max_conns", "2")
	u.RawQuery = q.Encode()
	openStoreAt(t, u.String())
}

// Scenario: First run.
func TestFirstRun(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	if run := mustLatestRun(t, s); run != nil {
		t.Fatalf("LatestRun on a fresh database = %+v, want nil", run)
	}

	run := mustBeginRun(t, s, mustAcquire(t, s))
	if run.ID == 0 || run.State != RunRunning {
		t.Errorf("BeginRun = id %d state %q, want a running run", run.ID, run.State)
	}
	requireZeroCounters(t, &run)
	if run.FinishedAt != nil || run.Error != nil || run.StartedAt.IsZero() || run.UpdatedAt.IsZero() {
		t.Errorf("BeginRun = finished_at %v error %v started_at %v updated_at %v",
			run.FinishedAt, run.Error, run.StartedAt, run.UpdatedAt)
	}

	latest := mustLatestRun(t, s)
	requireRun(t, "LatestRun while running", latest, run.ID, RunRunning)
	if latest.Finished() {
		t.Error("a running run reports Finished")
	}
}

// Scenario: Second session cannot take the lock.
func TestSecondSessionCannotTakeTheLock(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL) // two replicas, two pools

	if mustLockHeld(t, b) {
		t.Fatal("IngestLockHeld before anyone acquired it = true")
	}
	lockA := mustAcquire(t, a)

	if _, err := b.AcquireIngestLock(t.Context()); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("session B AcquireIngestLock error = %v, want ErrLockHeld", err)
	}
	if !mustLockHeld(t, b) {
		t.Error("IngestLockHeld from session B = false, want true")
	}
	// The same replica cannot take it twice either: each call is a new session.
	if _, err := a.AcquireIngestLock(t.Context()); !errors.Is(err, ErrLockHeld) {
		t.Errorf("second AcquireIngestLock on session A's store error = %v, want ErrLockHeld", err)
	}

	if err := lockA.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := lockA.Release(t.Context()); err != nil {
		t.Errorf("second Release: %v, want nil (idempotent)", err)
	}
	if mustLockHeld(t, b) {
		t.Error("IngestLockHeld after Release = true")
	}
	mustAcquire(t, b)
}

// Scenario: Lock is released when the owner's connection dies.
func TestLockIsReleasedWhenOwnersConnectionDies(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)

	lockA := mustAcquire(t, a)
	run1 := mustBeginRun(t, a, lockA)

	killLockHolder(t, b)

	// Before anyone restarts, /status already reports the orphaned run as
	// failed (ADR 0010), without changing the stored row.
	latest := mustLatestRun(t, b)
	requireRun(t, "LatestRun after the owner died", latest, run1.ID, RunFailed)
	if latest.Error == nil || *latest.Error != ErrorInterrupted {
		t.Errorf("error = %v, want %q", latest.Error, ErrorInterrupted)
	}
	if got := storedRun(t, b, run1.ID); got.State != RunRunning {
		t.Errorf("stored run 1 state = %q, want still running until the next BeginRun", got.State)
	}

	run2 := mustBeginRun(t, b, mustAcquire(t, b))
	if run2.ID <= run1.ID || run2.State != RunRunning {
		t.Errorf("run 2 = id %d state %q, want a new running run after %d", run2.ID, run2.State, run1.ID)
	}
	got1 := storedRun(t, b, run1.ID)
	if got1.State != RunFailed || got1.Error == nil || *got1.Error != ErrorInterrupted || got1.FinishedAt == nil {
		t.Errorf("run 1 after BeginRun = state %q error %v finished_at %v, want failed %q with finished_at",
			got1.State, got1.Error, got1.FinishedAt, ErrorInterrupted)
	}
	requireRun(t, "LatestRun", mustLatestRun(t, b), run2.ID, RunRunning)

	// Releasing the dead lock does not fail and does not affect B's lock.
	if err := lockA.Release(t.Context()); err != nil {
		t.Errorf("releasing a lock whose connection died: %v", err)
	}
	if !mustLockHeld(t, a) {
		t.Error("session B's lock was affected by session A's Release")
	}
}

// A replica whose lock connection was dropped (idle timeout, network) must
// not touch the run of the replica that took the lock over.
func TestLostLockCannotWriteRuns(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)

	lockA := mustAcquire(t, a)
	killLockHolder(t, b)
	run1 := mustBeginRun(t, b, mustAcquire(t, b))

	if _, err := a.BeginRun(t.Context(), lockA); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("BeginRun with a lost lock error = %v, want ErrLockNotHeld", err)
	}
	if err := a.UpdateRunProgress(t.Context(), lockA, run1.ID, Counters{FilesSeen: 1}); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("UpdateRunProgress with a lost lock error = %v, want ErrLockNotHeld", err)
	}
	if err := a.FinishRun(t.Context(), lockA, run1.ID, RunFailed, Counters{}, nil); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("FinishRun with a lost lock error = %v, want ErrLockNotHeld", err)
	}
	if got := storedRun(t, b, run1.ID); got.State != RunRunning || got.FilesSeen != 0 {
		t.Errorf("B's run after A's attempts = state %q files_seen %d, want untouched", got.State, got.FilesSeen)
	}
}

func TestRunWritesRequireAHeldLockOfThisStore(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)

	if _, err := a.BeginRun(t.Context(), nil); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("BeginRun(nil lock) error = %v, want ErrLockNotHeld", err)
	}

	lockB := mustAcquire(t, b)
	if _, err := a.BeginRun(t.Context(), lockB); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("BeginRun with another store's lock error = %v, want ErrLockNotHeld", err)
	}
	run := mustBeginRun(t, b, lockB)
	if err := lockB.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := b.BeginRun(t.Context(), lockB); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("BeginRun with a released lock error = %v, want ErrLockNotHeld", err)
	}
	if err := b.UpdateRunProgress(t.Context(), lockB, run.ID, Counters{}); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("UpdateRunProgress with a released lock error = %v, want ErrLockNotHeld", err)
	}
	if err := b.FinishRun(t.Context(), lockB, run.ID, RunCompleted, Counters{}, nil); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("FinishRun with a released lock error = %v, want ErrLockNotHeld", err)
	}
	if run := mustLatestRun(t, a); run.State != RunFailed { // orphaned: lock released while running
		t.Errorf("LatestRun = %+v, want the orphaned run reported failed", run)
	}
}

// BeginRun's guard checks pg_locks for the lock's own backend, not for any
// holder.
func TestBeginRunChecksThisSessionHoldsTheLock(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	lock := mustAcquire(t, s)
	l := lock.(*pgIngestLock)
	if _, err := l.conn.Exec(t.Context(), unlockSQL, IngestLockKey); err != nil {
		t.Fatalf("unlocking behind the handle's back: %v", err)
	}
	other := mustAcquire(t, s) // someone else holds the lock now

	if _, err := s.BeginRun(t.Context(), lock); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("BeginRun on a session that lost the lock error = %v, want ErrLockNotHeld", err)
	}
	if err := lock.Release(t.Context()); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("Release of a session that lost the lock error = %v, want ErrLockNotHeld", err)
	}
	mustBeginRun(t, s, other)
}

// Scenario: Progress and completion.
func TestProgressAndCompletion(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	lock := mustAcquire(t, s)
	run := mustBeginRun(t, s, lock)

	progress := Counters{FilesSeen: 10, MessagesProcessed: 9, MessagesInserted: 9, MessagesSkipped: 1,
		SkipReasons: map[string]int64{"empty_file": 1}, FieldErrors: map[string]int64{"invalid_date": 2}}
	if err := s.UpdateRunProgress(t.Context(), lock, run.ID, progress); err != nil {
		t.Fatalf("UpdateRunProgress: %v", err)
	}
	mid := mustLatestRun(t, s)
	if mid.State != RunRunning || mid.FilesSeen != 10 || !maps.Equal(mid.SkipReasons, progress.SkipReasons) {
		t.Errorf("LatestRun after progress = %+v, want running with the progress counters", mid)
	}

	if err := s.FinishRun(t.Context(), lock, run.ID, RunCompleted, progress, nil); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	if err := lock.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	got := mustLatestRun(t, s)
	requireRun(t, "LatestRun", got, run.ID, RunCompleted)
	if !got.Finished() || got.FinishedAt == nil || got.FinishedAt.Before(got.StartedAt) || got.Error != nil {
		t.Errorf("finished %v, finished_at %v (started %v), error %v; want finished with a finish time and no error",
			got.Finished(), got.FinishedAt, got.StartedAt, got.Error)
	}
	c := got.Counters
	if c.FilesSeen != 10 || c.MessagesProcessed != 9 || c.MessagesInserted != 9 || c.MessagesSkipped != 1 ||
		!maps.Equal(c.SkipReasons, progress.SkipReasons) || !maps.Equal(c.FieldErrors, progress.FieldErrors) {
		t.Errorf("counters = %+v, want %+v", c, progress)
	}
	if c.FilesSeen != c.MessagesProcessed+c.MessagesSkipped {
		t.Errorf("files_seen %d != messages_processed %d + messages_skipped %d",
			c.FilesSeen, c.MessagesProcessed, c.MessagesSkipped)
	}
}

func TestFailedRunRecordsErrorAndIsNotFinished(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	lock := mustAcquire(t, s)
	run := mustBeginRun(t, s, lock)

	errMsg := "data directory unreadable\x00: \xff"
	if err := s.FinishRun(t.Context(), lock, run.ID, RunFailed, Counters{}, &errMsg); err != nil {
		t.Fatalf("FinishRun(failed): %v", err)
	}
	got := mustLatestRun(t, s)
	if got.State != RunFailed || got.Finished() || got.FinishedAt == nil {
		t.Errorf("LatestRun = state %q finished %v finished_at %v, want failed, not finished, with a finish time",
			got.State, got.Finished(), got.FinishedAt)
	}
	if want := "data directory unreadable: �"; got.Error == nil || *got.Error != want {
		t.Errorf("error = %v, want sanitized %q", got.Error, want)
	}
	requireZeroCounters(t, got)
}

// Scenario: Finishing twice is rejected.
func TestFinishingTwiceIsRejected(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	lock := mustAcquire(t, s)
	run := mustBeginRun(t, s, lock)
	final := Counters{FilesSeen: 3, MessagesProcessed: 3}
	if err := s.FinishRun(t.Context(), lock, run.ID, RunCompleted, final, nil); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}

	msg := "late failure"
	if err := s.FinishRun(t.Context(), lock, run.ID, RunFailed, Counters{}, &msg); !errors.Is(err, ErrRunNotRunning) {
		t.Errorf("second FinishRun error = %v, want ErrRunNotRunning", err)
	}
	if err := s.UpdateRunProgress(t.Context(), lock, run.ID, Counters{}); !errors.Is(err, ErrRunNotRunning) {
		t.Errorf("UpdateRunProgress after finish error = %v, want ErrRunNotRunning", err)
	}
	if err := s.FinishRun(t.Context(), lock, run.ID+100, RunCompleted, Counters{}, nil); !errors.Is(err, ErrRunNotRunning) {
		t.Errorf("FinishRun of an unknown run error = %v, want ErrRunNotRunning", err)
	}
	if got := mustLatestRun(t, s); got.State != RunCompleted || got.FilesSeen != 3 || got.Error != nil {
		t.Errorf("run after rejected calls = %+v, want unchanged and completed", got)
	}
}

// Scenario: Database forbids two running rows.
func TestDatabaseForbidsTwoRunningRows(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	mustBeginRun(t, s, mustAcquire(t, s))

	_, err := s.pool.Exec(t.Context(), "INSERT INTO ingestion_runs (state) VALUES ('running')")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "ingestion_runs_one_running" {
		t.Fatalf("second running insert error = %v, want a unique violation on ingestion_runs_one_running", err)
	}
}

// Replicas racing to start: exactly one wins every round.
func TestConcurrentAcquireHasExactlyOneHolder(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	const replicas, rounds = 8, 20
	stores := make([]*Postgres, replicas)
	for i := range stores {
		stores[i] = openStoreAt(t, dbURL)
	}

	for round := range rounds {
		start := make(chan struct{})
		locks := make([]IngestLock, replicas)
		errs := make([]error, replicas)
		var wg sync.WaitGroup
		for i := range stores {
			wg.Go(func() {
				<-start
				locks[i], errs[i] = stores[i].AcquireIngestLock(t.Context())
			})
		}
		close(start)
		wg.Wait()

		var winner IngestLock
		holders := 0
		for i, err := range errs {
			switch {
			case err == nil:
				holders++
				winner = locks[i]
			case !errors.Is(err, ErrLockHeld):
				t.Fatalf("round %d replica %d: AcquireIngestLock: %v", round, i, err)
			}
		}
		if holders != 1 {
			t.Fatalf("round %d: %d holders, want exactly 1", round, holders)
		}
		if err := winner.Release(t.Context()); err != nil {
			t.Fatalf("round %d: Release: %v", round, err)
		}
	}
}

func TestReleaseWithCanceledContextStillReleasesTheLock(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	lock := mustAcquire(t, s)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // as on shutdown
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("Release with a canceled context: %v", err)
	}
	waitFor(t, "the lock to be released", func() bool { return !mustLockHeld(t, s) })
	mustAcquire(t, s)
}

// closeWithin calls s.Close and fails the test if it does not return in time.
func closeWithin(t *testing.T, s *Postgres, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Close()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("Close did not return within %v", d)
	}
}

func TestCloseDropsAForgottenLock(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	s, observer := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	lock, err := s.AcquireIngestLock(t.Context())
	if err != nil {
		t.Fatalf("AcquireIngestLock: %v", err)
	}

	closeWithin(t, s, 10*time.Second) // Release was never called
	waitFor(t, "the lock to be released", func() bool { return !mustLockHeld(t, observer) })
	if err := lock.Release(t.Context()); err != nil {
		t.Errorf("Release after Close: %v", err)
	}
	if _, err := s.BeginRun(t.Context(), lock); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("BeginRun after Close error = %v, want ErrLockNotHeld", err)
	}
	if _, err := s.AcquireIngestLock(t.Context()); err == nil {
		t.Error("AcquireIngestLock after Close succeeded")
	}
}

func TestCloseDoesNotWaitForAQueryOnTheLockConnection(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	s, observer := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	lock, err := s.AcquireIngestLock(t.Context())
	if err != nil {
		t.Fatalf("AcquireIngestLock: %v", err)
	}

	// A run write stuck on the lock connection (here: a long sleep).
	started := make(chan struct{})
	queryErr := make(chan error, 1)
	go func() {
		queryErr <- s.withLock(lock, func(conn *pgx.Conn) error {
			close(started)
			_, err := conn.Exec(context.Background(), "SELECT pg_sleep(60)")
			return err
		})
	}()
	<-started
	waitFor(t, "the sleep to start", func() bool {
		var n int
		_ = observer.pool.QueryRow(t.Context(),
			"SELECT count(*) FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(60)' AND state = 'active'").Scan(&n)
		return n > 0
	})

	closeWithin(t, s, 10*time.Second)
	if err := <-queryErr; !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("interrupted lock query error = %v, want ErrLockNotHeld", err)
	}
	waitFor(t, "the lock to be released", func() bool { return !mustLockHeld(t, observer) })
}

// Reviewer's interleaving: a whole run starts and completes while LatestRun
// is between its queries. The completed run must not be reported as
// interrupted.
func TestLatestRunDoesNotReportACompletedRunAsInterrupted(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)

	lockB := mustAcquire(t, b)
	run := mustBeginRun(t, b, lockB)
	a.testHook = onceAt(hookAfterRead, func() {
		if err := b.FinishRun(t.Context(), lockB, run.ID, RunCompleted, Counters{}, nil); err != nil {
			t.Errorf("FinishRun: %v", err)
		}
		if err := lockB.Release(t.Context()); err != nil {
			t.Errorf("Release: %v", err)
		}
	})

	requireRun(t, "LatestRun", mustLatestRun(t, a), run.ID, RunCompleted)
}

// An orphaned run is replaced by a new run while LatestRun is between its
// queries, either before or after its lock check: the new run is reported.
func TestLatestRunReportsARunThatStartsDuringTheRead(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{hookAfterRead, hookAfterLockCheck} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			dbURL := newDatabase(t)
			a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
			orphan := mustBeginRun(t, a, mustAcquire(t, a))
			killLockHolder(t, b)

			var next Run
			a.testHook = onceAt(stage, func() { next = mustBeginRun(t, b, mustAcquire(t, b)) })

			got := mustLatestRun(t, a)
			if next.ID <= orphan.ID {
				t.Fatalf("hook did not start a new run (orphan %d, next %d)", orphan.ID, next.ID)
			}
			requireRun(t, "LatestRun", got, next.ID, RunRunning)
		})
	}
}

func TestMessagesReferenceTheirRun(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	run := mustBeginRun(t, s, mustAcquire(t, s))

	inRun := msg("allen-p/inbox/1.", "phillip.allen@enron.com")
	inRun.RunID = run.ID
	outsideRun := msg("allen-p/inbox/2.", "phillip.allen@enron.com") // RunID 0 is NULL
	if n := mustInsert(t, s, []Message{inRun, outsideRun}); n != 2 {
		t.Fatalf("inserted %d, want 2", n)
	}

	unknown := msg("allen-p/inbox/3.", "phillip.allen@enron.com")
	unknown.RunID = run.ID + 100
	_, err := s.InsertMessages(t.Context(), []Message{unknown})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.ConstraintName != "messages_run_fk" {
		t.Errorf("insert with an unknown run id error = %v, want a messages_run_fk violation", err)
	}
}
