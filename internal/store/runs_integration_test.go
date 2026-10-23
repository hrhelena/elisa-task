//go:build integration

package store

import (
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Each test runs in its own database. Advisory locks are scoped to a
// database, so parallel tests never contend for the ingest lock.

func mustAcquire(t *testing.T, s *Postgres) func(context.Context) error {
	t.Helper()
	release, err := s.AcquireIngestLock(t.Context())
	if err != nil {
		t.Fatalf("AcquireIngestLock: %v", err)
	}
	t.Cleanup(func() { _ = release(context.Background()) })
	return release
}

func mustBeginRun(t *testing.T, s *Postgres) Run {
	t.Helper()
	run, err := s.BeginRun(t.Context())
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

// storedRun reads a run row as stored, bypassing LatestRun's reporting.
func storedRun(t *testing.T, s *Postgres, id int64) Run {
	t.Helper()
	run, err := scanRun(s.pool.QueryRow(t.Context(), "SELECT "+runColumnList+" FROM ingestion_runs WHERE id = $1", id))
	if err != nil {
		t.Fatalf("reading run %d: %v", id, err)
	}
	return run
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

func TestMigrationCreatesIngestionRuns(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	for _, name := range []string{"public.ingestion_runs", "public.ingestion_runs_one_running"} {
		if !regclassExists(t, s, name) {
			t.Errorf("%s does not exist after Open", name)
		}
	}
}

// Scenario: First run.
func TestFirstRun(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	if run := mustLatestRun(t, s); run != nil {
		t.Fatalf("LatestRun on a fresh database = %+v, want nil", run)
	}

	mustAcquire(t, s)
	run := mustBeginRun(t, s)
	if run.ID == 0 || run.State != RunRunning {
		t.Errorf("BeginRun = id %d state %q, want a running run", run.ID, run.State)
	}
	requireZeroCounters(t, &run)
	if run.FinishedAt != nil || run.Error != nil || run.StartedAt.IsZero() || run.UpdatedAt.IsZero() {
		t.Errorf("BeginRun = finished_at %v error %v started_at %v updated_at %v",
			run.FinishedAt, run.Error, run.StartedAt, run.UpdatedAt)
	}

	latest := mustLatestRun(t, s)
	if latest == nil || latest.ID != run.ID || latest.State != RunRunning || latest.Finished() {
		t.Errorf("LatestRun while running = %+v, want run %d running and not finished", latest, run.ID)
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
	releaseA := mustAcquire(t, a)

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

	if err := releaseA(t.Context()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := releaseA(t.Context()); err != nil {
		t.Errorf("second release: %v, want nil (idempotent)", err)
	}
	if mustLockHeld(t, b) {
		t.Error("IngestLockHeld after release = true")
	}
	mustAcquire(t, b)
}

// Scenario: Lock is released when the owner's connection dies.
func TestLockIsReleasedWhenOwnersConnectionDies(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)

	releaseA := mustAcquire(t, a)
	run1 := mustBeginRun(t, a)

	// Kill session A's lock connection, as a crashed pod would.
	var terminated bool
	if err := b.pool.QueryRow(t.Context(), `SELECT pg_terminate_backend(pid) FROM pg_locks
WHERE locktype = 'advisory' AND granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND objid::bigint = $1`, IngestLockKey&0xffffffff).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("pg_terminate_backend on the lock holder = %v, %v", terminated, err)
	}

	waitFor(t, "the lock to be released", func() bool { return !mustLockHeld(t, b) })

	// Before anyone restarts, /status already reports the orphaned run as
	// failed (ADR 0010), without changing the stored row.
	latest := mustLatestRun(t, b)
	if latest == nil || latest.ID != run1.ID || latest.State != RunFailed || latest.Error == nil ||
		*latest.Error != ErrorInterrupted {
		t.Errorf("LatestRun after the owner died = %+v, want run %d failed with %q", latest, run1.ID, ErrorInterrupted)
	}
	if got := storedRun(t, b, run1.ID); got.State != RunRunning {
		t.Errorf("stored run 1 state = %q, want still running until the next BeginRun", got.State)
	}

	mustAcquire(t, b)
	run2 := mustBeginRun(t, b)
	if run2.ID <= run1.ID || run2.State != RunRunning {
		t.Errorf("run 2 = id %d state %q, want a new running run after %d", run2.ID, run2.State, run1.ID)
	}
	got1 := storedRun(t, b, run1.ID)
	if got1.State != RunFailed || got1.Error == nil || *got1.Error != ErrorInterrupted || got1.FinishedAt == nil {
		t.Errorf("run 1 after BeginRun = state %q error %v finished_at %v, want failed %q with finished_at",
			got1.State, got1.Error, got1.FinishedAt, ErrorInterrupted)
	}
	if latest := mustLatestRun(t, b); latest == nil || latest.ID != run2.ID || latest.State != RunRunning {
		t.Errorf("LatestRun = %+v, want run %d running", latest, run2.ID)
	}

	// The old owner, if it lived on, can no longer write to its run, and
	// releasing its dead lock connection does not fail.
	if err := a.UpdateRunProgress(t.Context(), run1.ID, Counters{FilesSeen: 1}); !errors.Is(err, ErrRunNotRunning) {
		t.Errorf("UpdateRunProgress on the interrupted run error = %v, want ErrRunNotRunning", err)
	}
	if err := releaseA(t.Context()); err != nil {
		t.Errorf("releasing a lock whose connection died: %v", err)
	}
	if !mustLockHeld(t, a) {
		t.Error("session B's lock was affected by session A's release")
	}
}

// Scenario: Progress and completion.
func TestProgressAndCompletion(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	release := mustAcquire(t, s)
	run := mustBeginRun(t, s)

	progress := Counters{FilesSeen: 10, MessagesProcessed: 9, MessagesInserted: 9, MessagesSkipped: 1,
		SkipReasons: map[string]int64{"empty_file": 1}, FieldErrors: map[string]int64{"invalid_date": 2}}
	if err := s.UpdateRunProgress(t.Context(), run.ID, progress); err != nil {
		t.Fatalf("UpdateRunProgress: %v", err)
	}
	mid := mustLatestRun(t, s)
	if mid.State != RunRunning || mid.FilesSeen != 10 || !maps.Equal(mid.SkipReasons, progress.SkipReasons) {
		t.Errorf("LatestRun after progress = %+v, want running with the progress counters", mid)
	}

	if err := s.FinishRun(t.Context(), run.ID, RunCompleted, progress, nil); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	if err := release(t.Context()); err != nil {
		t.Fatalf("release: %v", err)
	}

	got := mustLatestRun(t, s)
	if got == nil || got.ID != run.ID || got.State != RunCompleted || !got.Finished() {
		t.Fatalf("LatestRun = %+v, want run %d completed", got, run.ID)
	}
	if got.FinishedAt == nil || got.FinishedAt.Before(got.StartedAt) || got.Error != nil {
		t.Errorf("finished_at %v (started %v), error %v; want a finish time and no error",
			got.FinishedAt, got.StartedAt, got.Error)
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
	mustAcquire(t, s)
	run := mustBeginRun(t, s)

	errMsg := "data directory unreadable\x00: \xff"
	if err := s.FinishRun(t.Context(), run.ID, RunFailed, Counters{}, &errMsg); err != nil {
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
	mustAcquire(t, s)
	run := mustBeginRun(t, s)
	final := Counters{FilesSeen: 3, MessagesProcessed: 3}
	if err := s.FinishRun(t.Context(), run.ID, RunCompleted, final, nil); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}

	msg := "late failure"
	if err := s.FinishRun(t.Context(), run.ID, RunFailed, Counters{}, &msg); !errors.Is(err, ErrRunNotRunning) {
		t.Errorf("second FinishRun error = %v, want ErrRunNotRunning", err)
	}
	if err := s.UpdateRunProgress(t.Context(), run.ID, Counters{}); !errors.Is(err, ErrRunNotRunning) {
		t.Errorf("UpdateRunProgress after finish error = %v, want ErrRunNotRunning", err)
	}
	if err := s.FinishRun(t.Context(), run.ID+100, RunCompleted, Counters{}, nil); !errors.Is(err, ErrRunNotRunning) {
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
	mustAcquire(t, s)
	mustBeginRun(t, s)

	_, err := s.pool.Exec(t.Context(), "INSERT INTO ingestion_runs (state) VALUES ('running')")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" || pgErr.ConstraintName != "ingestion_runs_one_running" {
		t.Fatalf("second running insert error = %v, want a unique violation on ingestion_runs_one_running", err)
	}
}

func TestBeginRunWithoutTheLockIsRejected(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	if _, err := s.BeginRun(t.Context()); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("BeginRun without the lock error = %v, want ErrLockNotHeld", err)
	}
	if run := mustLatestRun(t, s); run != nil {
		t.Errorf("LatestRun after a rejected BeginRun = %+v, want nil", run)
	}
}

// Two (here: eight) replicas racing to start: exactly one wins every round.
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
		releases := make([]func(context.Context) error, replicas)
		errs := make([]error, replicas)
		var wg sync.WaitGroup
		for i := range stores {
			wg.Go(func() {
				<-start
				releases[i], errs[i] = stores[i].AcquireIngestLock(t.Context())
			})
		}
		close(start)
		wg.Wait()

		var winner func(context.Context) error
		holders := 0
		for i, err := range errs {
			switch {
			case err == nil:
				holders++
				winner = releases[i]
			case !errors.Is(err, ErrLockHeld):
				t.Fatalf("round %d replica %d: AcquireIngestLock: %v", round, i, err)
			}
		}
		if holders != 1 {
			t.Fatalf("round %d: %d holders, want exactly 1", round, holders)
		}
		if err := winner(t.Context()); err != nil {
			t.Fatalf("round %d: release: %v", round, err)
		}
	}
}

func TestReleaseWithCanceledContextStillReleasesTheLock(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	release := mustAcquire(t, s)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // as on shutdown
	if err := release(ctx); err != nil {
		t.Fatalf("release with a canceled context: %v", err)
	}
	waitFor(t, "the lock to be released", func() bool { return !mustLockHeld(t, s) })
	mustAcquire(t, s)
}

func TestMessagesReferenceTheirRun(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	mustAcquire(t, s)
	run := mustBeginRun(t, s)

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
