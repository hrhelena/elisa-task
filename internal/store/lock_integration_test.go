//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// Tests for the lock handle's behavior under cancellation and connection
// loss (review round 2 of #29).

// requireDone fails unless lock.Done is closed within d and Err wraps
// ErrLockNotHeld.
func requireDone(t *testing.T, lock IngestLock, d time.Duration) {
	t.Helper()
	select {
	case <-lock.Done():
	case <-time.After(d):
		t.Fatalf("lock.Done not closed within %v", d)
	}
	if err := lock.Err(); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("lock.Err() = %v, want ErrLockNotHeld", err)
	}
}

// requireHeld fails unless lock is still held by its own session: Done open,
// Err nil, and another replica cannot take the lock.
func requireHeld(t *testing.T, lock IngestLock, other *Postgres) {
	t.Helper()
	select {
	case <-lock.Done():
		t.Fatalf("lock.Done closed: %v", lock.Err())
	default:
	}
	if err := lock.Err(); err != nil {
		t.Errorf("lock.Err() = %v, want nil", err)
	}
	if _, err := other.AcquireIngestLock(t.Context()); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("another replica's AcquireIngestLock error = %v, want ErrLockHeld", err)
	}
	if !mustLockHeld(t, other) {
		t.Error("IngestLockHeld = false, want true")
	}
}

// lockRow holds a row lock on ingestion_runs row id in a transaction of s
// until the returned function is called, so writes to that row block.
func lockRow(t *testing.T, s *Postgres, id int64) (unlock func()) {
	t.Helper()
	tx, err := s.pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(t.Context(), "SELECT 1 FROM ingestion_runs WHERE id = $1 FOR UPDATE", id); err != nil {
		t.Fatalf("locking run %d: %v", id, err)
	}
	var once sync.Once
	unlock = func() { once.Do(func() { _ = tx.Rollback(context.Background()) }) }
	t.Cleanup(unlock)
	return unlock
}

// A run write whose context times out while its query runs (slow database,
// per-write timeout, SIGTERM) cancels the query on the server and keeps the
// lock, so the run can still be finished (ADR 0010 shutdown path).
func TestTimedOutRunWriteKeepsTheLock(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	lock := mustAcquire(t, a)
	run := mustBeginRun(t, a, lock)

	unlockRow := lockRow(t, b, run.ID) // the progress write blocks on the row
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := a.UpdateRunProgress(ctx, lock, run.ID, Counters{FilesSeen: 5})
	if err == nil || errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("timed-out UpdateRunProgress error = %v, want a cancellation error, not ErrLockNotHeld", err)
	}
	if elapsed := time.Since(start); elapsed >= cancelDeadlineDelay {
		t.Errorf("canceled write took %v: stopped by the socket deadline, not a server-side cancel", elapsed)
	}
	requireHeld(t, lock, b)

	unlockRow()
	msg := "interrupted: shutdown"
	final := Counters{FilesSeen: 7, MessagesProcessed: 7}
	if err := a.FinishRun(t.Context(), lock, run.ID, RunFailed, final, &msg); err != nil {
		t.Fatalf("FinishRun after a timed-out write: %v", err)
	}
	got := storedRun(t, b, run.ID)
	if got.State != RunFailed || got.Error == nil || *got.Error != msg || got.FilesSeen != 7 {
		t.Errorf("stored run = state %q error %v files_seen %d, want failed %q with the final counters",
			got.State, got.Error, got.FilesSeen, msg)
	}
}

// A BeginRun canceled inside its transaction rolls back and keeps the lock.
func TestCanceledBeginRunKeepsTheLock(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)

	// A stale running row (its owner died) whose row lock makes BeginRun's
	// "mark interrupted" update wait.
	var staleID int64
	if err := b.pool.QueryRow(t.Context(),
		"INSERT INTO ingestion_runs (state) VALUES ('running') RETURNING id").Scan(&staleID); err != nil {
		t.Fatalf("inserting a stale run: %v", err)
	}
	unlockRow := lockRow(t, b, staleID)

	lock := mustAcquire(t, a)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, err := a.BeginRun(ctx, lock); err == nil || errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("timed-out BeginRun error = %v, want a cancellation error, not ErrLockNotHeld", err)
	}
	requireHeld(t, lock, b)
	if got := storedRun(t, b, staleID); got.State != RunRunning {
		t.Errorf("stale run after the rolled-back BeginRun = %q, want still running", got.State)
	}

	unlockRow()
	run := mustBeginRun(t, a, lock)
	if got := storedRun(t, b, staleID); got.State != RunFailed || run.ID <= staleID {
		t.Errorf("after BeginRun: stale run %q, new run %d; want the stale run failed and a newer run", got.State, run.ID)
	}
}

// An already canceled context is returned without touching the lock's
// connection (pgx closes a connection whose BEGIN fails that way).
func TestAlreadyCanceledRunWritesKeepTheLock(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	lock := mustAcquire(t, a)
	run := mustBeginRun(t, a, lock)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.BeginRun(ctx, lock); !errors.Is(err, context.Canceled) {
		t.Errorf("BeginRun error = %v, want context.Canceled", err)
	}
	if err := a.UpdateRunProgress(ctx, lock, run.ID, Counters{}); !errors.Is(err, context.Canceled) {
		t.Errorf("UpdateRunProgress error = %v, want context.Canceled", err)
	}
	if err := a.FinishRun(ctx, lock, run.ID, RunCompleted, Counters{}, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("FinishRun error = %v, want context.Canceled", err)
	}
	requireHeld(t, lock, b)
	if err := a.FinishRun(t.Context(), lock, run.ID, RunCompleted, Counters{}, nil); err != nil {
		t.Errorf("FinishRun afterwards: %v", err)
	}
}

// Done is closed promptly when the lock's connection dies, without waiting
// for a run write.
func TestDoneIsClosedWhenTheLockConnectionDies(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	a.lockCheckInterval = 50 * time.Millisecond
	lock := mustAcquire(t, a)

	time.Sleep(200 * time.Millisecond) // several checks pass while it is held
	requireHeld(t, lock, b)

	killLockHolder(t, b)
	requireDone(t, lock, 5*time.Second)
	if err := lock.Err(); err == nil || !strings.Contains(err.Error(), "connection lost") {
		t.Errorf("lock.Err() = %v, want a lost connection", err)
	}
	if err := lock.Release(t.Context()); err != nil {
		t.Errorf("Release of a lost lock: %v", err)
	}
}

func TestDoneIsClosedOnRelease(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	lock := mustAcquire(t, s)
	if err := lock.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	requireDone(t, lock, time.Second)
}
