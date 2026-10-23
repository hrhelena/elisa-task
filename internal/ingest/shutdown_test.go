package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hrhelena/elisa-task/internal/store"
)

// The walk does not follow a symlinked root, so a symlinked root is rejected
// up front (as POST /fetch does) rather than completing with 0 files.
func TestSymlinkedDatasetRootIsRejected(t *testing.T) {
	t.Parallel()
	target := smallMaildir(t)
	link := filepath.Join(t.TempDir(), "maildir")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	st := newFakeStore()
	s := newTestService(t, NewMaildirSource(link), st)
	_, err := s.Start(t.Context())
	if !errors.Is(err, ErrDatasetMissing) || !errors.Is(err, ErrDatasetInvalid) {
		t.Fatalf("Start = %v, want ErrDatasetInvalid (wrapping ErrDatasetMissing)", err)
	}
	if inv, ok := errors.AsType[*InvalidRootError](err); !ok || !strings.Contains(inv.Problem, "symlink") {
		t.Errorf("Start = %v, want an *InvalidRootError naming the symlink", err)
	}
	if run, _ := st.LatestRun(t.Context()); run != nil {
		t.Errorf("a run was created: %+v", run)
	}
}

// With entries queued, parsed and in an insert when Close runs, the failed
// run's counters still add up.
func TestFilesInFlightAtCloseKeepTheCountersConsistent(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.insertStarted = make(chan struct{}, 1)
	st.insertGate = make(chan struct{})
	s := newTestService(t, &memSource{files: memMessages(200)}, st)
	mustStart(t, s)
	<-st.insertStarted // a batch is in an insert; more entries are queued behind it

	closed := make(chan error, 1)
	go func() { closed <- s.Close(context.Background()) }()
	waitFor(t, "Close to cancel the run", func() bool { return s.life.Err() != nil })
	close(st.insertGate) // the insert now fails with the canceled context
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}

	run, _ := st.LatestRun(t.Context())
	if run.State != store.RunFailed || deref(run.Error) != ErrorShutdown {
		t.Fatalf("run state = %q, error = %q; want failed %q", run.State, deref(run.Error), ErrorShutdown)
	}
	if run.FilesSeen != run.MessagesProcessed+run.MessagesSkipped {
		t.Errorf("files_seen %d != messages_processed %d + messages_skipped %d",
			run.FilesSeen, run.MessagesProcessed, run.MessagesSkipped)
	}
	if run.FilesSeen >= 200 {
		t.Errorf("files_seen = %d; the test needs entries in flight", run.FilesSeen)
	}
}

func TestEverySnapshotKeepsTheCountersConsistent(t *testing.T) {
	t.Parallel()
	c := newCounters()
	c.skipped("empty_file")
	c.stored(3, 2)
	c.skipped("not_an_email")
	snap := c.snapshot()
	if snap.FilesSeen != 5 || snap.FilesSeen != snap.MessagesProcessed+snap.MessagesSkipped {
		t.Errorf("snapshot = %+v", snap)
	}
}

// An insert that never answers (a black-holed database) times out per
// attempt and fails the run after the retries.
func TestAnUnresponsiveDatabaseFailsTheRunAfterTheRetries(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.insertBlocks = true
	cfg := testConfig()
	cfg.InsertTimeout = 20 * time.Millisecond
	s := NewService(&memSource{files: memMessages(4)}, st, cfg, slog.New(slog.DiscardHandler), nil)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	mustStart(t, s)
	run := waitDone(t, s)
	if run.State != store.RunFailed || !strings.Contains(deref(run.Error), "after 4 attempts") ||
		!strings.Contains(deref(run.Error), "deadline exceeded") {
		t.Fatalf("run state = %q, error = %q; want failed after 4 timed-out attempts", run.State, deref(run.Error))
	}
}

// On shutdown a progress write in flight is canceled at once, and the final
// write fits in a close budget of ShutdownWriteTimeout plus a second, however
// long WriteTimeout is.
func TestShutdownFitsTheCloseBudgetWithAWriteInFlight(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.updateBlocks = true // every progress write hangs until canceled
	cfg := testConfig()
	cfg.WriteTimeout = time.Minute
	cfg.ShutdownWriteTimeout = 500 * time.Millisecond
	s := NewService(&memSource{files: memMessages(10), blockAfter: 5}, st, cfg, slog.New(slog.DiscardHandler), nil)
	mustStart(t, s)
	time.Sleep(50 * time.Millisecond) // let a progress write start

	ctx, cancel := context.WithTimeout(t.Context(), cfg.ShutdownWriteTimeout+time.Second)
	defer cancel()
	begin := time.Now()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close = %v after %s", err, time.Since(begin))
	}
	recs := st.finishRecords()
	if len(recs) != 1 || recs[0].err != nil || recs[0].msg != ErrorShutdown {
		t.Errorf("final writes = %+v, want one accepted %q", recs, ErrorShutdown)
	}
}

// Close is not delayed by a Start stuck in the database; that Start is
// canceled and reports ErrClosed.
func TestCloseIsNotDelayedByASlowStart(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.acquireBlocks = true
	s := NewService(&memSource{}, st, testConfig(), slog.New(slog.DiscardHandler), nil)
	started := make(chan error, 1)
	go func() {
		_, err := s.Start(context.Background())
		started <- err
	}()
	waitFor(t, "Start to claim the service", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.done != nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := <-started; !errors.Is(err, ErrClosed) {
		t.Errorf("Start = %v, want ErrClosed", err)
	}
}

// A store closed under a run (for example if the closers ran in the wrong
// order) is a shutdown, not a lost lock, and the lock is not taken again.
func TestStoreClosedMidRunIsRecordedAsShutdownWithoutReacquiring(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	s := newTestService(t, &memSource{files: memMessages(10), blockAfter: 5}, st)
	mustStart(t, s)
	waitFor(t, "the run to get going", func() bool { return st.progressWrites() > 0 })

	st.closeStore()
	waitDone(t, s)
	recs := st.finishRecords()
	if len(recs) != 1 {
		t.Fatalf("final writes = %+v, want exactly one (no second try under a new lock)", recs)
	}
	if recs[0].state != store.RunFailed || recs[0].msg != ErrorShutdown || !errors.Is(recs[0].err, store.ErrStoreClosed) {
		t.Errorf("final write = %+v, want failed %q refused with ErrStoreClosed", recs[0], ErrorShutdown)
	}
	if n := st.acquireCount(); n != 1 {
		t.Errorf("AcquireIngestLock called %d times, want 1 (no re-acquire after store close)", n)
	}
}

func TestLockEndCause(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want error
		lost bool
	}{
		{"released", fmt.Errorf("%w: %w", store.ErrLockNotHeld, store.ErrLockReleased), errLockReleased, false},
		{"store closed", fmt.Errorf("%w: %w", store.ErrLockNotHeld, store.ErrStoreClosed), errShutdown, false},
		{"connection lost", fmt.Errorf("%w: lock connection lost: EOF", store.ErrLockNotHeld), errLockLost, true},
	}
	for _, tt := range tests {
		if got := lockEndCause(tt.err); !errors.Is(got, tt.want) {
			t.Errorf("%s: lockEndCause = %v, want %v", tt.name, got, tt.want)
		}
		if got := lockWasLost(tt.err); got != tt.lost {
			t.Errorf("%s: lockWasLost = %v, want %v", tt.name, got, tt.lost)
		}
	}
}
