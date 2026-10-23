package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/hrhelena/elisa-task/internal/mailparse"
	"github.com/hrhelena/elisa-task/internal/store"
)

// testConfig keeps runs fast: small batches, frequent progress writes.
func testConfig() Config {
	return Config{
		Workers:          4,
		BatchSize:        2,
		ProgressInterval: 10 * time.Millisecond,
		FlushInterval:    10 * time.Millisecond,
		RetryBackoff:     time.Millisecond,
		WriteTimeout:     5 * time.Second,
	}
}

func newTestService(t *testing.T, src Source, st Store) *Service {
	t.Helper()
	s := NewService(src, st, testConfig(), slog.New(slog.DiscardHandler), prometheus.NewRegistry())
	t.Cleanup(func() {
		if err := s.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

// waitDone waits until the service has no active run and returns the
// latest run.
func waitDone(t *testing.T, s *Service) *store.Run {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s.mu.Lock()
		done := s.done
		s.mu.Unlock()
		if done == nil {
			break
		}
		select {
		case <-done:
		case <-time.After(time.Until(deadline)):
			t.Fatal("timed out waiting for the run to finish")
		}
	}
	st, err := s.Status(t.Context())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Run == nil {
		t.Fatal("no run recorded")
	}
	return st.Run
}

func mustStart(t *testing.T, s *Service) store.Run {
	t.Helper()
	run, err := s.Start(t.Context())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if run.State != store.RunRunning {
		t.Fatalf("Start returned state %q, want running", run.State)
	}
	return run
}

func requireCompleted(t *testing.T, run *store.Run) {
	t.Helper()
	if run.State != store.RunCompleted || run.Error != nil {
		t.Fatalf("run state = %q, error = %v; want completed", run.State, deref(run.Error))
	}
	if run.FilesSeen != run.MessagesProcessed+run.MessagesSkipped {
		t.Errorf("files_seen %d != messages_processed %d + messages_skipped %d",
			run.FilesSeen, run.MessagesProcessed, run.MessagesSkipped)
	}
}

func requireCounts(t *testing.T, run *store.Run, seen, processed, inserted, skipped int64) {
	t.Helper()
	if run.FilesSeen != seen || run.MessagesProcessed != processed || run.MessagesInserted != inserted ||
		run.MessagesSkipped != skipped {
		t.Errorf("files_seen/processed/inserted/skipped = %d/%d/%d/%d, want %d/%d/%d/%d",
			run.FilesSeen, run.MessagesProcessed, run.MessagesInserted, run.MessagesSkipped,
			seen, processed, inserted, skipped)
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func TestSplitPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		rel, mailbox, folder string
		ok                   bool
	}{
		{"allen-p/_sent_mail/1.", "allen-p", "_sent_mail", true},
		{"allen-p/inbox/deals/2001/3.", "allen-p", "inbox/deals/2001", true},
		{"allen-p/1.", "allen-p", "", true},
		{"README", "", "", false},
		{"allen-p/", "", "", false},
		{"/x", "", "", false},
	}
	for _, tt := range tests {
		mailbox, folder, ok := splitPath(tt.rel)
		if mailbox != tt.mailbox || folder != tt.folder || ok != tt.ok {
			t.Errorf("splitPath(%q) = %q, %q, %v; want %q, %q, %v",
				tt.rel, mailbox, folder, ok, tt.mailbox, tt.folder, tt.ok)
		}
	}
}

func TestToStoreMessageMapsOptionalFields(t *testing.T) {
	t.Parallel()
	m := &mailparse.Message{From: "", Subject: "hi", To: []string{"a@b.c"}}
	got := toStoreMessage("a/b/1.", "a", "b", m, 7)
	if got.FromAddr != nil || got.MessageID != nil || got.SentAt != nil {
		t.Errorf("empty fields not nil: from=%v id=%v sent=%v", got.FromAddr, got.MessageID, got.SentAt)
	}
	if got.Subject == nil || *got.Subject != "hi" || got.RunID != 7 || len(got.ContentSHA256) != 32 {
		t.Errorf("mapped message = %+v", got)
	}
}

func TestIngestASmallMaildir(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	s := newTestService(t, NewMaildirSource(smallMaildir(t)), st)

	started := mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 5, 5, 5, 0)
	if run.ID != started.ID {
		t.Errorf("finished run %d, started %d", run.ID, started.ID)
	}
	if n, _ := st.CountMessages(t.Context()); n != 5 {
		t.Errorf("CountMessages = %d, want 5", n)
	}
	m, ok := st.message("allen-p/_sent_mail/1.")
	if !ok {
		t.Fatal("allen-p/_sent_mail/1. not stored")
	}
	if m.Mailbox != "allen-p" || m.Folder != "_sent_mail" || m.RunID != run.ID {
		t.Errorf("row mailbox/folder/run = %q/%q/%d", m.Mailbox, m.Folder, m.RunID)
	}
	if m.FromAddr == nil || *m.FromAddr != "phillip.allen@enron.com" || m.SentAt == nil {
		t.Errorf("row from/sent_at = %v/%v", deref(m.FromAddr), m.SentAt)
	}
	if st.heldLock() != nil {
		t.Error("ingest lock still held after the run")
	}
}

func TestMalformedEntriesAreSkippedOrFlaggedNeverFatal(t *testing.T) {
	t.Parallel()
	s := newTestService(t, NewMaildirSource(malformedMaildir(t)), newFakeStore())
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 8, 6, 6, 2)
	if want := map[string]int64{"empty_file": 1, "not_an_email": 1}; !maps.Equal(run.SkipReasons, want) {
		t.Errorf("skip_reasons = %v, want %v", run.SkipReasons, want)
	}
	if run.FieldErrors["invalid_sender"] != 1 || run.FieldErrors["invalid_date"] != 1 {
		t.Errorf("field_errors = %v, want invalid_sender 1 and invalid_date 1", run.FieldErrors)
	}
}

func TestSymlinksAndStrayRootFilesAreSkipped(t *testing.T) {
	t.Parallel()
	root := smallMaildir(t)
	if err := os.Symlink(filepath.Join(root, "allen-p/inbox/1."), filepath.Join(root, "allen-p/inbox/link.")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "README", "not a mailbox\n")
	s := newTestService(t, NewMaildirSource(root), newFakeStore())
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 7, 5, 5, 2)
	if run.SkipReasons[SkipNotRegularFile] != 1 || run.SkipReasons[SkipOutsideMailbox] != 1 {
		t.Errorf("skip_reasons = %v, want not_regular_file 1 and outside_mailbox 1", run.SkipReasons)
	}
}

func TestUnreadableDirectoryIsSkippedAndTheWalkContinues(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs POSIX permissions and a non-root user")
	}
	root := smallMaildir(t)
	writeFile(t, root, "allen-p/secret/1.", message(9, "a@b.c"))
	secret := filepath.Join(root, "allen-p/secret")
	if err := os.Chmod(secret, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(secret, 0o750) }) //nolint:gosec // restore so TempDir cleanup works

	s := newTestService(t, NewMaildirSource(root), newFakeStore())
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 6, 5, 5, 1)
	if run.SkipReasons["read_error"] != 1 {
		t.Errorf("skip_reasons = %v, want read_error 1", run.SkipReasons)
	}
}

func TestDatasetMissing(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	s := newTestService(t, NewMaildirSource(filepath.Join(t.TempDir(), "maildir")), st)
	_, err := s.Start(t.Context())
	if !errors.Is(err, ErrDatasetMissing) {
		t.Fatalf("Start error = %v, want ErrDatasetMissing", err)
	}
	if run, _ := st.LatestRun(t.Context()); run != nil {
		t.Errorf("a run was created: %+v", run)
	}
	if st.heldLock() != nil {
		t.Error("lock held after a failed Start")
	}
}

func TestDatasetRootRemovedDuringTheRunFailsIt(t *testing.T) {
	t.Parallel()
	root := smallMaildir(t)
	src := &removingSource{Source: NewMaildirSource(root), root: root}
	s := newTestService(t, src, newFakeStore())
	mustStart(t, s)
	run := waitDone(t, s)
	if run.State != store.RunFailed || !strings.Contains(deref(run.Error), "dataset not found") {
		t.Fatalf("run state = %q, error = %q; want failed with dataset not found", run.State, deref(run.Error))
	}
}

// removingSource deletes the dataset root after the first entry.
type removingSource struct {
	Source
	root string
}

func (s *removingSource) Walk(ctx context.Context, fn func(string, OpenFunc) error) error {
	first := true
	return s.Source.Walk(ctx, func(rel string, open OpenFunc) error {
		if first {
			first = false
			if err := os.RemoveAll(s.root); err != nil {
				return err
			}
		}
		return fn(rel, open)
	})
}

func TestConcurrentStartIsSingleFlight(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	src := &memSource{files: memMessages(4), blockAfter: 2}
	a := newTestService(t, src, st)
	b := newTestService(t, src, st)

	errs := make(chan error, 2)
	for _, s := range []*Service{a, b} {
		go func() {
			_, err := s.Start(t.Context())
			errs <- err
		}()
	}
	var ok, busy int
	for range 2 {
		switch err := <-errs; {
		case err == nil:
			ok++
		case errors.Is(err, ErrAlreadyRunning):
			busy++
		default:
			t.Errorf("Start: %v", err)
		}
	}
	if ok != 1 || busy != 1 {
		t.Errorf("started %d, already running %d; want 1 and 1", ok, busy)
	}
	// A second Start on the same service is refused too.
	if _, err := a.Start(t.Context()); !errors.Is(err, ErrAlreadyRunning) {
		t.Errorf("second Start = %v, want ErrAlreadyRunning", err)
	}
}

func TestReRunIsIdempotent(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	s := newTestService(t, NewMaildirSource(smallMaildir(t)), st)
	mustStart(t, s)
	requireCompleted(t, waitDone(t, s))

	second := mustStart(t, s)
	run := waitDone(t, s)
	if run.ID != second.ID || run.ID == 1 {
		t.Fatalf("latest run %d, want the second run %d", run.ID, second.ID)
	}
	requireCompleted(t, run)
	requireCounts(t, run, 5, 5, 0, 0)
	if n, _ := st.CountMessages(t.Context()); n != 5 {
		t.Errorf("CountMessages = %d, want 5", n)
	}
}

func TestReRunAfterAFailedRunResumesWithoutDoubleCounting(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	// A first run stored 3 of the 5 files, then failed.
	for _, rel := range smallFiles[:3] {
		st.msgs[rel] = store.Message{SourcePath: rel}
	}
	lock, _ := st.AcquireIngestLock(t.Context())
	failed, _ := st.BeginRun(t.Context(), lock)
	msg := "batch insert failed"
	if err := st.FinishRun(t.Context(), lock, failed.ID, store.RunFailed, store.Counters{}, &msg); err != nil {
		t.Fatal(err)
	}
	_ = lock.Release(t.Context())

	s := newTestService(t, NewMaildirSource(smallMaildir(t)), st)
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 5, 5, 2, 0)
	if n, _ := st.CountMessages(t.Context()); n != 5 {
		t.Errorf("CountMessages = %d, want 5", n)
	}
}

func TestShutdownDuringARun(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	s := newTestService(t, &memSource{files: memMessages(10), blockAfter: 5}, st)
	mustStart(t, s)
	// Wait until the first 5 files are stored and a progress write saw them.
	waitFor(t, "progress to be persisted", func() bool {
		run, _ := st.LatestRun(t.Context())
		return run != nil && run.MessagesProcessed == 5
	})

	if err := s.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	run, _ := st.LatestRun(t.Context())
	if run.State != store.RunFailed || deref(run.Error) != ErrorShutdown {
		t.Fatalf("run state = %q, error = %q; want failed %q", run.State, deref(run.Error), ErrorShutdown)
	}
	if run.FilesSeen != 5 || run.MessagesProcessed != 5 {
		t.Errorf("files_seen/processed = %d/%d, want 5/5", run.FilesSeen, run.MessagesProcessed)
	}
	if st.heldLock() != nil {
		t.Error("ingest lock still held after Close")
	}
	if _, err := s.Start(t.Context()); !errors.Is(err, ErrClosed) {
		t.Errorf("Start after Close = %v, want ErrClosed", err)
	}
}

func TestCloseWithoutARunReturnsAtOnce(t *testing.T) {
	t.Parallel()
	s := newTestService(t, &memSource{}, newFakeStore())
	if err := s.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCloseIsBoundedByItsContext(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.insertStarted = make(chan struct{}, 1)
	st.insertGate = make(chan struct{}) // a stuck insert that ignores its context
	s := NewService(&memSource{files: memMessages(4)}, st, testConfig(), slog.New(slog.DiscardHandler), nil)
	mustStart(t, s)
	<-st.insertStarted

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close with a stuck insert = %v, want DeadlineExceeded", err)
	}
	close(st.insertGate)
	if err := s.Close(t.Context()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if run, _ := st.LatestRun(t.Context()); run.State != store.RunFailed || deref(run.Error) != ErrorShutdown {
		t.Errorf("run state = %q, error = %q; want failed %q", run.State, deref(run.Error), ErrorShutdown)
	}
}

func TestCrashedOwnerIsDetected(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	lock, _ := st.AcquireIngestLock(t.Context())
	if _, err := st.BeginRun(t.Context(), lock); err != nil {
		t.Fatal(err)
	}
	lock.(*fakeLock).lose() // the owner died without finishing the run

	s := newTestService(t, &memSource{}, st)
	status, err := s.Status(t.Context())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != StateFailed || deref(status.Run.Error) != store.ErrorInterrupted {
		t.Errorf("state = %q, error = %q; want failed interrupted", status.State, deref(status.Run.Error))
	}
}

func TestStatusIsIdleBeforeTheFirstRun(t *testing.T) {
	t.Parallel()
	s := newTestService(t, &memSource{}, newFakeStore())
	status, err := s.Status(t.Context())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != StateIdle || status.Run != nil || status.TotalMessages != 0 {
		t.Errorf("status = %+v, want idle", status)
	}
}

func TestDatabaseOutageDuringARunFailsItAfterRetries(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.insertFn = func(int) error { return errors.New("dial tcp: connection refused") }
	s := newTestService(t, &memSource{files: memMessages(4)}, st)
	mustStart(t, s)
	run := waitDone(t, s)
	if run.State != store.RunFailed || !strings.Contains(deref(run.Error), "batch insert") {
		t.Fatalf("run state = %q, error = %q; want failed mentioning the batch insert", run.State, deref(run.Error))
	}
	if !strings.Contains(deref(run.Error), "after 4 attempts") {
		t.Errorf("error %q does not report 1 attempt + 3 retries", deref(run.Error))
	}
	// The service is still usable.
	if _, err := s.Status(t.Context()); err != nil {
		t.Errorf("Status after the failure: %v", err)
	}
}

func TestATransientInsertErrorIsRetried(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.insertFn = func(call int) error {
		if call <= 2 {
			return errors.New("connection reset")
		}
		return nil
	}
	s := newTestService(t, &memSource{files: memMessages(4)}, st)
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 4, 4, 4, 0)
}

func TestAParserPanicIsSkippedAndTheRunContinues(t *testing.T) {
	t.Parallel()
	files := memMessages(4)
	files[1].content = "PANIC\n" + files[1].content
	s := newTestService(t, &memSource{files: files}, newFakeStore())
	s.parse = func(r io.Reader) (*mailparse.Message, error) {
		b, _ := io.ReadAll(r)
		if strings.HasPrefix(string(b), "PANIC") {
			panic("boom")
		}
		return mailparse.Parse(strings.NewReader(string(b)))
	}
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 4, 3, 3, 1)
	if run.SkipReasons[SkipParsePanic] != 1 {
		t.Errorf("skip_reasons = %v, want parse_panic 1", run.SkipReasons)
	}
}

func TestFieldErrorsCountMessagesNotHeaders(t *testing.T) {
	t.Parallel()
	bad := strings.Replace(message(1, "a@b.c"), "To: tim.belden@enron.com", "To: <@>\nCc: <@>", 1)
	s := newTestService(t, &memSource{files: []memFile{{rel: "a/inbox/1.", content: bad}}}, newFakeStore())
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	if run.FieldErrors["invalid_recipient"] != 1 {
		t.Errorf("field_errors = %v, want invalid_recipient 1 for one message with bad To and Cc", run.FieldErrors)
	}
}

func TestOpenErrorsAreSkipped(t *testing.T) {
	t.Parallel()
	files := []memFile{
		{rel: "a/inbox/1.", openErr: os.ErrPermission},
		{rel: "a/inbox/2.", openErr: ErrNotRegularFile},
		{rel: "a/inbox/3.", content: message(3, "a@b.c")},
	}
	s := newTestService(t, &memSource{files: files}, newFakeStore())
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	if want := map[string]int64{"read_error": 1, "not_regular_file": 1}; !maps.Equal(run.SkipReasons, want) {
		t.Errorf("skip_reasons = %v, want %v", run.SkipReasons, want)
	}
}

func TestLockLossStopsTheRunAndRecordsLockLost(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	s := newTestService(t, &memSource{files: memMessages(10), blockAfter: 5}, st)
	mustStart(t, s)
	waitFor(t, "the run to get going", func() bool { return st.progressWrites() > 0 })

	st.heldLock().lose()
	run := waitDone(t, s)
	if run.State != store.RunFailed || deref(run.Error) != ErrorLockLost {
		t.Fatalf("run state = %q, error = %q; want failed %q", run.State, deref(run.Error), ErrorLockLost)
	}
	if st.heldLock() != nil {
		t.Error("the lock taken again to record the outcome was not released")
	}
}

func TestLostLockHeldByAnotherReplicaLeavesTheRunToIt(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	s := newTestService(t, &memSource{files: memMessages(10), blockAfter: 5}, st)
	run := mustStart(t, s)
	waitFor(t, "the run to get going", func() bool { return st.progressWrites() > 0 })

	// The lock's session dies and another replica takes the freed lock before
	// this one notices. This must be one atomic step: with lose followed by
	// AcquireIngestLock, the engine can see the loss in between, take the
	// free lock back and record lock_lost itself (correct, and covered by
	// TestLockLossStopsTheRunAndRecordsLockLost), which made this test flaky
	// (#44).
	other := st.loseToAnotherHolder()
	defer func() { _ = other.Release(context.Background()) }()
	waitDone(t, s)

	st.mu.Lock()
	state := st.runs[run.ID-1].State
	st.mu.Unlock()
	if state != store.RunRunning {
		t.Errorf("run state = %q; the engine must not write without the lock", state)
	}
	if st.heldLock() != other {
		t.Error("the other replica's lock was released or replaced; the engine must leave it alone")
	}
	if n := st.acquireCount(); n != 2 { // this run's, then the other replica's
		t.Errorf("successful lock acquisitions = %d, want 2; the engine must not take the lock back", n)
	}
	// The engine did try to record the outcome, and was refused.
	finishes := st.finishRecords()
	if len(finishes) == 0 {
		t.Fatal("the engine never tried to record the outcome")
	}
	for _, f := range finishes {
		if !errors.Is(f.err, store.ErrLockNotHeld) {
			t.Errorf("FinishRun(%s, %q) = %v; want an error wrapping ErrLockNotHeld", f.state, f.msg, f.err)
		}
	}
}

func TestRunNoLongerRunningStopsTheRun(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	s := newTestService(t, &memSource{files: memMessages(10), blockAfter: 5}, st)
	run := mustStart(t, s)
	waitFor(t, "the run to get going", func() bool { return st.progressWrites() > 0 })

	st.failRun(run.ID)
	waitDone(t, s) // returns only once the pipeline stopped
	if st.heldLock() != nil {
		t.Error("lock still held")
	}
}

func TestMetricsCountFilesMessagesAndSkips(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	st := newFakeStore()
	s := NewService(NewMaildirSource(malformedMaildir(t)), st, testConfig(), slog.New(slog.DiscardHandler), reg)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	mustStart(t, s)
	waitDone(t, s)

	for _, tt := range []struct {
		name string
		c    prometheus.Collector
		want float64
	}{
		{"files", s.m.files, 8},
		{"messages", s.m.messages, 6},
		{"skipped empty_file", s.m.skipped.WithLabelValues("empty_file"), 1},
		{"field error invalid_sender", s.m.fieldErrors.WithLabelValues("invalid_sender"), 1},
		{"running", s.m.running, 0},
	} {
		if got := testutil.ToFloat64(tt.c); got != tt.want {
			t.Errorf("%s = %v, want %v", tt.name, got, tt.want)
		}
	}
	if n, err := testutil.GatherAndCount(reg, "enron_ingest_batch_duration_seconds"); err != nil || n != 1 {
		t.Errorf("batch duration histogram count = %d, %v", n, err)
	}
	// A second service on the same registry reuses the collectors.
	_ = NewService(&memSource{}, st, testConfig(), slog.New(slog.DiscardHandler), reg)
}

func TestSamplerLogsTheFirstHundredThenOneInAThousand(t *testing.T) {
	t.Parallel()
	s := newSampler()
	logged := 0
	for range 3000 {
		if s.allow("skip:empty_file") {
			logged++
		}
	}
	if logged != 103 { // events 1..100, 1000, 2000 and 3000
		t.Errorf("logged %d of 3000, want 103", logged)
	}
}

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	c := Config{}.withDefaults()
	if c.Workers != 4 || c.BatchSize != 2000 || c.ProgressInterval != 5*time.Second || c.MaxRetries != 3 ||
		c.FlushInterval != time.Second || c.ProgressLogInterval != 30*time.Second {
		t.Errorf("defaults = %+v", c)
	}
}

// waitFor polls cond until it is true or a deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
