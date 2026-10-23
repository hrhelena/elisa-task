package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hrhelena/elisa-task/internal/store"
)

// recordHandler is a slog.Handler that keeps every record, for assertions
// on log levels.
type recordHandler struct {
	mu   *sync.Mutex
	recs *[]slog.Record
}

func newRecordHandler() recordHandler {
	return recordHandler{mu: &sync.Mutex{}, recs: &[]slog.Record{}}
}

func (recordHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, r.Clone())
	return nil
}

// WithAttrs and WithGroup drop the attributes: the tests only look at
// levels, messages and record attributes.
func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordHandler) WithGroup(string) slog.Handler      { return h }

func (h recordHandler) records() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), *h.recs...)
}

// find returns the records at level whose message contains sub.
func (h recordHandler) find(level slog.Level, sub string) []slog.Record {
	var out []slog.Record
	for _, r := range h.records() {
		if r.Level == level && strings.Contains(r.Message, sub) {
			out = append(out, r)
		}
	}
	return out
}

func (h recordHandler) dump() string {
	var b strings.Builder
	for _, r := range h.records() {
		fmt.Fprintf(&b, "%s %s", r.Level, r.Message)
		r.Attrs(func(a slog.Attr) bool {
			fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
			return true
		})
		b.WriteByte('\n')
	}
	return b.String()
}

// Scenario: Normal release logs no error (#37). A run is shut down while a
// progress write hangs on a slow database past the final-write deadline;
// only the lock release (which closes the lock's connection) ends that
// write. No ERROR about the ingest lock is logged: the write's end is
// logged at DEBUG with the reason.
func TestNormalReleaseLogsNoError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		endErr error // what the hanging write returns once the lock ends; nil: the lock's Err
	}{
		{"write ends with the released lock's error", nil},
		{"write ends with a connection error", errors.New("conn closed")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			st := newFakeStore()
			st.updateUntilLockEnds = true
			st.updateEndErr = tt.endErr
			st.updateStarted = make(chan struct{}, 1)
			cfg := testConfig()
			cfg.ShutdownWriteTimeout = 100 * time.Millisecond
			logs := newRecordHandler()
			s := NewService(&memSource{files: memMessages(10), blockAfter: 5}, st, cfg, slog.New(logs), nil)
			mustStart(t, s)
			<-st.updateStarted // Given a slow progress write is in flight

			ctx, cancel := context.WithTimeout(t.Context(), cfg.ShutdownWriteTimeout+time.Second)
			defer cancel()
			if err := s.Close(ctx); err != nil { // When the run shuts down and releases the lock
				t.Fatalf("Close: %v", err)
			}

			// Then no ERROR-level "ingest lock ..." is logged.
			if errs := logs.find(slog.LevelError, "ingest lock"); len(errs) != 0 {
				t.Errorf("ERROR about the ingest lock after a normal release:\n%s", logs.dump())
			}
			if tt.endErr == nil && len(logs.find(slog.LevelDebug, "ingest lock ended after the run stopped")) != 1 {
				t.Errorf("want one DEBUG \"ingest lock ended after the run stopped\":\n%s", logs.dump())
			}
			if n := len(st.releaseRecords()); n != 1 {
				t.Errorf("Release called %d times, want 1", n)
			}
		})
	}
}

func TestLogLockEndLevels(t *testing.T) {
	t.Parallel()
	released := fmt.Errorf("%w: %w", store.ErrLockNotHeld, store.ErrLockReleased)
	closed := fmt.Errorf("%w: %w", store.ErrLockNotHeld, store.ErrStoreClosed)
	lost := fmt.Errorf("%w: lock connection lost: EOF", store.ErrLockNotHeld)
	tests := []struct {
		name    string
		stopped bool
		err     error
		want    slog.Level
	}{
		{"released after the run stopped", true, released, slog.LevelDebug},
		{"store closed after the run stopped", true, closed, slog.LevelDebug},
		{"lost after the run stopped", true, lost, slog.LevelError},
		{"released while running", false, released, slog.LevelError},
		{"store closed while running", false, closed, slog.LevelError},
		{"lost while running", false, lost, slog.LevelError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			logs := newRecordHandler()
			logLockEnd(slog.New(logs), tt.stopped, "ingest lock ended, stopping the run", tt.err)
			recs := logs.records()
			if len(recs) != 1 || recs[0].Level != tt.want {
				t.Fatalf("logged:\n%s\nwant one record at %s", logs.dump(), tt.want)
			}
			var hasErr bool
			recs[0].Attrs(func(a slog.Attr) bool {
				hasErr = hasErr || strings.Contains(a.Value.String(), tt.err.Error())
				return true
			})
			if !hasErr {
				t.Errorf("record does not carry the reason %q:\n%s", tt.err, logs.dump())
			}
		})
	}
}

func TestRunStopped(t *testing.T) {
	t.Parallel()
	open := make(chan struct{})
	closedCh := make(chan struct{})
	close(closedCh)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if runStopped(t.Context(), open) {
		t.Error("runStopped = true while running")
	}
	if !runStopped(t.Context(), closedCh) {
		t.Error("runStopped = false after stop")
	}
	if !runStopped(canceled, open) {
		t.Error("runStopped = false with a canceled run context")
	}
}

// When the lock was taken but BeginRun fails because Close was called, the
// lock is released within ShutdownWriteTimeout, not WriteTimeout (#37), so
// Close fits in its budget even if the release is slow.
func TestStartFailureDuringShutdownReleasesWithTheShutdownBudget(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.beginBlocks = true
	st.beginStarted = make(chan struct{}, 1)
	st.releaseBlocks = true // the release takes its whole budget
	cfg := testConfig()
	cfg.WriteTimeout = time.Minute
	cfg.ShutdownWriteTimeout = 200 * time.Millisecond
	s := NewService(&memSource{}, st, cfg, slog.New(slog.DiscardHandler), nil)

	started := make(chan error, 1)
	go func() {
		_, err := s.Start(context.Background())
		started <- err
	}()
	<-st.beginStarted // the lock is held and BeginRun is in flight

	ctx, cancel := context.WithTimeout(t.Context(), cfg.ShutdownWriteTimeout+time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close = %v; the failed start's release did not fit the shutdown budget", err)
	}
	if err := <-started; !errors.Is(err, ErrClosed) {
		t.Errorf("Start = %v, want ErrClosed", err)
	}
	budgets := st.releaseRecords()
	if len(budgets) != 1 || budgets[0] < 0 || budgets[0] > cfg.ShutdownWriteTimeout {
		t.Errorf("release budgets = %v, want one of at most %s", budgets, cfg.ShutdownWriteTimeout)
	}
}

// Without shutdown, a failed start still releases with WriteTimeout.
func TestStartFailureReleasesWithTheWriteTimeout(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.beginBlocks = true
	cfg := testConfig()
	cfg.WriteTimeout = time.Minute
	cfg.ShutdownWriteTimeout = 200 * time.Millisecond
	s := NewService(&memSource{}, st, cfg, slog.New(slog.DiscardHandler), nil)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Start(ctx); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrClosed) {
		t.Fatalf("Start = %v, want a deadline error, not ErrClosed", err)
	}
	budgets := st.releaseRecords()
	if len(budgets) != 1 || budgets[0] <= cfg.ShutdownWriteTimeout {
		t.Errorf("release budgets = %v, want one of about %s", budgets, cfg.WriteTimeout)
	}
}
