package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"testing"
	"time"

	"github.com/hrhelena/elisa-task/internal/httpapi"
	"github.com/hrhelena/elisa-task/internal/ingest"
	"github.com/hrhelena/elisa-task/internal/store"
)

type fakeIngestService struct {
	run       store.Run
	startErr  error
	status    ingest.Status
	statusErr error
	starts    int
}

func (f *fakeIngestService) Start(context.Context) (store.Run, error) {
	f.starts++
	return f.run, f.startErr
}

func (f *fakeIngestService) Status(context.Context) (ingest.Status, error) {
	return f.status, f.statusErr
}

type fakeLockChecker struct {
	held bool
	err  error
}

func (f fakeLockChecker) IngestLockHeld(context.Context) (bool, error) { return f.held, f.err }

func newTestIngestion(svc ingestService, lock lockChecker) ingestion {
	return ingestion{svc: svc, lock: lock, maildir: "/data/maildir", logger: slog.New(slog.DiscardHandler)}
}

func TestIngestionStart(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	missing := fmt.Errorf("%w: lstat /data/maildir: no such file or directory", ingest.ErrDatasetMissing)
	tests := []struct {
		name       string
		lock       fakeLockChecker
		startErr   error
		wantStarts int
		wantRun    httpapi.IngestionRun
		// check validates the returned error; nil means no error expected.
		check func(t *testing.T, err error)
	}{
		{
			name:       "started",
			wantStarts: 1,
			wantRun:    httpapi.IngestionRun{ID: 4, State: "running", StartedAt: started},
		},
		{
			// The pool check answers without dialing a lock connection (#29).
			name:       "lock held elsewhere: no start attempt",
			lock:       fakeLockChecker{held: true},
			wantStarts: 0,
			check:      wantIs(httpapi.ErrIngestionRunning),
		},
		{
			// The check failed: the engine still decides (and reports the
			// database error itself).
			name:       "lock check fails: start is still attempted",
			lock:       fakeLockChecker{err: errors.New("pool closed")},
			wantStarts: 1,
			wantRun:    httpapi.IngestionRun{ID: 4, State: "running", StartedAt: started},
		},
		{
			// The check races with another replica taking the lock.
			name:       "lock taken after the check",
			startErr:   ingest.ErrAlreadyRunning,
			wantStarts: 1,
			check:      wantIs(httpapi.ErrIngestionRunning),
		},
		{
			name:       "dataset missing",
			startErr:   missing,
			wantStarts: 1,
			check: func(t *testing.T, err error) {
				t.Helper()
				var dm *httpapi.DatasetMissingError
				if !errors.As(err, &dm) || dm.Path != "/data/maildir" || !errors.Is(err, ingest.ErrDatasetMissing) {
					t.Errorf("err = %v, want a DatasetMissingError for /data/maildir wrapping ErrDatasetMissing", err)
				}
			},
		},
		{
			name:       "closed",
			startErr:   ingest.ErrClosed,
			wantStarts: 1,
			check:      wantIs(httpapi.ErrIngesterClosed),
		},
		{
			// #7 review: Start joins ErrClosed with the error it failed with;
			// shutdown wins, so the handler answers 503, never 409.
			name:       "closed joined with dataset missing",
			startErr:   errors.Join(ingest.ErrClosed, missing),
			wantStarts: 1,
			check: func(t *testing.T, err error) {
				t.Helper()
				var dm *httpapi.DatasetMissingError
				if !errors.Is(err, httpapi.ErrIngesterClosed) || errors.As(err, &dm) {
					t.Errorf("err = %v, want ErrIngesterClosed and no DatasetMissingError", err)
				}
			},
		},
		{
			name:       "closed joined with already running",
			startErr:   errors.Join(ingest.ErrClosed, ingest.ErrAlreadyRunning),
			wantStarts: 1,
			check:      wantIs(httpapi.ErrIngesterClosed),
		},
		{
			name:       "database error passes through",
			startErr:   fmt.Errorf("acquiring ingest lock: %w", context.DeadlineExceeded),
			wantStarts: 1,
			check: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, httpapi.ErrIngestionRunning) ||
					errors.Is(err, httpapi.ErrIngesterClosed) {
					t.Errorf("err = %v, want the unmapped database error", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &fakeIngestService{
				run:      store.Run{ID: 4, State: store.RunRunning, StartedAt: started},
				startErr: tt.startErr,
			}
			run, err := newTestIngestion(svc, tt.lock).Start(t.Context())
			if svc.starts != tt.wantStarts {
				t.Errorf("engine Start calls = %d, want %d", svc.starts, tt.wantStarts)
			}
			if tt.check == nil {
				if err != nil || run != tt.wantRun {
					t.Errorf("Start = %+v, %v; want %+v, nil", run, err, tt.wantRun)
				}
				return
			}
			tt.check(t, err)
		})
	}
}

func wantIs(target error) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, target) {
			t.Errorf("err = %v, want %v", err, target)
		}
	}
}

func TestIngestionStatus(t *testing.T) {
	t.Parallel()

	t.Run("idle", func(t *testing.T) {
		t.Parallel()
		svc := &fakeIngestService{status: ingest.Status{State: ingest.StateIdle, TotalMessages: 7}}
		got, err := newTestIngestion(svc, fakeLockChecker{}).Status(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if got.State != httpapi.StateIdle || got.RunID != nil || got.StartedAt != nil || got.UpdatedAt != nil ||
			got.TotalMessages != 7 || got.SkipReasons == nil || got.FieldErrors == nil {
			t.Errorf("Status = %+v, want idle, no run fields, total 7, empty maps", got)
		}
	})

	t.Run("run", func(t *testing.T) {
		t.Parallel()
		start := time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
		end := start.Add(time.Minute)
		msg := "interrupted: shutdown"
		run := &store.Run{
			ID: 3, State: store.RunFailed, StartedAt: start, FinishedAt: &end, UpdatedAt: end, Error: &msg,
			Counters: store.Counters{
				FilesSeen: 8, MessagesProcessed: 6, MessagesInserted: 6, MessagesSkipped: 2,
				SkipReasons: map[string]int64{"empty_file": 1, "not_an_email": 1},
				FieldErrors: map[string]int64{"invalid_sender": 1},
			},
		}
		svc := &fakeIngestService{status: ingest.Status{State: ingest.StateFailed, Run: run, TotalMessages: 6}}
		got, err := newTestIngestion(svc, fakeLockChecker{}).Status(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if got.State != "failed" || *got.RunID != 3 || !got.StartedAt.Equal(start) || !got.FinishedAt.Equal(end) ||
			!got.UpdatedAt.Equal(end) || *got.Error != msg || got.FilesSeen != 8 || got.MessagesProcessed != 6 ||
			got.MessagesSkipped != 2 || got.TotalMessages != 6 ||
			!maps.Equal(got.SkipReasons, run.SkipReasons) || !maps.Equal(got.FieldErrors, run.FieldErrors) {
			t.Errorf("Status = %+v, want the run's fields", got)
		}
	})

	t.Run("nil maps from the store are empty", func(t *testing.T) {
		t.Parallel()
		svc := &fakeIngestService{status: ingest.Status{State: ingest.StateRunning, Run: &store.Run{ID: 1, State: store.RunRunning}}}
		got, err := newTestIngestion(svc, fakeLockChecker{}).Status(t.Context())
		if err != nil || got.SkipReasons == nil || got.FieldErrors == nil {
			t.Errorf("Status = %+v, %v; want empty non-nil maps", got, err)
		}
	})

	t.Run("errors pass through", func(t *testing.T) {
		t.Parallel()
		svc := &fakeIngestService{statusErr: context.DeadlineExceeded}
		if _, err := newTestIngestion(svc, fakeLockChecker{}).Status(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want context.DeadlineExceeded", err)
		}
	})
}
