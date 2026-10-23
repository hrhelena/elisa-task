package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
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
	transientErr := fmt.Errorf("%w: %w", ingest.ErrTransient,
		&fs.PathError{Op: "open", Path: "/data/maildir", Err: syscall.EMFILE})
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
				if dm != nil && dm.Problem != "" {
					t.Errorf("Problem = %q, want empty for a maildir that does not exist", dm.Problem)
				}
			},
		},
		{
			// #45: an existing but unusable maildir carries the problem, so
			// the handler does not send the caller to POST /fetch.
			name: "dataset invalid",
			startErr: fmt.Errorf("checking the dataset: %w",
				&ingest.InvalidRootError{Path: "/data/maildir", Problem: "it is a symlink, which is not followed"}),
			wantStarts: 1,
			check: func(t *testing.T, err error) {
				t.Helper()
				var dm *httpapi.DatasetMissingError
				if !errors.As(err, &dm) || dm.Path != "/data/maildir" || dm.Problem != "it is a symlink, which is not followed" ||
					!errors.Is(err, ingest.ErrDatasetInvalid) {
					t.Errorf("err = %v, want a DatasetMissingError with the symlink problem wrapping ErrDatasetInvalid", err)
				}
			},
		},
		{
			// Scenario: Running out of file descriptors is transient (#49):
			// a 503 to retry, never the 409 dataset_missing.
			name:       "dataset check fails with EMFILE",
			startErr:   transientErr,
			wantStarts: 1,
			check: func(t *testing.T, err error) {
				t.Helper()
				var dm *httpapi.DatasetMissingError
				if !errors.Is(err, httpapi.ErrDatasetUnavailable) || !errors.Is(err, syscall.EMFILE) || errors.As(err, &dm) {
					t.Errorf("err = %v, want ErrDatasetUnavailable wrapping EMFILE and no DatasetMissingError", err)
				}
			},
		},
		{
			// Shutdown still wins over a transient error.
			name:       "closed joined with a transient error",
			startErr:   errors.Join(ingest.ErrClosed, transientErr),
			wantStarts: 1,
			check: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, httpapi.ErrIngesterClosed) {
					t.Errorf("err = %v, want ErrIngesterClosed", err)
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
		// Idle keeps empty maps, as in the ticket's idle response.
		if got.State != httpapi.StateIdle || got.RunID != nil || got.StartedAt != nil || got.UpdatedAt != nil ||
			got.TotalMessages != 7 || got.SkipReasons == nil || got.FieldErrors == nil ||
			len(got.SkipReasons) != 0 || len(got.FieldErrors) != 0 {
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
			got.MessagesSkipped != 2 || got.TotalMessages != 6 {
			t.Errorf("Status = %+v, want the run's fields", got)
		}
		// Every known key is listed, with 0 for those that did not occur.
		wantSkips := map[string]int64{
			"empty_file": 1, "not_an_email": 1, "header_too_large": 0, "read_error": 0,
			"outside_mailbox": 0, "not_regular_file": 0, "parse_panic": 0, "parse_error": 0,
		}
		wantFields := map[string]int64{
			"invalid_sender": 1, "missing_sender": 0, "invalid_date": 0, "missing_date": 0,
			"invalid_encoding": 0, "invalid_recipient": 0,
		}
		if !maps.Equal(got.SkipReasons, wantSkips) || !maps.Equal(got.FieldErrors, wantFields) {
			t.Errorf("skip_reasons = %v, field_errors = %v; want %v, %v", got.SkipReasons, got.FieldErrors, wantSkips, wantFields)
		}
		// The run's own maps are not modified.
		if len(run.SkipReasons) != 2 || len(run.FieldErrors) != 1 {
			t.Errorf("the store's maps were modified: %v, %v", run.SkipReasons, run.FieldErrors)
		}
	})

	t.Run("unknown keys from the engine are kept", func(t *testing.T) {
		t.Parallel()
		run := &store.Run{ID: 1, State: store.RunRunning, Counters: store.Counters{
			SkipReasons: map[string]int64{"future_reason": 2}, FieldErrors: map[string]int64{"future_issue": 3},
		}}
		svc := &fakeIngestService{status: ingest.Status{State: ingest.StateRunning, Run: run}}
		got, err := newTestIngestion(svc, fakeLockChecker{}).Status(t.Context())
		if err != nil || got.SkipReasons["future_reason"] != 2 || got.FieldErrors["future_issue"] != 3 {
			t.Errorf("Status = %+v, %v; want the unknown keys kept", got, err)
		}
	})

	t.Run("nil maps from the store are pre-filled", func(t *testing.T) {
		t.Parallel()
		svc := &fakeIngestService{status: ingest.Status{State: ingest.StateRunning, Run: &store.Run{ID: 1, State: store.RunRunning}}}
		got, err := newTestIngestion(svc, fakeLockChecker{}).Status(t.Context())
		if err != nil || len(got.SkipReasons) != len(knownSkipReasons) || len(got.FieldErrors) != len(knownFieldErrors) {
			t.Errorf("Status = %+v, %v; want every known key at 0", got, err)
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

// funcLockChecker is a lockChecker backed by a function.
type funcLockChecker func(ctx context.Context) (bool, error)

func (f funcLockChecker) IngestLockHeld(ctx context.Context) (bool, error) { return f(ctx) }

// The pre-check has its own short budget: a hung database must not use up
// the request's whole budget before the engine can answer.
func TestIngestionLockCheckTimeout(t *testing.T) {
	t.Parallel()
	var left time.Duration
	check := funcLockChecker(func(ctx context.Context) (bool, error) {
		d, ok := ctx.Deadline()
		if !ok {
			return false, errors.New("no deadline")
		}
		left = time.Until(d)
		return false, context.DeadlineExceeded // as if the database hung
	})
	svc := &fakeIngestService{startErr: fmt.Errorf("%w: absent", ingest.ErrDatasetMissing)}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := newTestIngestion(svc, check).Start(ctx)
	if left <= 0 || left > lockCheckTimeout {
		t.Errorf("pre-check deadline in %v, want at most %v", left, lockCheckTimeout)
	}
	var dm *httpapi.DatasetMissingError
	if svc.starts != 1 || !errors.As(err, &dm) {
		t.Errorf("after a timed-out pre-check: %d engine starts, err %v; want the engine's dataset_missing", svc.starts, err)
	}
}

// A failed pre-check is logged at WARN with the request ID, or at DEBUG when
// the client went away.
func TestIngestionLockCheckLogging(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		cancel    bool
		wantLevel string
	}{
		{"database error", false, "WARN"},
		{"client went away", true, "DEBUG"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var logs syncBuffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			check := funcLockChecker(func(context.Context) (bool, error) {
				if tt.cancel {
					cancel()
					return false, context.Canceled
				}
				return false, errors.New("checking ingest lock: connection refused")
			})
			ing := ingestion{svc: &fakeIngestService{startErr: errors.New("boom")}, lock: check, maildir: "/m", logger: logger}
			h := httpapi.New(httpapi.Deps{Ingester: ing}, logger)
			req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/start", nil)
			req.Header.Set(httpapi.HeaderRequestID, "req-123")
			h.ServeHTTP(httptest.NewRecorder(), req)

			rec := findLog(decodeLogLines(t, []byte(logs.String())), "checking the ingest lock failed; trying to start anyway")
			if rec == nil || rec["level"] != tt.wantLevel || rec["request_id"] != "req-123" {
				t.Errorf("log record = %v, want level %s with request_id req-123", rec, tt.wantLevel)
			}
		})
	}
}

// Scenario: Running out of file descriptors is transient (#49), from the
// engine's error to the response: POST /start answers 503 unavailable with a
// retry message, not 409 dataset_missing.
func TestStartTransientDatasetErrorIs503(t *testing.T) {
	t.Parallel()
	engineErr := fmt.Errorf("%w: %w", ingest.ErrTransient,
		&fs.PathError{Op: "open", Path: "/data/maildir", Err: syscall.EMFILE})
	ing := newTestIngestion(&fakeIngestService{startErr: engineErr}, fakeLockChecker{})
	h := httpapi.New(httpapi.Deps{Ingester: ing}, slog.New(slog.DiscardHandler))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/start", nil))

	const want = `{"error":{"code":"unavailable","message":"the dataset cannot be checked right now ` +
		`(a temporary system error such as too many open files); retry later"}}`
	if rec.Code != http.StatusServiceUnavailable || strings.TrimSpace(rec.Body.String()) != want {
		t.Errorf("POST /start = %d %s, want 503 %s", rec.Code, rec.Body.String(), want)
	}
}
