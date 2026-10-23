package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hrhelena/elisa-task/internal/fetch"
)

var updateGolden = flag.Bool("update", false, "rewrite the golden files in testdata/")

// fakeIngester returns fixed results and records the contexts it was called with.
type fakeIngester struct {
	run      IngestionRun
	startErr error
	status   IngestionStatus
	statErr  error

	mu        sync.Mutex
	starts    int
	deadlines []time.Duration
}

func (f *fakeIngester) record(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var left time.Duration
	if d, ok := ctx.Deadline(); ok {
		left = time.Until(d)
	}
	f.deadlines = append(f.deadlines, left)
}

func (f *fakeIngester) Start(ctx context.Context) (IngestionRun, error) {
	f.record(ctx)
	f.mu.Lock()
	f.starts++
	f.mu.Unlock()
	return f.run, f.startErr
}

func (f *fakeIngester) Status(ctx context.Context) (IngestionStatus, error) {
	f.record(ctx)
	return f.status, f.statErr
}

type fakeDatasets struct{ p fetch.Progress }

func (f fakeDatasets) Progress() fetch.Progress { return f.p }

func newIngestAPI(t *testing.T, ing Ingester, ds DatasetReporter) *testAPI {
	t.Helper()
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := prometheus.NewRegistry()
	return &testAPI{handler: New(Deps{Registry: reg, Ingester: ing, Datasets: ds}, logger), logs: logs, reg: reg}
}

func ptr[T any](v T) *T { return &v }

var (
	t0 = time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)
	// A non-UTC zone, to check that timestamps are served in UTC.
	helsinki = time.FixedZone("EEST", 3*60*60)
)

// The JSON bodies of POST /start (ADR 0011), one row per situation of the
// ticket's table, plus the shutdown and joined-error cases.
func TestStartHandler(t *testing.T) {
	t.Parallel()
	missing := &DatasetMissingError{Path: "/data/maildir", Err: errors.New("dataset not found: lstat: no such file")}
	tests := []struct {
		name         string
		run          IngestionRun
		err          error
		wantStatus   int
		wantLocation string
		wantBody     string
	}{
		{
			// Scenario: Start an ingestion; also covers starting after a
			// completed or failed run (the ingester starts a new run).
			name:         "started",
			run:          IngestionRun{ID: 4, State: StateRunning, StartedAt: t0.In(helsinki)},
			wantStatus:   http.StatusAccepted,
			wantLocation: "/status",
			wantBody:     `{"run_id":4,"state":"running","started_at":"2026-10-23T10:00:00Z"}`,
		},
		{
			// Scenario: Start while running; Concurrent starts on two replicas.
			name:       "already running",
			err:        fmt.Errorf("lock: %w", ErrIngestionRunning),
			wantStatus: http.StatusConflict,
			wantBody:   `{"error":{"code":"ingestion_running","message":"an ingestion is already running"}}`,
		},
		{
			// Scenario: Dataset missing.
			name:       "dataset missing",
			err:        missing,
			wantStatus: http.StatusConflict,
			wantBody: `{"error":{"code":"dataset_missing",` +
				`"message":"maildir not found at /data/maildir; call POST /fetch first"}}`,
		},
		{
			// Scenario: Start explains a symlinked maildir (#45): the
			// message names the problem instead of sending the caller to
			// POST /fetch, which would refuse the symlink too.
			name: "maildir is a symlink",
			err: fmt.Errorf("starting: %w", &DatasetMissingError{
				Path:    "/data/maildir",
				Problem: "it is a symlink, which is not followed; replace it with the directory itself",
				Err:     errors.New("dataset not found: dataset root is not usable"),
			}),
			wantStatus: http.StatusConflict,
			wantBody: `{"error":{"code":"dataset_missing","message":"the maildir at /data/maildir cannot be used: ` +
				`it is a symlink, which is not followed; replace it with the directory itself"}}`,
		},
		{
			name:       "shutting down",
			err:        ErrIngesterClosed,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":{"code":"unavailable","message":"the service is shutting down"}}`,
		},
		{
			// Shutdown wins over the error the canceled Start was failing with.
			name:       "shutting down joined with dataset missing",
			err:        errors.Join(ErrIngesterClosed, missing),
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":{"code":"unavailable","message":"the service is shutting down"}}`,
		},
		{
			name:       "shutting down joined with already running",
			err:        errors.Join(ErrIngestionRunning, ErrIngesterClosed),
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":{"code":"unavailable","message":"the service is shutting down"}}`,
		},
		{
			// Scenario: Running out of file descriptors is transient (#49):
			// 503, not 409 dataset_missing.
			name:       "dataset check fails with EMFILE",
			err:        fmt.Errorf("%w: open /data/maildir: too many open files", ErrDatasetUnavailable),
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":{"code":"unavailable","message":"` + transientMessage + `"}}`,
		},
		{
			name:       "shutting down joined with a transient dataset error",
			err:        errors.Join(ErrDatasetUnavailable, ErrIngesterClosed),
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":{"code":"unavailable","message":"the service is shutting down"}}`,
		},
		{
			// DB unavailable: taking the lock or beginning the run failed.
			name:       "database unavailable",
			err:        errors.New("acquiring ingest lock: dial tcp: connection refused"),
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":{"code":"unavailable","message":"the database is unavailable, retry later"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ing := &fakeIngester{run: tt.run, startErr: tt.err}
			api := newIngestAPI(t, ing, nil)
			rec := api.do(t, http.MethodPost, "/start", nil)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tt.wantBody {
				t.Errorf("body = %s\nwant   %s", got, tt.wantBody)
			}
			if got := rec.Header().Get("Location"); got != tt.wantLocation {
				t.Errorf("Location = %q, want %q", got, tt.wantLocation)
			}
			if got := rec.Header().Get("Content-Type"); got != contentTypeJSON {
				t.Errorf("Content-Type = %q", got)
			}
			if ing.deadlines[0] <= 0 || ing.deadlines[0] > startTimeout {
				t.Errorf("Start context deadline in %v, want within %v", ing.deadlines[0], startTimeout)
			}
		})
	}
}

func TestStartHandlerLogsDatabaseFailures(t *testing.T) {
	t.Parallel()
	api := newIngestAPI(t, &fakeIngester{startErr: errors.New("boom")}, nil)
	api.do(t, http.MethodPost, "/start", nil)
	if lines := api.logs.find(t, "starting ingestion failed"); len(lines) != 1 || lines[0]["level"] != "ERROR" {
		t.Errorf("log lines = %v, want one ERROR", lines)
	}

	// Expected outcomes are not errors.
	api = newIngestAPI(t, &fakeIngester{startErr: ErrIngestionRunning}, nil)
	api.do(t, http.MethodPost, "/start", nil)
	if lines := api.logs.find(t, "starting ingestion failed"); len(lines) != 0 {
		t.Errorf("409 logged as a failure: %v", lines)
	}
}

// POST /start ignores any request body.
func TestStartHandlerIgnoresBody(t *testing.T) {
	t.Parallel()
	ing := &fakeIngester{run: IngestionRun{ID: 1, State: StateRunning, StartedAt: t0}}
	api := newIngestAPI(t, ing, nil)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/start", strings.NewReader(`{"force":true}`))
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	api.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", rec.Code)
	}
}

// skipReasons and fieldErrors return the maps the adapter in main serves for
// a run: every known key, 0 unless overridden.
func skipReasons(set map[string]int64) map[string]int64 {
	m := map[string]int64{
		"empty_file": 0, "not_an_email": 0, "header_too_large": 0, "read_error": 0,
		"outside_mailbox": 0, "not_regular_file": 0, "parse_panic": 0, "parse_error": 0,
	}
	maps.Copy(m, set)
	return m
}

func fieldErrors(set map[string]int64) map[string]int64 {
	m := map[string]int64{
		"invalid_sender": 0, "missing_sender": 0, "invalid_date": 0, "missing_date": 0,
		"invalid_encoding": 0, "invalid_recipient": 0,
	}
	maps.Copy(m, set)
	return m
}

// statusFixtures are the four states of GET /status, served from golden files.
func statusFixtures() map[string]IngestionStatus {
	updated := t0.Add(65 * time.Second).In(helsinki)
	finished := t0.Add(90 * time.Second)
	return map[string]IngestionStatus{
		// Scenario: Status before any ingestion (maps from the ingester are
		// nil; the ticket's idle response has empty maps).
		"idle": {State: StateIdle},
		// Scenario: Status while running.
		"running": {
			State: StateRunning, RunID: ptr(int64(3)), StartedAt: &t0, UpdatedAt: &updated,
			FilesSeen: 120347, MessagesProcessed: 120345, MessagesSkipped: 2, TotalMessages: 120345,
			SkipReasons: skipReasons(map[string]int64{"empty_file": 1, "not_an_email": 1}),
			FieldErrors: fieldErrors(map[string]int64{"invalid_sender": 1}),
		},
		// Scenario: Status after completion; Malformed files are reported.
		"completed": {
			State: StateCompleted, RunID: ptr(int64(3)), StartedAt: &t0, UpdatedAt: &finished, FinishedAt: &finished,
			FilesSeen: 8, MessagesProcessed: 6, MessagesSkipped: 2, TotalMessages: 6,
			SkipReasons: skipReasons(map[string]int64{"empty_file": 1, "not_an_email": 1}),
			FieldErrors: fieldErrors(map[string]int64{"invalid_sender": 1, "invalid_date": 1}),
		},
		// Scenario: Failed run is not finished.
		"failed": {
			State: StateFailed, RunID: ptr(int64(2)), StartedAt: &t0, UpdatedAt: &finished, FinishedAt: &finished,
			FilesSeen: 3, MessagesProcessed: 3, TotalMessages: 5,
			SkipReasons: skipReasons(nil), FieldErrors: fieldErrors(nil),
			Error: ptr("interrupted: shutdown"),
		},
	}
}

func TestStatusHandlerGolden(t *testing.T) {
	t.Parallel()
	for name, st := range statusFixtures() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ing := &fakeIngester{status: st}
			rec := newIngestAPI(t, ing, nil).do(t, http.MethodGet, "/status", nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %s", rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", got)
			}
			if ing.deadlines[0] <= 0 || ing.deadlines[0] > statusTimeout {
				t.Errorf("Status context deadline in %v, want within %v", ing.deadlines[0], statusTimeout)
			}
			var pretty bytes.Buffer
			if err := json.Indent(&pretty, bytes.TrimSpace(rec.Body.Bytes()), "", "  "); err != nil {
				t.Fatalf("indenting %s: %v", rec.Body.String(), err)
			}
			pretty.WriteByte('\n') // files end with a newline (pre-commit end-of-file-fixer)
			golden := filepath.Join("testdata", "status_"+name+".golden.json")
			if *updateGolden {
				if err := os.WriteFile(golden, pretty.Bytes(), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden) //nolint:gosec // fixed test file name
			if err != nil {
				t.Fatalf("reading golden file (run with -update to create it): %v", err)
			}
			if pretty.String() != string(want) {
				t.Errorf("GET /status body differs from %s:\n%s", golden, pretty.String())
			}
		})
	}
}

// finished is true only for a completed run (ADR 0011); an empty state is
// reported as idle; maps are never null.
func TestStatusResponseFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state        string
		wantState    string
		wantFinished bool
	}{
		{"", StateIdle, false},
		{StateIdle, StateIdle, false},
		{StateRunning, StateRunning, false},
		{StateCompleted, StateCompleted, true},
		{StateFailed, StateFailed, false},
	}
	for _, tt := range tests {
		resp := newStatusResponse(IngestionStatus{State: tt.state})
		if resp.State != tt.wantState || resp.Finished != tt.wantFinished {
			t.Errorf("state %q: got state %q finished %v, want %q %v",
				tt.state, resp.State, resp.Finished, tt.wantState, tt.wantFinished)
		}
		if resp.SkipReasons == nil || resp.FieldErrors == nil {
			t.Errorf("state %q: nil maps", tt.state)
		}
	}
}

// The dataset section of GET /status (#10, ADR 0011).
func TestStatusHandlerDatasetSection(t *testing.T) {
	t.Parallel()
	ds := fakeDatasets{p: fetch.Progress{State: fetch.StateReady, Path: "/data/maildir", BytesTotal: -1}}
	rec := newIngestAPI(t, &fakeIngester{status: IngestionStatus{State: StateIdle}}, ds).do(t, http.MethodGet, "/status", nil)
	body := decodeBody(t, rec)
	want := map[string]any{
		"state": "ready", "path": "/data/maildir", "bytes_downloaded": 0.0, "bytes_total": -1.0,
		"files_extracted": 0.0, "error": nil, "started_at": nil, "finished_at": nil,
	}
	got, ok := body["dataset"].(map[string]any)
	if !ok {
		t.Fatalf("no dataset object in %s", rec.Body.String())
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("dataset.%s = %v, want %v", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("dataset = %v, want exactly the keys of %v", got, want)
	}

	failed := fakeDatasets{p: fetch.Progress{State: fetch.StateFailed, Path: "/data/maildir", Error: "download failed: HTTP 404"}}
	rec = newIngestAPI(t, &fakeIngester{}, failed).do(t, http.MethodGet, "/status", nil)
	if d, _ := decodeBody(t, rec)["dataset"].(map[string]any); d["state"] != "failed" || d["error"] != "download failed: HTTP 404" {
		t.Errorf("failed dataset = %v", d)
	}
}

// Scenario: Database down.
func TestStatusHandlerDatabaseDown(t *testing.T) {
	t.Parallel()
	api := newIngestAPI(t, &fakeIngester{statErr: errors.New("ingestion status: dial tcp: connection refused")}, nil)
	rec := api.do(t, http.MethodGet, "/status", nil)
	want := `{"error":{"code":"unavailable","message":"the database is unavailable, retry later"}}`
	if rec.Code != http.StatusServiceUnavailable || strings.TrimSpace(rec.Body.String()) != want {
		t.Errorf("GET /status = %d %s, want 503 %s", rec.Code, rec.Body.String(), want)
	}
	if lines := api.logs.find(t, "reading ingestion status failed"); len(lines) != 1 || lines[0]["level"] != "ERROR" {
		t.Errorf("log lines = %v, want one ERROR", lines)
	}
}

// The production registration in routes puts POST /start and POST /fetch in
// the mutating group (ADR 0005: where authorization goes) and GET /status in
// the read group. The guard is installed before routes runs, so this checks
// routes itself, not a re-registration; without an Ingester neither
// ingestion route is registered.
func TestIngestRoutes(t *testing.T) {
	t.Parallel()
	ing := &fakeIngester{run: IngestionRun{ID: 1, State: StateRunning, StartedAt: t0}}
	fetcher := &fakeFetcher{progress: fetch.Progress{State: fetch.StateReady}}
	deps := Deps{Ingester: ing, Fetcher: fetcher}
	logs := &syncBuffer{}
	rt := newRouter(deps, slog.New(slog.NewJSONHandler(logs, nil)))
	rt.mutating = append(rt.mutating, func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusForbidden, "forbidden", "admin only")
		})
	})
	rt.routes(deps)
	guarded := &testAPI{handler: rt.handler(), logs: logs}

	if rec := guarded.do(t, http.MethodPost, "/start", nil); rec.Code != http.StatusForbidden || ing.starts != 0 {
		t.Errorf("POST /start = %d (%d starts), want 403 from the mutating middleware and no Start call", rec.Code, ing.starts)
	}
	if rec := guarded.do(t, http.MethodPost, "/fetch", nil); rec.Code != http.StatusForbidden || fetcher.calls.Load() != 0 {
		t.Errorf("POST /fetch = %d (%d starts), want 403 from the mutating middleware and no Start call",
			rec.Code, fetcher.calls.Load())
	}
	if rec := guarded.do(t, http.MethodGet, "/status", nil); rec.Code != http.StatusOK {
		t.Errorf("GET /status = %d, want 200 (no mutating middleware)", rec.Code)
	}

	api := newIngestAPI(t, ing, nil)
	if rec := api.do(t, http.MethodGet, "/start", nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /start = %d, want 405", rec.Code)
	}
	if rec := api.do(t, http.MethodPost, "/status", nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /status = %d, want 405", rec.Code)
	}
	empty := newIngestAPI(t, nil, nil)
	for _, r := range []struct{ method, path string }{{http.MethodPost, "/start"}, {http.MethodGet, "/status"}} {
		if rec := empty.do(t, r.method, r.path, nil); rec.Code != http.StatusNotFound {
			t.Errorf("%s %s without an Ingester = %d, want 404", r.method, r.path, rec.Code)
		}
	}
}
