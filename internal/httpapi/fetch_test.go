package httpapi

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hrhelena/elisa-task/internal/fetch"
)

// fakeFetcher returns a fixed progress and error from Start.
type fakeFetcher struct {
	progress fetch.Progress
	err      error
	calls    atomic.Int64
}

func (f *fakeFetcher) Start() (fetch.Progress, error) {
	f.calls.Add(1)
	return f.progress, f.err
}

func newFetchAPI(t *testing.T, f Fetcher) *testAPI {
	t.Helper()
	logs := &syncBuffer{}
	return &testAPI{
		handler: New(Deps{Fetcher: f}, slog.New(slog.NewJSONHandler(logs, nil))),
		logs:    logs,
	}
}

func TestFetchHandler(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		progress     fetch.Progress
		err          error
		wantStatus   int
		wantLocation string
		wantCode     string
		wantFields   map[string]any
	}{
		{
			name:       "already ready",
			progress:   fetch.Progress{State: fetch.StateReady, Path: "/data/maildir", BytesTotal: -1},
			wantStatus: http.StatusOK,
			wantFields: map[string]any{"state": "ready", "path": "/data/maildir", "error": nil},
		},
		{
			name:         "started now",
			progress:     fetch.Progress{State: fetch.StateDownloading, Path: "/data/maildir", BytesTotal: -1, StartedAt: &started},
			wantStatus:   http.StatusAccepted,
			wantLocation: "/status",
			wantFields: map[string]any{
				"state": "downloading", "bytes_downloaded": 0.0, "bytes_total": -1.0, "files_extracted": 0.0,
				"error": nil, "started_at": "2026-10-23T09:00:00Z", "finished_at": nil,
			},
		},
		{
			name: "already in progress",
			progress: fetch.Progress{
				State: fetch.StateExtracting, Path: "/data/maildir", BytesDownloaded: 100, BytesTotal: 200, FilesExtracted: 7,
			},
			wantStatus:   http.StatusAccepted,
			wantLocation: "/status",
			wantFields:   map[string]any{"state": "extracting", "bytes_downloaded": 100.0, "bytes_total": 200.0, "files_extracted": 7.0},
		},
		{
			name:       "data dir not writable",
			progress:   fetch.Progress{State: fetch.StateAbsent},
			err:        errors.Join(fetch.ErrDataDirNotWritable, os.ErrPermission),
			wantStatus: http.StatusInternalServerError,
			wantCode:   CodeDataDirNotWritable,
		},
		{
			name:       "shutting down",
			err:        fetch.ErrClosed,
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   CodeUnavailable,
		},
		{
			name:       "unexpected error",
			err:        errors.New("boom"),
			wantStatus: http.StatusInternalServerError,
			wantCode:   CodeInternal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			api := newFetchAPI(t, &fakeFetcher{progress: tt.progress, err: tt.err})
			rec := api.do(t, http.MethodPost, "/fetch", nil)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body %s", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if got := rec.Header().Get("Location"); got != tt.wantLocation {
				t.Errorf("Location = %q, want %q", got, tt.wantLocation)
			}
			if tt.wantCode != "" {
				if got := errorCode(t, rec); got != tt.wantCode {
					t.Errorf("error code = %q, want %q", got, tt.wantCode)
				}
				return
			}
			body := decodeBody(t, rec)
			for k, want := range tt.wantFields {
				if got, ok := body[k]; !ok || got != want {
					t.Errorf("%s = %v (present %v), want %v", k, got, ok, want)
				}
			}
		})
	}
}

func TestFetchHandler_FailedProgressReportsError(t *testing.T) {
	t.Parallel()
	msg := "download failed: HTTP 404"
	api := newFetchAPI(t, &fakeFetcher{progress: fetch.Progress{State: fetch.StateFailed, Error: msg}})
	rec := api.do(t, http.MethodPost, "/fetch", nil)
	if got := decodeBody(t, rec)["error"]; got != msg {
		t.Errorf("error = %v, want %q", got, msg)
	}
}

func TestFetchRoute(t *testing.T) {
	t.Parallel()
	api := newFetchAPI(t, &fakeFetcher{progress: fetch.Progress{State: fetch.StateReady}})
	rec := api.do(t, http.MethodGet, "/fetch", nil)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
		t.Errorf("GET /fetch = %d (Allow %q), want 405 with Allow: POST", rec.Code, rec.Header().Get("Allow"))
	}

	noFetcher := newFetchAPI(t, nil)
	if rec := noFetcher.do(t, http.MethodPost, "/fetch", nil); rec.Code != http.StatusNotFound {
		t.Errorf("POST /fetch without a fetcher = %d, want 404", rec.Code)
	}
}

// Round trips through the handler with a real fetch.Fetcher and a test
// archive server (ticket #10 test plan; no real download).

func tarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

type roundTrip struct {
	api      *testAPI
	fetcher  *fetch.Fetcher
	dataDir  string
	requests *atomic.Int64
}

func newRoundTrip(t *testing.T, dataDir string, h http.HandlerFunc) *roundTrip {
	t.Helper()
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	f := fetch.New(dataDir, srv.URL, srv.Client(), nil, nil, fetch.WithMinFreeBytes(0))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = f.Close(ctx)
	})
	return &roundTrip{api: newFetchAPI(t, f), fetcher: f, dataDir: dataDir, requests: &requests}
}

func (rt *roundTrip) waitFor(t *testing.T, want fetch.State) fetch.Progress {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		p := rt.fetcher.Progress()
		if p.State == want {
			return p
		}
		if time.Now().After(deadline) {
			t.Fatalf("state = %q (%s), want %q", p.State, p.Error, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func serveArchive(b []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		_, _ = w.Write(b)
	}
}

// Scenario: Dataset already present locally (through the handler).
func TestFetchRoundTrip_DatasetAlreadyPresentLocally(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dataDir, "maildir"), 0o750); err != nil {
		t.Fatal(err)
	}
	rt := newRoundTrip(t, dataDir, serveArchive(nil))

	rec := rt.api.do(t, http.MethodPost, "/fetch", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := decodeBody(t, rec)
	if body["state"] != "ready" || body["path"] != filepath.Join(dataDir, "maildir") {
		t.Errorf("body = %v", body)
	}
	if n := rt.requests.Load(); n != 0 {
		t.Errorf("requests to DATASET_URL = %d, want 0", n)
	}
}

// Scenario: Fresh download and extraction (through the handler).
func TestFetchRoundTrip_FreshDownloadAndExtraction(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	rt := newRoundTrip(t, dataDir, serveArchive(tarGz(t, map[string]string{
		"maildir/allen-p/inbox/1.": "a",
		"maildir/lay-k/sent/1.":    "b",
	})))

	rec := rt.api.do(t, http.MethodPost, "/fetch", nil)
	if rec.Code != http.StatusAccepted || rec.Header().Get("Location") != "/status" {
		t.Fatalf("status = %d, Location %q; want 202 /status", rec.Code, rec.Header().Get("Location"))
	}
	if body := decodeBody(t, rec); body["state"] != "downloading" || body["bytes_downloaded"] != 0.0 || body["bytes_total"] != -1.0 {
		t.Errorf("body = %v", body)
	}
	rt.waitFor(t, fetch.StateReady)
	for _, p := range []string{"maildir/allen-p/inbox/1.", "maildir/lay-k/sent/1.", ".fetch-complete"} {
		if _, err := os.Stat(filepath.Join(dataDir, p)); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
	// Now idempotent: 200 ready, no new download.
	if rec := rt.api.do(t, http.MethodPost, "/fetch", nil); rec.Code != http.StatusOK {
		t.Errorf("second POST /fetch = %d, want 200", rec.Code)
	}
	if n := rt.requests.Load(); n != 1 {
		t.Errorf("requests = %d, want 1", n)
	}
}

// Scenario: Second fetch while downloading (through the handler).
func TestFetchRoundTrip_SecondFetchWhileDownloading(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	sent := make(chan struct{})
	archive := tarGz(t, map[string]string{"maildir/allen-p/inbox/1.": string(make([]byte, 1<<20))})
	rt := newRoundTrip(t, t.TempDir(), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(archive)))
		_, _ = w.Write(archive[:len(archive)/2]) // a valid gzip prefix.
		w.(http.Flusher).Flush()                 //nolint:forcetypeassert // httptest writers flush.
		close(sent)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})

	if rec := rt.api.do(t, http.MethodPost, "/fetch", nil); rec.Code != http.StatusAccepted {
		t.Fatalf("first POST /fetch = %d", rec.Code)
	}
	<-sent
	deadline := time.Now().Add(5 * time.Second)
	for rt.fetcher.Progress().BytesDownloaded == 0 { // the client has the headers and first bytes.
		if time.Now().After(deadline) {
			t.Fatal("no bytes received")
		}
		time.Sleep(5 * time.Millisecond)
	}
	rec := rt.api.do(t, http.MethodPost, "/fetch", nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("second POST /fetch = %d, want 202", rec.Code)
	}
	if body := decodeBody(t, rec); body["state"] != "downloading" || body["bytes_total"] != float64(len(archive)) {
		t.Errorf("body = %v, want the current progress", body)
	}
	if n := rt.requests.Load(); n != 1 {
		t.Errorf("requests = %d, want exactly 1", n)
	}
}

// Scenarios: Server returns 404, then Retry after failure (through the handler).
func TestFetchRoundTrip_404ThenRetry(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	archive := tarGz(t, map[string]string{"maildir/allen-p/inbox/1.": "a"})
	var ok atomic.Bool
	rt := newRoundTrip(t, dataDir, func(w http.ResponseWriter, r *http.Request) {
		if !ok.Load() {
			http.NotFound(w, r)
			return
		}
		serveArchive(archive)(w, r)
	})

	if rec := rt.api.do(t, http.MethodPost, "/fetch", nil); rec.Code != http.StatusAccepted {
		t.Fatalf("POST /fetch = %d", rec.Code)
	}
	if p := rt.waitFor(t, fetch.StateFailed); p.Error != "download failed: HTTP 404" {
		t.Errorf("error = %q", p.Error)
	}
	matches, _ := filepath.Glob(filepath.Join(dataDir, ".fetch-tmp-*"))
	if _, err := os.Stat(filepath.Join(dataDir, "maildir")); err == nil || len(matches) > 0 {
		t.Errorf("leftovers after failure: maildir err=%v, temp dirs %v", err, matches)
	}

	ok.Store(true)
	rec := rt.api.do(t, http.MethodPost, "/fetch", nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("retry POST /fetch = %d, want 202", rec.Code)
	}
	if body := decodeBody(t, rec); body["state"] != "downloading" || body["error"] != nil {
		t.Errorf("retry body = %v", body)
	}
	rt.waitFor(t, fetch.StateReady)
}

// Scenario: DATA_DIR not writable (through the handler).
func TestFetchRoundTrip_DataDirNotWritable(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	rt := newRoundTrip(t, filepath.Join(file, "data"), serveArchive(nil))
	rec := rt.api.do(t, http.MethodPost, "/fetch", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if got := errorCode(t, rec); got != CodeDataDirNotWritable {
		t.Errorf("code = %q", got)
	}
}
