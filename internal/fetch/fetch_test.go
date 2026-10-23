package fetch

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Scenario: Dataset already present locally.
func TestFetch_DatasetAlreadyPresentLocally(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dataDir, MaildirName), 0o750); err != nil {
		t.Fatal(err)
	}
	srv := newArchiveServer(t, serveBytes(sampleArchive(t)))
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client())

	if p := f.Progress(); p.State != StateReady {
		t.Errorf("initial state = %q, want ready", p.State)
	}
	p, err := f.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if p.State != StateReady || p.Path != filepath.Join(dataDir, MaildirName) {
		t.Errorf("Start = %+v, want state ready with the maildir path", p)
	}
	if n := srv.requests.Load(); n != 0 {
		t.Errorf("requests to DATASET_URL = %d, want 0", n)
	}
	assertNoTempDirs(t, dataDir)
}

// Scenario: Fresh download and extraction.
func TestFetch_FreshDownloadAndExtraction(t *testing.T) {
	t.Parallel()
	dataDir := filepath.Join(t.TempDir(), "data") // created by the fetch.
	archive := sampleArchive(t)
	srv := newArchiveServer(t, serveBytes(archive))
	f, reg := newTestFetcher(t, dataDir, srv.URL, srv.Client())

	if p := f.Progress(); p.State != StateAbsent {
		t.Errorf("initial state = %q, want absent", p.State)
	}
	p, err := f.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if p.State != StateDownloading || p.BytesDownloaded != 0 || p.BytesTotal != -1 || p.StartedAt == nil {
		t.Errorf("Start = %+v, want a fresh downloading progress", p)
	}

	p = waitForState(t, f, StateReady)
	if p.BytesDownloaded != int64(len(archive)) || p.BytesTotal != int64(len(archive)) {
		t.Errorf("bytes = %d/%d, want %d/%d", p.BytesDownloaded, p.BytesTotal, len(archive), len(archive))
	}
	if p.FilesExtracted != 2 || p.Error != "" || p.FinishedAt == nil {
		t.Errorf("progress = %+v, want 2 files, no error, finished", p)
	}
	assertFile(t, filepath.Join(dataDir, "maildir", "allen-p", "inbox", "1."), "From: phillip.allen@enron.com\n\nhello\n")
	assertFile(t, filepath.Join(dataDir, "maildir", "lay-k", "sent", "1."), "From: kenneth.lay@enron.com\n\nbye\n")
	assertNoTempDirs(t, dataDir)

	var m Marker
	b, err := os.ReadFile(filepath.Join(dataDir, MarkerName)) //nolint:gosec // test path.
	if err != nil {
		t.Fatalf("reading marker: %v", err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decoding marker: %v", err)
	}
	sum := sha256.Sum256(archive)
	if m.URL != srv.URL || m.SHA256 != hex.EncodeToString(sum[:]) || m.Bytes != int64(len(archive)) ||
		m.Files != 2 || m.FinishedAt.IsZero() {
		t.Errorf("marker = %+v", m)
	}

	if got := testutil.ToFloat64(f.metrics.bytes); got != float64(len(archive)) {
		t.Errorf("enron_fetch_bytes_total = %v, want %d", got, len(archive))
	}
	if got := testutil.ToFloat64(f.metrics.state.WithLabelValues(string(StateReady))); got != 1 {
		t.Errorf("enron_fetch_state{state=ready} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(f.metrics.state.WithLabelValues(string(StateDownloading))); got != 0 {
		t.Errorf("enron_fetch_state{state=downloading} = %v, want 0", got)
	}
	if n, err := testutil.GatherAndCount(reg, "enron_fetch_duration_seconds"); err != nil || n != 1 {
		t.Errorf("enron_fetch_duration_seconds series = %d (%v), want 1", n, err)
	}

	// Idempotent: a later Start reports ready without downloading again.
	if p, err := f.Start(); err != nil || p.State != StateReady {
		t.Errorf("second Start = %+v, %v; want ready", p, err)
	}
	if n := srv.requests.Load(); n != 1 {
		t.Errorf("requests = %d, want 1", n)
	}
}

// Scenario: Second fetch while downloading. It also shows that extraction is
// streamed: files are on disk while the rest of the body is still pending.
func TestFetch_SecondFetchWhileDownloading(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	archive := buildArchive(t,
		entry{name: "maildir/allen-p/inbox/1.", body: randomBody(t, 64<<10)},
		entry{name: "maildir/lay-k/sent/1.", body: randomBody(t, 256<<10)},
	)
	g := newGate()
	srv := newArchiveServer(t, serveInTwoParts(archive, g))
	t.Cleanup(g.open)
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client())

	if p, err := f.Start(); err != nil || p.State != StateDownloading {
		t.Fatalf("Start = %+v, %v", p, err)
	}
	<-g.sent

	// Concurrent Starts all get the in-flight progress.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			p, err := f.Start()
			if err != nil || p.State != StateDownloading {
				t.Errorf("concurrent Start = %+v, %v; want downloading", p, err)
			}
		})
	}
	wg.Wait()

	deadline := time.Now().Add(5 * time.Second)
	for f.Progress().FilesExtracted < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("no file extracted while the download was in progress: %+v", f.Progress())
		}
		time.Sleep(5 * time.Millisecond)
	}
	p := f.Progress()
	if p.State != StateDownloading || p.BytesDownloaded == 0 || p.BytesTotal != int64(len(archive)) {
		t.Errorf("in-flight progress = %+v", p)
	}
	assertNotExist(t, filepath.Join(dataDir, MaildirName)) // not visible until complete.

	g.open()
	waitForState(t, f, StateReady)
	if n := srv.requests.Load(); n != 1 {
		t.Errorf("requests = %d, want exactly 1", n)
	}
}

// Scenario: Server returns 404.
func TestFetch_ServerReturns404(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	srv := newArchiveServer(t, http.NotFound)
	f, reg := newTestFetcher(t, dataDir, srv.URL, srv.Client())

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p := waitForState(t, f, StateFailed)
	if p.Error != "download failed: HTTP 404" {
		t.Errorf("error = %q, want %q", p.Error, "download failed: HTTP 404")
	}
	if p.FinishedAt == nil {
		t.Error("FinishedAt is nil")
	}
	assertNotExist(t, filepath.Join(dataDir, MaildirName))
	assertNotExist(t, filepath.Join(dataDir, MarkerName))
	assertNoTempDirs(t, dataDir)
	if got := testutil.ToFloat64(f.metrics.state.WithLabelValues(string(StateFailed))); got != 1 {
		t.Errorf("enron_fetch_state{state=failed} = %v, want 1", got)
	}
	if n, err := testutil.GatherAndCount(reg, "enron_fetch_duration_seconds"); err != nil || n != 1 {
		t.Errorf("enron_fetch_duration_seconds series = %d (%v), want 1", n, err)
	}
}

// Scenario: Retry after failure.
func TestFetch_RetryAfterFailure(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	archive := sampleArchive(t)
	var mu sync.Mutex
	fail := true
	srv := newArchiveServer(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		f := fail
		mu.Unlock()
		if f {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		serveBytes(archive)(w, r)
	})
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client())

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if p := waitForState(t, f, StateFailed); p.Error != "download failed: HTTP 500" {
		t.Errorf("error = %q", p.Error)
	}
	// The failure stays visible until the next Start.
	if p := f.Progress(); p.State != StateFailed {
		t.Errorf("state = %q, want failed", p.State)
	}

	mu.Lock()
	fail = false
	mu.Unlock()
	p, err := f.Start()
	if err != nil {
		t.Fatalf("retry Start: %v", err)
	}
	if p.State != StateDownloading || p.Error != "" || p.FinishedAt != nil {
		t.Errorf("retry Start = %+v, want a fresh downloading progress", p)
	}
	p = waitForState(t, f, StateReady)
	if p.Error != "" || p.FilesExtracted != 2 {
		t.Errorf("progress = %+v", p)
	}
	if n := srv.requests.Load(); n != 2 {
		t.Errorf("requests = %d, want 2", n)
	}
}

// Scenario: Path traversal entry is rejected (end to end).
func TestFetch_PathTraversalEntryIsRejected(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	dataDir := filepath.Join(parent, "a", "data")
	archive := buildArchive(t,
		entry{name: "../../evil.txt", body: "evil"},
		entry{name: "maildir/allen-p/inbox/1.", body: "ok"},
	)
	srv := newArchiveServer(t, serveBytes(archive))
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client())

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForState(t, f, StateReady)
	assertNotExist(t, filepath.Join(parent, "evil.txt"))
	assertNotExist(t, filepath.Join(parent, "a", "evil.txt"))
	assertNotExist(t, filepath.Join(dataDir, "evil.txt"))
	assertFile(t, filepath.Join(dataDir, "maildir", "allen-p", "inbox", "1."), "ok")
	if got := testutil.ToFloat64(f.metrics.skipped.WithLabelValues(skipUnsafePath)); got != 1 {
		t.Errorf("enron_fetch_entries_skipped_total{reason=unsafe_path} = %v, want 1", got)
	}
}

// Scenario: Symlink entry is skipped (end to end).
func TestFetch_SymlinkEntryIsSkipped(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	archive := buildArchive(t,
		entry{name: "maildir/allen-p/inbox/1.", body: "ok"},
		entry{name: "maildir/allen-p/link", typ: tar.TypeSymlink, linkname: "/etc/passwd"},
	)
	srv := newArchiveServer(t, serveBytes(archive))
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client())

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForState(t, f, StateReady)
	if links := symlinksUnder(t, dataDir); len(links) > 0 {
		t.Errorf("symlinks in DATA_DIR: %v", links)
	}
	assertNotExist(t, filepath.Join(dataDir, "maildir", "allen-p", "link"))
}

// Scenario: Truncated archive.
func TestFetch_TruncatedArchive(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	archive := buildArchive(t,
		entry{name: "maildir/allen-p/inbox/1.", body: strings.Repeat("a", 32<<10)},
		entry{name: "maildir/lay-k/sent/1.", body: strings.Repeat("b", 32<<10)},
	)
	// Promise the full length, send half, and close the connection.
	srv := newArchiveServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(archive)))
		_, _ = w.Write(archive[:len(archive)/2])
	})
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client())

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p := waitForState(t, f, StateFailed)
	if !strings.Contains(p.Error, "unexpected EOF") {
		t.Errorf("error = %q, want an unexpected EOF", p.Error)
	}
	assertNotExist(t, filepath.Join(dataDir, MaildirName))
	assertNotExist(t, filepath.Join(dataDir, MarkerName))
	assertNoTempDirs(t, dataDir)
}

// Scenario: Shutdown during download.
func TestFetch_ShutdownDuringDownload(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	archive := buildArchive(t,
		entry{name: "maildir/allen-p/inbox/1.", body: randomBody(t, 64<<10)},
		entry{name: "maildir/lay-k/sent/1.", body: randomBody(t, 256<<10)},
	)
	g := newGate()
	srv := newArchiveServer(t, serveInTwoParts(archive, g))
	t.Cleanup(g.open)
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client())

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	<-g.sent
	deadline := time.Now().Add(5 * time.Second)
	for f.Progress().FilesExtracted < 1 { // the temp dir has content.
		if time.Now().After(deadline) {
			t.Fatal("nothing extracted")
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertNoTempDirs(t, dataDir)
	assertNotExist(t, filepath.Join(dataDir, MaildirName))
	p := f.Progress()
	if p.State != StateFailed || !strings.Contains(p.Error, "fetch canceled") {
		t.Errorf("progress after Close = %+v, want failed with a cancellation error", p)
	}
	if _, err := f.Start(); !errors.Is(err, ErrClosed) {
		t.Errorf("Start after Close error = %v, want ErrClosed", err)
	}
	if err := f.Close(ctx); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestFetch_StalledDownloadIsAborted(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	g := newGate()
	srv := newArchiveServer(t, serveInTwoParts(sampleArchive(t), g))
	t.Cleanup(g.open)
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client(), WithStallTimeout(100*time.Millisecond))

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p := waitForState(t, f, StateFailed)
	if p.Error != "download stalled: no data received for 100ms" {
		t.Errorf("error = %q", p.Error)
	}
	assertNoTempDirs(t, dataDir)
	assertNotExist(t, filepath.Join(dataDir, MaildirName))
}

func TestFetch_InsufficientDiskSpace(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	srv := newArchiveServer(t, serveBytes(sampleArchive(t)))
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client(), WithMinFreeBytes(DefaultMinFreeBytes))
	f.freeSpace = func(string) (uint64, error) { return 1536 << 20, nil }

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p := waitForState(t, f, StateFailed)
	if p.Error != "insufficient disk space: need 4GiB, have 1.5GiB" {
		t.Errorf("error = %q", p.Error)
	}
	if n := srv.requests.Load(); n != 0 {
		t.Errorf("requests = %d, want 0", n)
	}
	assertNoTempDirs(t, dataDir)
}

func TestFetch_RealFreeSpaceCheckPasses(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	srv := newArchiveServer(t, serveBytes(sampleArchive(t)))
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client(), WithMinFreeBytes(1))
	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForState(t, f, StateReady)
}

func TestFetch_DataDirNotWritable(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	srv := newArchiveServer(t, serveBytes(sampleArchive(t)))
	f, _ := newTestFetcher(t, filepath.Join(file, "data"), srv.URL, srv.Client())

	p, err := f.Start()
	if !errors.Is(err, ErrDataDirNotWritable) {
		t.Fatalf("Start error = %v, want ErrDataDirNotWritable", err)
	}
	if p.State != StateAbsent {
		t.Errorf("state = %q, want absent", p.State)
	}
	if n := srv.requests.Load(); n != 0 {
		t.Errorf("requests = %d, want 0", n)
	}
}

func TestFetch_UnreachableServer(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	srv := newArchiveServer(t, serveBytes(nil))
	url := srv.URL
	srv.Close() // nothing listens there any more.
	f, _ := newTestFetcher(t, dataDir, url, nil)

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p := waitForState(t, f, StateFailed)
	if !strings.HasPrefix(p.Error, "download failed: ") {
		t.Errorf("error = %q", p.Error)
	}
	assertNoTempDirs(t, dataDir)
}

func TestNew_RemovesLeftoverTempDirs(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	left := filepath.Join(dataDir, TempPrefix+"123", "maildir", "a")
	if err := os.MkdirAll(left, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(left, "1."), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dataDir, "keep-me")
	if err := os.Mkdir(other, 0o750); err != nil {
		t.Fatal(err)
	}

	f, _ := newTestFetcher(t, dataDir, "http://127.0.0.1:1/unused", nil)
	assertNoTempDirs(t, dataDir)
	if _, err := os.Stat(other); err != nil {
		t.Errorf("unrelated directory removed: %v", err)
	}
	if p := f.Progress(); p.State != StateAbsent {
		t.Errorf("state = %q, want absent (a leftover temp dir is not a dataset)", p.State)
	}
}

func TestProgress_FollowsTheFilesystem(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	f, _ := newTestFetcher(t, dataDir, "http://127.0.0.1:1/unused", nil)
	maildir := filepath.Join(dataDir, MaildirName)

	if p := f.Progress(); p.State != StateAbsent || p.BytesTotal != -1 || p.Path != maildir {
		t.Errorf("progress = %+v, want absent", p)
	}
	if err := os.Mkdir(maildir, 0o750); err != nil {
		t.Fatal(err)
	}
	if p := f.Progress(); p.State != StateReady {
		t.Errorf("state = %q after the maildir appeared, want ready", p.State)
	}
	if err := os.Remove(maildir); err != nil {
		t.Fatal(err)
	}
	if p := f.Progress(); p.State != StateAbsent {
		t.Errorf("state = %q after the maildir disappeared, want absent", p.State)
	}
	// A regular file named maildir is not a dataset.
	if err := os.WriteFile(maildir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if p := f.Progress(); p.State != StateAbsent {
		t.Errorf("state = %q with a file named maildir, want absent", p.State)
	}
}

func TestNew_SharedRegistryDoesNotPanic(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	_, reg := newTestFetcher(t, dataDir, "http://127.0.0.1:1/unused", nil)
	f2 := New(dataDir, "http://127.0.0.1:1/unused", nil, nil, reg)
	t.Cleanup(func() { _ = f2.Close(context.Background()) })
	if n, err := testutil.GatherAndCount(reg, "enron_fetch_state"); err != nil || n != len(States) {
		t.Errorf("enron_fetch_state series = %d (%v), want %d", n, err, len(States))
	}
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestFetch_LogsProgressAndOutcome(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	archive := sampleArchive(t)
	srv := newArchiveServer(t, serveBytes(archive))
	var logs syncBuffer
	f := New(dataDir, srv.URL, srv.Client(), slog.New(slog.NewJSONHandler(&logs, nil)), nil,
		WithMinFreeBytes(0), WithProgressLogInterval(time.Nanosecond))
	t.Cleanup(func() { _ = f.Close(context.Background()) })

	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForState(t, f, StateReady)
	sum := sha256.Sum256(archive)
	for _, want := range []string{
		`"msg":"fetch started"`,
		`"msg":"fetch progress"`,
		`"msg":"fetch completed"`,
		`"sha256":"` + hex.EncodeToString(sum[:]) + `"`,
		`"bytes":` + strconv.Itoa(len(archive)),
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs do not contain %s:\n%s", want, logs.String())
		}
	}
}

func TestFormatBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		n    uint64
		want string
	}{
		{0, "0B"},
		{1023, "1023B"},
		{1024, "1KiB"},
		{1536, "1.5KiB"},
		{4 << 30, "4GiB"},
		{1536 << 20, "1.5GiB"},
		{3 << 40, "3TiB"},
	}
	for _, tt := range tests {
		if got := formatBytes(tt.n); got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}
