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
	"sync/atomic"
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

func TestFetch_DiskChecks(t *testing.T) {
	t.Parallel()
	const plenty = 1 << 40
	tests := []struct {
		name      string
		stats     diskStats
		statErr   error
		wantError string // empty: the fetch succeeds.
	}{
		{
			name:      "insufficient space",
			stats:     diskStats{freeBytes: 1536 << 20, freeInodes: plenty, inodesKnown: true},
			wantError: "insufficient disk space: need 4GiB, have 1.5GiB",
		},
		{
			name:      "insufficient inodes",
			stats:     diskStats{freeBytes: plenty, freeInodes: 100_000, inodesKnown: true},
			wantError: "insufficient free inodes: need 530000, have 100000 (the dataset has about 521k files and directories)",
		},
		{
			name:  "inodes not reported by the filesystem",
			stats: diskStats{freeBytes: plenty, freeInodes: 0, inodesKnown: false},
		},
		{
			name:  "enough of both",
			stats: diskStats{freeBytes: DefaultMinFreeBytes, freeInodes: DefaultMinFreeInodes, inodesKnown: true},
		},
		{
			name:    "platform without statfs",
			statErr: errDiskStatsUnsupported,
		},
		{
			name:      "statfs fails",
			statErr:   errors.New("EIO"),
			wantError: "checking free disk space: EIO",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			srv := newArchiveServer(t, serveBytes(sampleArchive(t)))
			f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client(),
				WithMinFreeBytes(DefaultMinFreeBytes), WithMinFreeInodes(DefaultMinFreeInodes))
			f.statDisk = func(string) (diskStats, error) { return tt.stats, tt.statErr }

			if _, err := f.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			if tt.wantError == "" {
				waitForState(t, f, StateReady)
				return
			}
			p := waitForState(t, f, StateFailed)
			if p.Error != tt.wantError {
				t.Errorf("error = %q, want %q", p.Error, tt.wantError)
			}
			if n := srv.requests.Load(); n != 0 {
				t.Errorf("requests = %d, want 0 (checks run before downloading)", n)
			}
			assertNoTempDirs(t, dataDir)
		})
	}
}

func TestStatDisk_ReportsTheRealFilesystem(t *testing.T) {
	t.Parallel()
	st, err := statDisk(t.TempDir())
	if errors.Is(err, errDiskStatsUnsupported) {
		t.Skip("no statfs on this platform")
	}
	if err != nil {
		t.Fatalf("statDisk: %v", err)
	}
	if st.freeBytes == 0 {
		t.Errorf("freeBytes = 0 for the test temp dir: %+v", st)
	}
	if st.inodesKnown && st.freeInodes == 0 {
		t.Errorf("inodes reported but none free: %+v", st)
	}
}

func TestFetch_RealDiskChecksPass(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	srv := newArchiveServer(t, serveBytes(sampleArchive(t)))
	f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client(), WithMinFreeBytes(1), WithMinFreeInodes(1))
	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForState(t, f, StateReady)
}

// Scenario: Fetch rejects a symlinked maildir (#45). A symlink is never
// followed, whatever it points to, so /fetch and /start agree.
func TestFetch_MaildirIsNotADirectory(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		make func(t *testing.T, maildir string)
		// wantMsg is a substring of the error.
		wantMsg string
	}{
		{
			name: "file",
			make: func(t *testing.T, maildir string) {
				t.Helper()
				if err := os.WriteFile(maildir, []byte("oops"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			wantMsg: "mode -rw-------",
		},
		{
			name:    "symlink to a directory",
			make:    func(t *testing.T, maildir string) { t.Helper(); symlink(t, t.TempDir(), maildir) },
			wantMsg: "is a symlink",
		},
		{
			name: "symlink to a file",
			make: func(t *testing.T, maildir string) {
				t.Helper()
				target := filepath.Join(t.TempDir(), "file")
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				symlink(t, target, maildir)
			},
			wantMsg: "is a symlink",
		},
		{
			name:    "dangling symlink",
			make:    func(t *testing.T, maildir string) { t.Helper(); symlink(t, "/nonexistent-target", maildir) },
			wantMsg: "is a symlink",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			maildir := filepath.Join(dataDir, MaildirName)
			tt.make(t, maildir)
			before, err := os.Lstat(maildir)
			if err != nil {
				t.Fatal(err)
			}
			srv := newArchiveServer(t, serveBytes(sampleArchive(t)))
			f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client())

			// The startup state reports the problem, not ready.
			if p := f.Progress(); p.State != StateAbsent || !strings.Contains(p.Error, tt.wantMsg) {
				t.Errorf("startup progress = %q %q, want absent with an error containing %q", p.State, p.Error, tt.wantMsg)
			}

			p, err := f.Start()
			if !errors.Is(err, ErrMaildirNotDirectory) {
				t.Fatalf("Start error = %v, want ErrMaildirNotDirectory", err)
			}
			if !strings.Contains(err.Error(), maildir) || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Errorf("error %q does not name the path and %q", err, tt.wantMsg)
			}
			if p.State != StateAbsent || p.Error != err.Error() {
				t.Errorf("progress = %q %q, want absent with the error", p.State, p.Error)
			}
			if n := srv.requests.Load(); n != 0 {
				t.Errorf("requests = %d, want 0", n)
			}
			if got := testutil.ToFloat64(f.metrics.state.WithLabelValues(string(StateAbsent))); got != 1 {
				t.Errorf("state metric absent = %v, want 1", got)
			}
			assertNoTempDirs(t, dataDir)
			after, err := os.Lstat(maildir) // left untouched.
			if err != nil || !os.SameFile(before, after) {
				t.Errorf("maildir was replaced or removed: %v", err)
			}
		})
	}
}

// Once the bad maildir is removed, the error clears; once it is replaced by
// a real directory, the dataset is ready.
func TestFetch_SymlinkedMaildirRecovers(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	maildir := filepath.Join(dataDir, MaildirName)
	symlink(t, t.TempDir(), maildir)
	f, _ := newTestFetcher(t, dataDir, "http://127.0.0.1:0/unused", nil)
	if p := f.Progress(); p.State != StateAbsent || p.Error == "" {
		t.Fatalf("progress = %+v, want absent with an error", p)
	}

	if err := os.Remove(maildir); err != nil {
		t.Fatal(err)
	}
	if p := f.Progress(); p.State != StateAbsent || p.Error != "" {
		t.Errorf("after removing the symlink: progress = %+v, want absent without an error", p)
	}

	symlink(t, t.TempDir(), maildir)
	if p := f.Progress(); p.State != StateAbsent || p.Error == "" {
		t.Errorf("after a new symlink: progress = %+v, want absent with an error", p)
	}
	if err := os.Remove(maildir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(maildir, 0o750); err != nil {
		t.Fatal(err)
	}
	if p := f.Progress(); p.State != StateReady || p.Error != "" {
		t.Errorf("after a real directory: progress = %+v, want ready", p)
	}
}

func symlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
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
	// Glob metacharacters in DATA_DIR must neither break nor widen the match.
	for _, name := range []string{"data", "data[1]", "da*ta?", `data\[x`} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dataDir := filepath.Join(t.TempDir(), name)
			for _, d := range []string{TempPrefix + "123", TempPrefix + "abc"} {
				left := filepath.Join(dataDir, d, "maildir", "a")
				if err := os.MkdirAll(left, 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(left, "1."), []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			keep := []string{"keep-me", "maildir", "x" + TempPrefix + "1"}
			for _, d := range keep {
				if err := os.Mkdir(filepath.Join(dataDir, d), 0o750); err != nil {
					t.Fatal(err)
				}
			}
			// A regular file with the prefix is not ours (we only create directories).
			if err := os.WriteFile(filepath.Join(dataDir, TempPrefix+"file"), nil, 0o600); err != nil {
				t.Fatal(err)
			}

			f, _ := newTestFetcher(t, dataDir, "http://127.0.0.1:1/unused", nil)
			for _, d := range []string{TempPrefix + "123", TempPrefix + "abc"} {
				assertNotExist(t, filepath.Join(dataDir, d))
			}
			for _, d := range append(keep, TempPrefix+"file") {
				if _, err := os.Stat(filepath.Join(dataDir, d)); err != nil {
					t.Errorf("unrelated entry %q removed: %v", d, err)
				}
			}
			if p := f.Progress(); p.State != StateReady {
				t.Errorf("state = %q, want ready (maildir exists)", p.State)
			}
		})
	}
}

func TestNew_LeftoverCleanupWithMissingDataDir(t *testing.T) {
	t.Parallel()
	dataDir := filepath.Join(t.TempDir(), "does-not-exist")
	f, _ := newTestFetcher(t, dataDir, "http://127.0.0.1:1/unused", nil)
	if p := f.Progress(); p.State != StateAbsent {
		t.Errorf("state = %q, want absent", p.State)
	}
	assertNotExist(t, dataDir) // New does not create the directory.
}

func TestNewHTTPClient_DoesNotRequestCompression(t *testing.T) {
	t.Parallel()
	archive := sampleArchive(t)
	var acceptEncoding atomic.Value
	srv := newArchiveServer(t, func(w http.ResponseWriter, r *http.Request) {
		acceptEncoding.Store(r.Header.Get("Accept-Encoding"))
		serveBytes(archive)(w, r)
	})
	dataDir := t.TempDir()
	f, _ := newTestFetcher(t, dataDir, srv.URL, nil) // nil: the production client.
	if _, err := f.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	p := waitForState(t, f, StateReady)
	if got := acceptEncoding.Load(); got != "" {
		t.Errorf("Accept-Encoding = %q, want none (the archive must arrive byte for byte)", got)
	}
	if p.BytesDownloaded != int64(len(archive)) {
		t.Errorf("bytes = %d, want %d", p.BytesDownloaded, len(archive))
	}
	if tr, ok := NewHTTPClient().Transport.(*http.Transport); !ok || !tr.DisableCompression || !tr.DisableKeepAlives {
		t.Errorf("transport = %+v, want DisableCompression and DisableKeepAlives", tr)
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
