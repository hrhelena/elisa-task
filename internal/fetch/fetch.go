package fetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hrhelena/elisa-task/internal/transient"
)

// State is the lifecycle state of the dataset.
type State string

// Dataset states.
const (
	// StateAbsent means there is no usable dataset and no fetch has run. If
	// $DATA_DIR/maildir exists but cannot be used (a file, a symlink, which
	// is never followed, or an unreadable directory), Error says so.
	StateAbsent State = "absent"
	// StateDownloading means the archive is being downloaded and, because
	// extraction is streamed, extracted at the same time.
	StateDownloading State = "downloading"
	// StateExtracting means the whole archive has been received and the
	// fetch is finishing (gzip trailer check, atomic rename, marker).
	StateExtracting State = "extracting"
	// StateReady means $DATA_DIR/maildir is a readable directory (not a symlink).
	StateReady State = "ready"
	// StateFailed means the last fetch failed; Error says why.
	StateFailed State = "failed"
)

// States lists every state, in lifecycle order.
var States = []State{StateAbsent, StateDownloading, StateExtracting, StateReady, StateFailed}

// File and directory names inside the data directory.
const (
	// MaildirName is the dataset directory inside the data directory.
	MaildirName = "maildir"
	// MarkerName is the marker written after a successful fetch.
	MarkerName = ".fetch-complete"
	// TempPrefix prefixes the per-fetch temporary extraction directories.
	TempPrefix = ".fetch-tmp-"
)

// Defaults for the tunables (ticket #10).
const (
	// DefaultStallTimeout aborts a download that receives no bytes for this long.
	DefaultStallTimeout = 60 * time.Second
	// DefaultMinFreeBytes is the free space required in the data directory
	// before a download starts (the extracted dataset is about 2.3 GB).
	DefaultMinFreeBytes uint64 = 4 << 30
	// DefaultMinFreeInodes is the number of free inodes required before a
	// download starts: the dataset has 520,901 entries (517,401 files and
	// 3,500 directories), plus about 2% headroom. Small ext4 volumes can run
	// out of inodes long before they run out of bytes.
	DefaultMinFreeInodes uint64 = 530_000
	// DefaultProgressLogInterval is how often download progress is logged.
	DefaultProgressLogInterval = 10 * time.Second
)

var (
	// ErrClosed is returned by Start after Close.
	ErrClosed = errors.New("fetcher is closed")
	// ErrDataDirNotWritable is returned by Start when the data directory
	// cannot be created or written to.
	ErrDataDirNotWritable = errors.New("data directory is not writable")
	// ErrMaildirNotDirectory is returned by Start when $DATA_DIR/maildir
	// exists but is not a directory, so a fetch could never install the
	// dataset. A symlink counts as not a directory, whatever it points to:
	// ingestion does not follow it either (#45).
	ErrMaildirNotDirectory = errors.New("maildir path exists and is not a directory")
	// ErrMaildirUnreadable is returned by Start when $DATA_DIR/maildir is a
	// directory the service cannot open: it is not a usable dataset
	// (ingestion refuses it too), and a fetch could not replace it.
	ErrMaildirUnreadable = errors.New("maildir directory is not readable")
	// ErrTransient is returned by Start when $DATA_DIR/maildir could not be
	// checked because of a transient system error (out of file descriptors
	// or memory, an interrupted call). It says nothing about the maildir:
	// the dataset state is left as it was, and the caller should retry
	// (POST /fetch answers 503 unavailable, #49).
	ErrTransient = errors.New("checking the maildir failed temporarily")
	// errStalled is the cancel cause of a download that stopped receiving bytes.
	errStalled = errors.New("download stalled")
)

// Progress is a snapshot of the dataset state and of the current or last fetch.
type Progress struct {
	State State
	// Path is the maildir path, $DATA_DIR/maildir.
	Path            string
	BytesDownloaded int64
	// BytesTotal comes from Content-Length; -1 if unknown.
	BytesTotal     int64
	FilesExtracted int64
	// Error is set when State is StateFailed, or when State is StateAbsent
	// because $DATA_DIR/maildir exists but cannot be used.
	Error      string
	StartedAt  *time.Time
	FinishedAt *time.Time
}

// Marker is the content of the $DATA_DIR/.fetch-complete file.
type Marker struct {
	URL        string    `json:"url"`
	SHA256     string    `json:"sha256"`
	Bytes      int64     `json:"bytes"`
	Files      int64     `json:"files"`
	FinishedAt time.Time `json:"finished_at"`
}

// Option customizes a Fetcher.
type Option func(*Fetcher)

// WithStallTimeout sets how long a download may receive no bytes before it
// is aborted.
func WithStallTimeout(d time.Duration) Option {
	return func(f *Fetcher) { f.stallTimeout = d }
}

// WithMinFreeBytes sets the free space the data directory must have before a
// download starts. Zero disables the check.
func WithMinFreeBytes(n uint64) Option {
	return func(f *Fetcher) { f.minFreeBytes = n }
}

// WithMinFreeInodes sets the free inodes the data directory must have before
// a download starts, where the platform reports inodes. Zero disables the check.
func WithMinFreeInodes(n uint64) Option {
	return func(f *Fetcher) { f.minFreeInodes = n }
}

// WithProgressLogInterval sets how often download progress is logged.
func WithProgressLogInterval(d time.Duration) Option {
	return func(f *Fetcher) { f.logInterval = d }
}

// Fetcher downloads the dataset archive and extracts it into the data
// directory. It runs at most one fetch at a time; it is safe for concurrent use.
type Fetcher struct {
	dataDir string
	url     string
	client  *http.Client
	logger  *slog.Logger
	metrics *metrics

	stallTimeout  time.Duration
	minFreeBytes  uint64
	minFreeInodes uint64
	logInterval   time.Duration
	// statDisk reports free space and inodes; replaced in tests.
	statDisk func(dir string) (diskStats, error)
	// lstat and openDir check the maildir (os.Lstat and os.Open); replaced
	// in tests to inject errors.
	lstat   func(name string) (fs.FileInfo, error)
	openDir func(name string) (*os.File, error)

	// baseCtx is the service-lifetime context fetches run under (never a
	// request context); Close cancels it.
	baseCtx    context.Context //nolint:containedctx // owner of background work, canceled by Close.
	baseCancel context.CancelFunc

	bytesDownloaded atomic.Int64
	filesExtracted  atomic.Int64

	mu       sync.Mutex
	progress Progress      // guarded by mu; the counters above are merged in on read.
	done     chan struct{} // closed when the in-flight fetch ends; nil if none ran.
	closed   bool
}

// New returns a Fetcher that downloads url into dataDir/maildir. A nil client
// means NewHTTPClient(); a nil reg means metrics are not registered. Leftover
// temporary directories from an interrupted fetch are removed.
func New(dataDir, url string, client *http.Client, logger *slog.Logger, reg prometheus.Registerer, opts ...Option) *Fetcher {
	if client == nil {
		client = NewHTTPClient()
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &Fetcher{
		dataDir:       dataDir,
		url:           url,
		client:        client,
		logger:        logger.With(slog.String("component", "fetch")),
		stallTimeout:  DefaultStallTimeout,
		minFreeBytes:  DefaultMinFreeBytes,
		minFreeInodes: DefaultMinFreeInodes,
		logInterval:   DefaultProgressLogInterval,
		statDisk:      statDisk,
		lstat:         os.Lstat,
		openDir:       os.Open,
		baseCtx:       ctx,
		baseCancel:    cancel,
	}
	for _, opt := range opts {
		opt(f)
	}
	f.metrics = newMetrics(reg, f.logger)
	f.removeLeftovers()
	f.progress = Progress{State: StateAbsent, Path: f.maildirPath(), BytesTotal: -1}
	_ = f.refreshLocked()
	return f
}

// Start starts a fetch unless the dataset is already present or a fetch is
// in progress, and returns the resulting progress. It never waits for the
// download. It returns ErrDataDirNotWritable if the data directory cannot be
// written, ErrMaildirNotDirectory if $DATA_DIR/maildir exists but is not a
// directory (including a symlink to one), ErrMaildirUnreadable if it is a
// directory that cannot be opened, ErrTransient if it could not be checked
// right now, and ErrClosed after Close.
func (f *Fetcher) Start() (Progress, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return f.snapshotLocked(), ErrClosed
	}
	if f.inFlightLocked() {
		return f.snapshotLocked(), nil
	}
	// One check decides both the state and the answer, so a 409 always
	// comes with a snapshot whose error matches it.
	err := f.refreshLocked()
	if err == nil {
		return f.snapshotLocked(), nil
	}
	if errors.Is(err, ErrTransient) {
		f.logger.Warn("cannot fetch now: checking the maildir failed temporarily",
			slog.String("path", f.maildirPath()), slog.Any("err", err))
		return f.snapshotLocked(), err
	}
	if unusable(err) {
		f.logger.Error("cannot fetch: maildir path exists and is not usable",
			slog.String("path", f.maildirPath()), slog.Any("err", err))
		return f.snapshotLocked(), err
	}

	tmp, err := f.makeTempDir()
	if err != nil {
		f.logger.Error("data directory is not writable", slog.String("data_dir", f.dataDir), slog.Any("err", err))
		return f.snapshotLocked(), fmt.Errorf("%w: %w", ErrDataDirNotWritable, err)
	}

	now := time.Now().UTC()
	f.bytesDownloaded.Store(0)
	f.filesExtracted.Store(0)
	f.progress = Progress{State: StateDownloading, Path: f.maildirPath(), BytesTotal: -1, StartedAt: &now}
	f.metrics.setState(StateDownloading)
	f.done = make(chan struct{})

	ctx, cancel := context.WithCancel(f.baseCtx)
	go f.run(ctx, cancel, tmp, f.done)

	f.logger.Info("fetch started", slog.String("url", f.url), slog.String("data_dir", f.dataDir))
	return f.snapshotLocked(), nil
}

// Progress returns the current progress. When no fetch is running, the state
// is re-derived from the filesystem, so a maildir that appears or disappears
// out of band is reflected.
func (f *Fetcher) Progress() Progress {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.inFlightLocked() {
		_ = f.refreshLocked()
	}
	return f.snapshotLocked()
}

// Close cancels an in-flight fetch and waits until it has cleaned up its
// temporary directory, or until ctx is done. Start fails with ErrClosed afterwards.
func (f *Fetcher) Close(ctx context.Context) error {
	f.mu.Lock()
	f.closed = true
	done := f.done
	f.mu.Unlock()

	f.baseCancel()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for the fetch to stop: %w", ctx.Err())
	}
}

func (f *Fetcher) maildirPath() string { return filepath.Join(f.dataDir, MaildirName) }

func (f *Fetcher) inFlightLocked() bool {
	return f.progress.State == StateDownloading || f.progress.State == StateExtracting
}

// refreshLocked derives the state from the filesystem when no fetch runs:
// a readable maildir directory means ready (this covers a pre-extracted
// local copy without a marker); a vanished maildir turns ready back into
// absent; a maildir that exists but cannot be used (a file, a symlink to
// anything, an unreadable directory) is absent with an error that says so.
// A failed state is kept so its error stays visible, and a transient error
// changes nothing: it says nothing about the maildir (#49). It returns the
// result of checkMaildir.
func (f *Fetcher) refreshLocked() error {
	err := f.checkMaildir()
	switch {
	case errors.Is(err, ErrTransient):
		return err
	case err == nil:
		if f.progress.State == StateReady {
			return nil
		}
		f.progress.State = StateReady
		f.progress.Error = ""
	case f.progress.State == StateFailed:
		return err
	default:
		want := Progress{State: StateAbsent, Path: f.maildirPath(), BytesTotal: -1}
		if unusable(err) {
			want.Error = err.Error()
		}
		if f.progress.State == StateAbsent && f.progress.Error == want.Error {
			return err
		}
		f.progress = want
	}
	f.metrics.setState(f.progress.State)
	return err
}

func (f *Fetcher) snapshotLocked() Progress {
	p := f.progress
	if f.inFlightLocked() {
		p.BytesDownloaded = f.bytesDownloaded.Load()
		p.FilesExtracted = f.filesExtracted.Load()
	}
	return p
}

func (f *Fetcher) setStateLocked(s State) {
	f.progress.State = s
	f.metrics.setState(s)
}

// checkMaildir returns nil if the maildir is a directory the service can
// open, an error wrapping ErrMaildirNotDirectory if it exists but is
// something else, one wrapping ErrMaildirUnreadable if it is a directory that
// cannot be opened, one wrapping ErrTransient if a transient system error
// prevented the check, and the Lstat error otherwise. It uses Lstat: a
// symlink is never followed. Ingestion applies the same rules, so /fetch,
// /status and /start agree (#45).
func (f *Fetcher) checkMaildir() error {
	path := f.maildirPath()
	fi, err := f.lstat(path)
	switch {
	case transient.Is(err):
		return fmt.Errorf("%w: %w", ErrTransient, err)
	case err != nil:
		return err //nolint:wrapcheck // *fs.PathError already names the path.
	case fi.Mode()&fs.ModeSymlink != 0:
		return fmt.Errorf("%w: %s is a symlink, which is not followed", ErrMaildirNotDirectory, path)
	case !fi.IsDir():
		return fmt.Errorf("%w: %s (mode %s)", ErrMaildirNotDirectory, path, fi.Mode())
	}
	d, err := f.openDir(path)
	if transient.Is(err) {
		return fmt.Errorf("%w: %w", ErrTransient, err)
	}
	if err != nil {
		cause := err
		if pe, ok := errors.AsType[*fs.PathError](err); ok {
			cause = pe.Err
		}
		return fmt.Errorf("%w: %s: %w", ErrMaildirUnreadable, path, cause)
	}
	_ = d.Close() // read-only directory handle
	return nil
}

// unusable reports whether err from checkMaildir means the maildir exists
// but cannot be used.
func unusable(err error) bool {
	return errors.Is(err, ErrMaildirNotDirectory) || errors.Is(err, ErrMaildirUnreadable)
}

// makeTempDir creates the data directory if needed and a fresh temporary
// extraction directory inside it, which also proves the directory is writable.
func (f *Fetcher) makeTempDir() (string, error) {
	if err := os.MkdirAll(f.dataDir, 0o755); err != nil { //nolint:gosec // dataset dirs are world-readable by design.
		return "", fmt.Errorf("creating data directory: %w", err)
	}
	tmp, err := os.MkdirTemp(f.dataDir, TempPrefix)
	if err != nil {
		return "", fmt.Errorf("creating temporary directory: %w", err)
	}
	return tmp, nil
}

// removeLeftovers deletes temporary directories left by a fetch that was
// interrupted by a crash or a kill (ticket #10: removed at startup). It lists
// the data directory and matches the prefix literally, so glob characters in
// DATA_DIR (for example "[") cannot break or widen the match.
func (f *Fetcher) removeLeftovers() {
	entries, err := os.ReadDir(f.dataDir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			f.logger.Warn("listing the data directory for leftovers failed",
				slog.String("data_dir", f.dataDir), slog.Any("err", err))
		}
		return
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), TempPrefix) {
			continue
		}
		path := filepath.Join(f.dataDir, e.Name())
		if err := os.RemoveAll(path); err != nil {
			f.logger.Warn("removing leftover temporary directory failed", slog.String("path", path), slog.Any("err", err))
			continue
		}
		f.logger.Info("removed leftover temporary directory", slog.String("path", path))
	}
}

// run performs one fetch and records its outcome. It owns tmp and removes it.
func (f *Fetcher) run(ctx context.Context, cancel context.CancelFunc, tmp string, done chan struct{}) {
	defer close(done)
	defer cancel()

	start := time.Now()
	res, err := f.fetch(ctx, tmp)
	if rmErr := os.RemoveAll(tmp); rmErr != nil {
		f.logger.Warn("removing temporary directory failed", slog.String("path", tmp), slog.Any("err", rmErr))
	}
	elapsed := time.Since(start)

	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now().UTC()
	f.progress.FinishedAt = &now
	f.progress.BytesDownloaded = f.bytesDownloaded.Load()
	f.progress.FilesExtracted = f.filesExtracted.Load()

	if err != nil {
		f.progress.Error = err.Error()
		f.setStateLocked(StateFailed)
		f.metrics.observeDuration(StateFailed, elapsed)
		f.logger.Error("fetch failed",
			slog.String("url", f.url), slog.Any("err", err), slog.Duration("elapsed", elapsed),
			slog.Int64("bytes", f.progress.BytesDownloaded), slog.Int64("files", f.progress.FilesExtracted))
		return
	}
	f.progress.Error = ""
	f.setStateLocked(StateReady)
	f.metrics.observeDuration(StateReady, elapsed)
	f.logger.Info("fetch completed",
		slog.String("url", f.url), slog.String("path", f.maildirPath()), slog.Duration("elapsed", elapsed),
		slog.Int64("bytes", res.bytes), slog.String("sha256", res.sha256),
		slog.Int64("files", res.files), slog.Any("skipped", res.skipped))
}

// result describes a successful fetch.
type result struct {
	bytes   int64
	sha256  string
	files   int64
	skipped map[string]int64
}

// fetch downloads and extracts the archive into tmp, then moves the maildir
// into place and writes the marker.
func (f *Fetcher) fetch(ctx context.Context, tmp string) (result, error) {
	if err := f.checkDisk(); err != nil {
		return result{}, err
	}

	// Stall detection: the timer cancels the download when no bytes arrive
	// for stallTimeout. It also covers connecting and waiting for headers.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stall := time.AfterFunc(f.stallTimeout, func() { cancel(errStalled) })
	defer stall.Stop()

	body, total, err := f.get(ctx)
	if err != nil {
		return result{}, f.describe(ctx, err)
	}
	defer func() { _ = body.Close() }()
	f.setBytesTotal(total)

	hash := sha256.New()
	cr := &countingReader{r: body, f: f, stall: stall, stallTimeout: f.stallTimeout, total: total, lastLog: time.Now()}
	ex := newExtractor(tmp, f.logger, f.metrics, &f.filesExtracted)
	if err := ex.extract(io.TeeReader(cr, hash)); err != nil {
		return result{}, f.describe(ctx, err)
	}
	stall.Stop()

	f.mu.Lock()
	f.setStateLocked(StateExtracting)
	f.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return result{}, f.describe(ctx, err)
	}
	if err := os.Rename(filepath.Join(tmp, MaildirName), f.maildirPath()); err != nil {
		return result{}, fmt.Errorf("moving maildir into place: %w", err)
	}

	res := result{
		bytes:   cr.n,
		sha256:  hex.EncodeToString(hash.Sum(nil)),
		files:   f.filesExtracted.Load(),
		skipped: ex.skipped,
	}
	// The maildir is complete and in place; a missing marker only loses the
	// download metadata, because a present maildir counts as ready.
	if err := f.writeMarker(res); err != nil {
		f.logger.Warn("writing fetch marker failed", slog.Any("err", err))
	}
	return res, nil
}

// get issues the download request and returns the body and Content-Length.
func (f *Fetcher) get(ctx context.Context) (io.ReadCloser, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, http.NoBody)
	if err != nil {
		return nil, 0, fmt.Errorf("building request: %w", err)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("download failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_ = resp.Body.Close()
		return nil, 0, fmt.Errorf("download failed: HTTP %d", resp.StatusCode)
	}
	return resp.Body, resp.ContentLength, nil
}

// describe replaces errors caused by cancellation with their cause, so the
// reported error says "stalled" or "canceled" rather than a read error.
func (f *Fetcher) describe(ctx context.Context, err error) error {
	switch cause := context.Cause(ctx); {
	case cause == nil:
		return err
	case errors.Is(cause, errStalled):
		return fmt.Errorf("download stalled: no data received for %s", f.stallTimeout)
	default:
		return fmt.Errorf("fetch canceled: %w", cause)
	}
}

// diskStats describes the free capacity of a filesystem.
type diskStats struct {
	freeBytes  uint64
	freeInodes uint64
	// inodesKnown is false when the filesystem reports no inode limit
	// (statfs f_files == 0); the inode check is then skipped.
	inodesKnown bool
}

// checkDisk fails fast, before any byte is downloaded, when the data
// directory lacks the space or the inodes for the extracted dataset.
func (f *Fetcher) checkDisk() error {
	if f.minFreeBytes == 0 && f.minFreeInodes == 0 {
		return nil
	}
	st, err := f.statDisk(f.dataDir)
	if err != nil {
		if errors.Is(err, errDiskStatsUnsupported) {
			return nil
		}
		return fmt.Errorf("checking free disk space: %w", err)
	}
	if st.freeBytes < f.minFreeBytes {
		return fmt.Errorf("insufficient disk space: need %s, have %s", formatBytes(f.minFreeBytes), formatBytes(st.freeBytes))
	}
	if st.inodesKnown && st.freeInodes < f.minFreeInodes {
		return fmt.Errorf("insufficient free inodes: need %d, have %d (the dataset has about 521k files and directories)",
			f.minFreeInodes, st.freeInodes)
	}
	return nil
}

func (f *Fetcher) setBytesTotal(total int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.progress.BytesTotal = total
}

// writeMarker writes the marker atomically (temp file, then rename).
func (f *Fetcher) writeMarker(res result) error {
	b, err := json.MarshalIndent(Marker{
		URL:        f.url,
		SHA256:     res.sha256,
		Bytes:      res.bytes,
		Files:      res.files,
		FinishedAt: time.Now().UTC(),
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding marker: %w", err)
	}
	path := filepath.Join(f.dataDir, MarkerName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil { //nolint:gosec // not secret.
		return fmt.Errorf("writing marker: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("renaming marker: %w", err)
	}
	return nil
}

// countingReader counts downloaded bytes, feeds the stall timer and the
// metrics, and logs progress periodically.
type countingReader struct {
	r            io.Reader
	f            *Fetcher
	stall        *time.Timer
	stallTimeout time.Duration
	total        int64
	n            int64
	lastLog      time.Time
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.stall.Reset(c.stallTimeout)
		c.n += int64(n)
		c.f.bytesDownloaded.Store(c.n)
		c.f.metrics.bytes.Add(float64(n))
		if now := time.Now(); now.Sub(c.lastLog) >= c.f.logInterval {
			c.lastLog = now
			c.f.logger.Info("fetch progress",
				slog.Int64("bytes_downloaded", c.n), slog.Int64("bytes_total", c.total),
				slog.Int64("files_extracted", c.f.filesExtracted.Load()))
		}
	}
	return n, err //nolint:wrapcheck // io.Reader contract: io.EOF must not be wrapped.
}

// formatBytes renders n with a binary unit, e.g. 4GiB or 512.0MiB.
func formatBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit && exp < 5; m /= unit {
		div *= unit
		exp++
	}
	suffix := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}[exp]
	if n%div == 0 {
		return fmt.Sprintf("%d%s", n/div, suffix)
	}
	return fmt.Sprintf("%.1f%s", float64(n)/float64(div), suffix)
}
