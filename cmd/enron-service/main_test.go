package main

import (
	"bufio"
	"bytes"
	"context"
	"debug/elf"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"

	"github.com/hrhelena/elisa-task/internal/config"
	"github.com/hrhelena/elisa-task/internal/httpapi"
	"github.com/hrhelena/elisa-task/internal/store"
)

// testEnvSetup, when set (by the integration-tagged file), prepares shared
// test infrastructure before the tests and returns its teardown.
var testEnvSetup func() (teardown func(), err error)

func TestMain(m *testing.M) {
	teardown := func() {}
	if testEnvSetup != nil {
		var err error
		if teardown, err = testEnvSetup(); err != nil {
			fmt.Fprintln(os.Stderr, "test setup:", err)
			os.Exit(1)
		}
	}
	code := m.Run()
	teardown()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	if code == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintln(os.Stderr, "goleak:", err)
			code = 1
		}
	}
	os.Exit(code)
}

// decodeLogLines parses newline-delimited JSON log output.
func decodeLogLines(t *testing.T, out []byte) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for line := range bytes.Lines(out) {
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		lines = append(lines, rec)
	}
	return lines
}

// findLog returns the first log record with msg, or nil.
func findLog(lines []map[string]any, msg string) map[string]any {
	for _, rec := range lines {
		if rec["msg"] == msg {
			return rec
		}
	}
	return nil
}

func envLookup(env map[string]string) config.LookupFunc {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

// fakeStore stands in for Postgres in run-level tests.
type fakeStore struct {
	checkErr atomic.Pointer[error] // nil -> healthy
	closed   atomic.Bool
	topLimit atomic.Int64 // limit of the last TopSenders call
}

func (*fakeStore) Name() string { return "postgres" }

func (f *fakeStore) Check(context.Context) error {
	if p := f.checkErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (f *fakeStore) Close() { f.closed.Store(true) }

func (f *fakeStore) setCheckErr(err error) { f.checkErr.Store(&err) }

// TopSenders returns one fixed sender and records the limit it was asked for.
func (f *fakeStore) TopSenders(_ context.Context, limit int) ([]store.SenderCount, error) {
	f.topLimit.Store(int64(limit))
	return []store.SenderCount{{Email: "kay.mann@enron.com", Count: 5}}, nil
}

// fakeOpener returns an openStoreFunc that hands out st (or fails with
// openErr) and records the URL it was called with.
type fakeOpener struct {
	st      *fakeStore
	openErr error
	mu      sync.Mutex
	url     string
	calls   int
}

func newFakeOpener() *fakeOpener { return &fakeOpener{st: &fakeStore{}} }

func (o *fakeOpener) open(ctx context.Context, url string, _ *slog.Logger) (dbStore, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.url, o.calls = url, o.calls+1
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("fake opener: open context has no deadline")
	}
	if o.openErr != nil {
		return nil, o.openErr
	}
	return o.st, nil
}

// newClient returns an HTTP client without keep-alives, so no idle
// connection goroutines outlive a test (goleak).
func newClient() *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 10 * time.Second}
}

func get(t *testing.T, client *http.Client, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return resp.StatusCode, string(body)
}

// Scenario: Defaults allow a local run.
func TestDefaultsAllowALocalRun(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var out bytes.Buffer
	opener := newFakeOpener()
	code := run(ctx, envLookup(nil), &out, opener.open, func(net.Addr) { cancel() })

	lines := decodeLogLines(t, out.Bytes())
	if opener.url != config.DefaultDatabaseURL {
		t.Errorf("store opened with %q, want the default DATABASE_URL %q", opener.url, config.DefaultDatabaseURL)
	}
	if code != 0 {
		if rec := findLog(lines, "listen failed"); rec != nil && strings.Contains(rec["err"].(string), "address already in use") {
			t.Skipf("port 8080 is busy on this host: %v", rec["err"])
		}
		t.Fatalf("run exit code = %d, want 0; logs:\n%s", code, out.String())
	}
	rec := findLog(lines, "listening")
	if rec == nil {
		t.Fatalf(`no "listening" log line in:\n%s`, out.String())
	}
	if rec["addr"] != ":8080" || rec["level"] != "INFO" || rec["service"] != "enron-service" {
		t.Errorf("unexpected listening record: %v", rec)
	}
	if findLog(lines, "stopped") == nil {
		t.Errorf(`no "stopped" log line in:\n%s`, out.String())
	}
}

// Scenario: Invalid configuration fails fast (run level; see also
// TestBinaryInvalidConfigurationExitsWithStatus1).
func TestInvalidConfigurationFailsFast(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	listened := false
	opener := newFakeOpener()
	code := run(t.Context(), envLookup(map[string]string{"INGEST_WORKERS": "zero"}), &out, opener.open,
		func(net.Addr) { listened = true })
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if opener.calls != 0 {
		t.Error("store was opened despite invalid configuration")
	}
	if listened {
		t.Error("service started listening despite invalid configuration")
	}
	rec := findLog(decodeLogLines(t, out.Bytes()), "invalid configuration")
	if rec == nil || rec["level"] != "ERROR" || !strings.Contains(rec["err"].(string), "INGEST_WORKERS") {
		t.Errorf("want an ERROR log mentioning INGEST_WORKERS, got:\n%s", out.String())
	}
}

func TestRunListenFailureExitsWithStatus1(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	busy, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = busy.Close() }()

	var out bytes.Buffer
	opener := newFakeOpener()
	code := run(t.Context(), envLookup(map[string]string{"HTTP_ADDR": busy.Addr().String()}), &out, opener.open, nil)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if findLog(decodeLogLines(t, out.Bytes()), "listen failed") == nil {
		t.Errorf(`no "listen failed" log line in:\n%s`, out.String())
	}
	if !opener.st.closed.Load() {
		t.Error("store was not closed after the listen failure")
	}
}

// A database that cannot be opened (unreachable, or a failed migration)
// exits 1 before the server listens (#4).
func TestRunStoreOpenFailureExitsWithStatus1(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	listened := false
	opener := newFakeOpener()
	opener.openErr = errors.New("connecting to postgres: connection refused")
	code := run(t.Context(), envLookup(map[string]string{"HTTP_ADDR": "127.0.0.1:0", "DATABASE_URL": "postgres://db/x"}),
		&out, opener.open, func(net.Addr) { listened = true })
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if listened {
		t.Error("service started listening without a database")
	}
	if opener.url != "postgres://db/x" {
		t.Errorf("store opened with %q, want DATABASE_URL", opener.url)
	}
	rec := findLog(decodeLogLines(t, out.Bytes()), "opening database failed")
	if rec == nil || rec["level"] != "ERROR" || !strings.Contains(rec["err"].(string), "connection refused") {
		t.Errorf("want an ERROR log with the open error, got:\n%s", out.String())
	}
}

// startRun runs run with a fake store in the background and returns the
// base URL, the fake opener, a cancel func and run's exit code channel.
func startRun(t *testing.T, env map[string]string) (base string, opener *fakeOpener, cancel context.CancelFunc, done <-chan int, out *syncBuffer) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	addrCh := make(chan net.Addr, 1)
	exit := make(chan int, 1)
	out = &syncBuffer{}
	opener = newFakeOpener()
	go func() { exit <- run(ctx, envLookup(env), out, opener.open, func(a net.Addr) { addrCh <- a }) }()
	select {
	case a := <-addrCh:
		base = "http://" + a.String()
	case code := <-exit:
		t.Fatalf("run exited early with %d:\n%s", code, out.String())
	}
	return base, opener, cancel, exit, out
}

// Scenario: Readiness reflects the database (#4), at the wiring level: the
// store is registered as the "postgres" readiness checker, and its failure
// turns /readyz into 503 with the error under checks.postgres. The real
// binary against a stopped container is TestBinaryReadinessReflectsDatabase
// (integration).
func TestRunReadinessReflectsDatabase(t *testing.T) {
	t.Parallel()

	base, opener, cancel, done, out := startRun(t, map[string]string{"HTTP_ADDR": "127.0.0.1:0"})
	client := newClient()
	if code, body := get(t, client, base+"/readyz"); code != http.StatusOK || strings.TrimSpace(body) != `{"status":"ready"}` {
		t.Errorf("GET /readyz with database up = %d %q, want 200 ready", code, body)
	}

	opener.st.setCheckErr(errors.New("pinging postgres: connection refused"))
	want := `{"status":"not_ready","checks":{"postgres":"pinging postgres: connection refused"}}`
	if code, body := get(t, client, base+"/readyz"); code != http.StatusServiceUnavailable || strings.TrimSpace(body) != want {
		t.Errorf("GET /readyz with database down = %d %q, want 503 %s", code, body, want)
	}

	cancel()
	if code := <-done; code != 0 {
		t.Errorf("exit code = %d, want 0:\n%s", code, out.String())
	}
	if !opener.st.closed.Load() {
		t.Error("store was not closed on shutdown")
	}
}

// GET /top-senders is wired to the store opened at startup. The real binary
// against Postgres is TestBinaryTopSenders (integration).
func TestRunServesTopSendersFromTheStore(t *testing.T) {
	t.Parallel()

	base, opener, cancel, done, out := startRun(t, map[string]string{"HTTP_ADDR": "127.0.0.1:0"})
	want := `{"senders":[{"email":"kay.mann@enron.com","count":5}]}`
	if code, body := get(t, newClient(), base+"/top-senders?limit=3"); code != http.StatusOK || strings.TrimSpace(body) != want {
		t.Errorf("GET /top-senders = %d %q, want 200 %s", code, body, want)
	}
	if got := opener.st.topLimit.Load(); got != 3 {
		t.Errorf("store TopSenders limit = %d, want 3", got)
	}

	cancel()
	if code := <-done; code != 0 {
		t.Errorf("exit code = %d, want 0:\n%s", code, out.String())
	}
}

type blockingStore struct {
	fakeStore
	release chan struct{}
}

func (b *blockingStore) Close() { <-b.release }

// A pool close that hangs gives up when the close budget ends instead of
// holding the process.
func TestCloseStoreHonorsContext(t *testing.T) {
	t.Parallel()

	if err := closeStore(t.Context(), &fakeStore{}); err != nil {
		t.Errorf("closeStore = %v, want nil", err)
	}
	st := &blockingStore{release: make(chan struct{})}
	defer close(st.release) // let the goroutine end (goleak)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := closeStore(ctx, st); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("closeStore with a hanging Close = %v, want a deadline error", err)
	}
}

// Process level: an unreachable database makes the real binary exit 1
// before listening, with a JSON error log.
func TestBinaryDatabaseUnavailableExitsWithStatus1(t *testing.T) {
	t.Parallel()
	bin := hostBinary(t)

	cmd := exec.CommandContext(t.Context(), bin) //nolint:gosec // bin is built by this test run.
	cmd.Env = []string{                          // Minimal environment, like the other binary tests.
		"HTTP_ADDR=127.0.0.1:0",
		// Port 1 on loopback refuses connections immediately.
		"DATABASE_URL=postgres://enron:s3cret@127.0.0.1:1/enron?sslmode=disable&connect_timeout=2",
	}
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("exit = %v, want status 1:\n%s", err, out)
	}
	lines := decodeLogLines(t, out)
	if findLog(lines, "listening") != nil {
		t.Errorf("binary listened without a database:\n%s", out)
	}
	if rec := findLog(lines, "opening database failed"); rec == nil || rec["level"] != "ERROR" {
		t.Errorf(`want an ERROR "opening database failed" log line, got:\n%s`, out)
	}
	if bytes.Contains(out, []byte("s3cret")) {
		t.Errorf("logs leak the database password:\n%s", out)
	}
}

func TestRunServesProbesAndMetrics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	addrCh := make(chan net.Addr, 1)
	done := make(chan int, 1)
	var out syncBuffer
	opener := newFakeOpener()
	go func() {
		done <- run(ctx, envLookup(map[string]string{"HTTP_ADDR": "127.0.0.1:0", "LOG_LEVEL": "debug"}), &out,
			opener.open, func(a net.Addr) { addrCh <- a })
	}()
	var base string
	select {
	case a := <-addrCh:
		base = "http://" + a.String()
	case code := <-done:
		t.Fatalf("run exited early with %d:\n%s", code, out.String())
	}

	if rec := findLog(decodeLogLines(t, []byte(out.String())), "listening"); rec == nil ||
		rec["addr"] != "127.0.0.1:0" || rec["listen_addr"] != strings.TrimPrefix(base, "http://") {
		t.Errorf(`want "listening" with the configured addr and the bound listen_addr, got %v`, rec)
	}

	client := newClient()
	for path, want := range map[string]string{
		"/healthz": `{"status":"ok"}`,
		"/readyz":  `{"status":"ready"}`,
	} {
		if code, body := get(t, client, base+path); code != http.StatusOK || strings.TrimSpace(body) != want {
			t.Errorf("GET %s = %d %q, want 200 %s", path, code, body, want)
		}
	}
	code, body := get(t, client, base+"/metrics")
	for _, want := range []string{"go_goroutines", "process_", `enron_http_requests_total{code="200",method="GET",route="GET /healthz"} 1`} {
		if code != http.StatusOK || !strings.Contains(body, want) {
			t.Errorf("GET /metrics = %d, missing %q", code, want)
		}
	}

	cancel()
	if code := <-done; code != 0 {
		t.Errorf("exit code = %d, want 0:\n%s", code, out.String())
	}
	if !opener.st.closed.Load() {
		t.Error("store was not closed on shutdown")
	}
}

// syncBuffer is a goroutine-safe io.Writer.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
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

// drainHarness runs serve with the real API handler plus a GET /slow route.
type drainHarness struct {
	addr      string
	ready     *httpapi.Readiness
	inFlight  chan struct{} // closed when /slow has started
	done      chan error    // serve's result
	cancel    context.CancelFunc
	closeLogs []string
	// closeCtxErrs is ctx.Err() as each closer saw it on entry.
	closeCtxErrs []error
	// slowDoneAtClose is whether the /slow handler had returned when each
	// closer ran.
	slowDoneAtClose []bool
	slowReturned    atomic.Bool
}

func startDrainHarness(t *testing.T, slow func(w http.ResponseWriter, r *http.Request), opts shutdownOpts) *drainHarness {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	h := &drainHarness{
		ready:    httpapi.NewReadiness(),
		inFlight: make(chan struct{}),
		done:     make(chan error, 1),
	}
	mux := http.NewServeMux()
	mux.Handle("/", httpapi.New(httpapi.Deps{Registry: prometheus.NewRegistry(), Readiness: h.ready}, logger))
	var once sync.Once
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(h.inFlight) })
		slow(w, r)
		h.slowReturned.Store(true)
	})
	srv := newServer(mux, logger)

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	h.addr = ln.Addr().String()

	var cl closers
	var mu sync.Mutex
	for _, name := range []string{"db", "ingest"} {
		cl.add(name, func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			h.closeLogs = append(h.closeLogs, name)
			h.closeCtxErrs = append(h.closeCtxErrs, ctx.Err())
			h.slowDoneAtClose = append(h.slowDoneAtClose, h.slowReturned.Load())
			return nil
		})
	}
	if opts.closeBudget == 0 {
		opts.closeBudget = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(t.Context())
	h.cancel = cancel
	go func() { h.done <- serve(ctx, srv, ln, h.ready, cl, opts, logger) }()
	return h
}

type slowResult struct {
	code int
	body string
	err  error
}

// startSlowRequest sends GET /slow in the background.
func (h *drainHarness) startSlowRequest(ctx context.Context) <-chan slowResult {
	res := make(chan slowResult, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+h.addr+"/slow", nil)
		if err != nil {
			res <- slowResult{err: err}
			return
		}
		resp, err := newClient().Do(req)
		if err != nil {
			res <- slowResult{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		res <- slowResult{code: resp.StatusCode, body: string(body), err: err}
	}()
	return res
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Scenario: Graceful shutdown drains in-flight requests. The drain delay
// (SHUTDOWN_DRAIN_DELAY) is the window in which the service still accepts
// connections but reports not ready; net/http drops requests read after
// Shutdown has begun, so without it no client could observe the 503.
func TestGracefulShutdownDrainsInFlightRequests(t *testing.T) {
	t.Parallel()

	h := startDrainHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(time.Second)
		_, _ = w.Write([]byte("done"))
	}, shutdownOpts{timeout: 10 * time.Second, drainDelay: 500 * time.Millisecond})

	// Given a request that takes 1s is in flight.
	slow := h.startSlowRequest(t.Context())
	<-h.inFlight

	// When the process receives SIGTERM (main turns it into ctx cancellation).
	h.cancel()
	waitFor(t, "readiness to flip", h.ready.ShuttingDown)

	// Then GET /readyz returns 503 during the drain.
	code, body := get(t, newClient(), "http://"+h.addr+"/readyz")
	if code != http.StatusServiceUnavailable || strings.TrimSpace(body) != `{"status":"shutting_down"}` {
		t.Errorf("GET /readyz during drain = %d %s, want 503 shutting_down", code, body)
	}

	// And the in-flight request completes with HTTP 200.
	res := <-slow
	if res.err != nil || res.code != http.StatusOK || res.body != "done" {
		t.Errorf("in-flight request = %d %q %v, want 200 done", res.code, res.body, res.err)
	}
	// And the process exits with status 0 (serve returns nil; run maps it to 0).
	if err := <-h.done; err != nil {
		t.Errorf("serve returned %v, want nil", err)
	}
	if got := strings.Join(h.closeLogs, ","); got != "ingest,db" {
		t.Errorf("closers ran as %q, want reverse registration order ingest,db", got)
	}
}

// TestShutdownWithoutDrainDelayStillDrains checks the default (no delay):
// the listener closes at once, yet the in-flight request still completes.
func TestShutdownWithoutDrainDelayStillDrains(t *testing.T) {
	t.Parallel()

	h := startDrainHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte("done"))
	}, shutdownOpts{timeout: 10 * time.Second})

	slow := h.startSlowRequest(t.Context())
	<-h.inFlight
	h.cancel()

	if res := <-slow; res.err != nil || res.code != http.StatusOK || res.body != "done" {
		t.Errorf("in-flight request = %d %q %v, want 200 done", res.code, res.body, res.err)
	}
	if err := <-h.done; err != nil {
		t.Errorf("serve returned %v, want nil", err)
	}
	if !h.ready.ShuttingDown() {
		t.Error("readiness was not flipped")
	}
}

func TestShutdownTimeoutIsAnError(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	h := startDrainHarness(t, func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}, shutdownOpts{timeout: 100 * time.Millisecond})
	defer close(release)

	slow := h.startSlowRequest(t.Context())
	<-h.inFlight
	h.cancel()

	err := <-h.done
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("serve returned %v, want a deadline error (exit status 1)", err)
	}
	if got := strings.Join(h.closeLogs, ","); got != "ingest,db" {
		t.Errorf("closers ran as %q, want them run even after a timeout", got)
	}
	// Closers get a fresh budget, not the expired drain context, and run only
	// after the force-closed handler has returned.
	for i, cerr := range h.closeCtxErrs {
		if cerr != nil {
			t.Errorf("closer %s got an already expired context: %v", h.closeLogs[i], cerr)
		}
		if !h.slowDoneAtClose[i] {
			t.Errorf("closer %s ran while a handler was still running", h.closeLogs[i])
		}
	}
	<-slow // The forced close ends the request.
}

// TestClosersWaitForSlowToStopHandlers: after a drain timeout, a handler that
// takes a while to notice its canceled context is still waited for before the
// closers release resources it may use.
func TestClosersWaitForSlowToStopHandlers(t *testing.T) {
	t.Parallel()

	h := startDrainHarness(t, func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		time.Sleep(300 * time.Millisecond) // e.g. finishing a DB write
	}, shutdownOpts{timeout: 100 * time.Millisecond, closeBudget: 5 * time.Second})

	slow := h.startSlowRequest(t.Context())
	<-h.inFlight
	h.cancel()

	if err := <-h.done; !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "waiting for handlers") {
		t.Errorf("serve returned %v, want only the drain deadline error", err)
	}
	if len(h.slowDoneAtClose) != 2 || !h.slowDoneAtClose[0] || !h.slowDoneAtClose[1] {
		t.Errorf("closers ran before the handler returned: %v", h.slowDoneAtClose)
	}
	<-slow
}

// TestHandlerIgnoringCancellationIsReported: a handler that ignores its
// context for the whole close budget is reported, and the closers still run.
func TestHandlerIgnoringCancellationIsReported(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	h := startDrainHarness(t, func(http.ResponseWriter, *http.Request) {
		<-release // Ignores r.Context().
	}, shutdownOpts{timeout: 50 * time.Millisecond, closeBudget: 200 * time.Millisecond})

	slow := h.startSlowRequest(t.Context())
	<-h.inFlight
	h.cancel()

	err := <-h.done
	if err == nil || !strings.Contains(err.Error(), "waiting for handlers") {
		t.Errorf("serve returned %v, want a waiting-for-handlers error", err)
	}
	if got := strings.Join(h.closeLogs, ","); got != "ingest,db" {
		t.Errorf("closers ran as %q, want them run anyway", got)
	}
	close(release)
	<-slow
}

func TestServeReturnsErrorWhenServerFails(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_ = ln.Close() // Serve fails immediately on a closed listener.

	ready := httpapi.NewReadiness()
	closed := false
	cl := closers{{name: "db", close: func(context.Context) error { closed = true; return nil }}}
	logger := slog.New(slog.DiscardHandler)
	opts := shutdownOpts{timeout: time.Second, drainDelay: time.Minute} // a failed server skips the delay
	err = serve(t.Context(), newServer(http.NotFoundHandler(), logger), ln, ready, cl, opts, logger)
	if err == nil || !strings.Contains(err.Error(), "serving") {
		t.Errorf("serve returned %v, want a serving error", err)
	}
	if !closed || !ready.ShuttingDown() {
		t.Errorf("after a server failure: closed=%v shuttingDown=%v, want both true", closed, ready.ShuttingDown())
	}
}

func TestClosersRunInReverseOrderAndJoinErrors(t *testing.T) {
	t.Parallel()

	var order []string
	var cl closers
	errA := errors.New("a failed")
	cl.add("a", func(context.Context) error { order = append(order, "a"); return errA })
	cl.add("b", func(context.Context) error { order = append(order, "b"); return nil })
	cl.add("c", func(context.Context) error { order = append(order, "c"); return errors.New("c failed") })

	var logs bytes.Buffer
	err := cl.closeAll(t.Context(), newLogger(&logs, slog.LevelInfo))
	if got := strings.Join(order, ","); got != "c,b,a" {
		t.Errorf("order = %q, want c,b,a", got)
	}
	if !errors.Is(err, errA) || !strings.Contains(err.Error(), "closing c") {
		t.Errorf("err = %v, want both failures joined", err)
	}
	if n := strings.Count(logs.String(), `"msg":"closing failed"`); n != 2 {
		t.Errorf("got %d closing-failed log lines, want 2", n)
	}
	if err := (closers{}).closeAll(t.Context(), newLogger(io.Discard, slog.LevelInfo)); err != nil {
		t.Errorf("no closers: err = %v", err)
	}
}

func TestNewServerTimeouts(t *testing.T) {
	t.Parallel()

	srv := newServer(http.NotFoundHandler(), newLogger(io.Discard, slog.LevelInfo))
	if srv.ReadHeaderTimeout != 5*time.Second || srv.ReadTimeout != 30*time.Second ||
		srv.WriteTimeout != 30*time.Second || srv.IdleTimeout != 120*time.Second {
		t.Errorf("unexpected timeouts: %+v", srv)
	}
}

func TestLoggerLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := newLogger(&buf, slog.LevelWarn)
	logger.Info("hidden")
	logger.Warn("shown")
	lines := decodeLogLines(t, buf.Bytes())
	if len(lines) != 1 || lines[0]["msg"] != "shown" || lines[0]["service"] != "enron-service" {
		t.Errorf("unexpected log output: %s", buf.String())
	}
}

var (
	buildOnce sync.Once
	buildDir  string // removed by TestMain
	builtBin  string
	buildErr  error
)

// buildBinary builds ./cmd/enron-service like `make build` (CGO_ENABLED=0,
// -trimpath) plus any extra env, and returns the binary path. Keep the two in
// sync.
func buildBinary(t *testing.T, dir string, env ...string) (string, error) {
	t.Helper()
	return buildBinaryArgs(t, dir, nil, env...)
}

// buildBinaryArgs is buildBinary with extra `go build` flags.
func buildBinaryArgs(t *testing.T, dir string, flags []string, env ...string) (string, error) {
	t.Helper()
	bin := filepath.Join(dir, "enron-service")
	args := append(append([]string{"build", "-trimpath"}, flags...), "-o", bin, ".")
	build := exec.CommandContext(t.Context(), "go", args...) //nolint:gosec // fixed command and flags; bin is a temp path.
	build.Env = append(append(os.Environ(), "CGO_ENABLED=0"), env...)
	if out, err := build.CombinedOutput(); err != nil {
		return "", errors.New("go build failed: " + err.Error() + "\n" + string(out))
	}
	return bin, nil
}

// hostBinary returns the service built for the host, built once per test run.
func hostBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds the binary; skipped in -short mode")
	}
	buildOnce.Do(func() {
		buildDir, buildErr = os.MkdirTemp("", "enron-service-test")
		if buildErr == nil {
			builtBin, buildErr = buildBinary(t, buildDir)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return builtBin
}

// TestBuildProducesStaticBinary covers the static half of the Gherkin
// scenario "Build produces a static binary" (#1). It cross-builds for the
// deployment target (linux/amd64; macOS has no static executables) and
// asserts the ELF has no dynamic interpreter and no shared-library imports.
func TestBuildProducesStaticBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary; skipped in -short mode")
	}
	t.Parallel()

	bin, err := buildBinary(t, t.TempDir(), "GOOS=linux", "GOARCH=amd64")
	if err != nil {
		t.Fatal(err)
	}
	f, err := elf.Open(bin)
	if err != nil {
		t.Fatalf("opening ELF: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			t.Error("binary has a PT_INTERP segment (dynamically linked)")
		}
	}
	libs, err := f.ImportedLibraries()
	if err != nil {
		t.Fatalf("reading imported libraries: %v", err)
	}
	if len(libs) != 0 {
		t.Errorf("binary imports shared libraries: %v", libs)
	}
}

// Scenario: Invalid configuration fails fast (process level).
func TestBinaryInvalidConfigurationExitsWithStatus1(t *testing.T) {
	t.Parallel()
	bin := hostBinary(t)

	cmd := exec.CommandContext(t.Context(), bin) //nolint:gosec // bin is built by this test run.
	cmd.Env = []string{"INGEST_WORKERS=zero"}    // Minimal environment, see startBinary.
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("exit = %v, want status 1", err)
	}
	if !bytes.Contains(out, []byte("INGEST_WORKERS")) {
		t.Errorf("output does not mention INGEST_WORKERS:\n%s", out)
	}
}

// TestBinaryReportsLinkerVersion: the version injected at link time with
// -ldflags "-X main.version=..." (as the Dockerfile does, #11) appears in the
// "starting" log line. "starting" is logged before the database is opened, so
// the test reads the log only up to that line and then kills the process: it
// never depends on how long the binary keeps trying an unreachable database.
func TestBinaryReportsLinkerVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary; skipped in -short mode")
	}
	t.Parallel()

	const want = "1.2.3-test"
	bin, err := buildBinaryArgs(t, t.TempDir(), []string{"-ldflags=-s -w -X main.version=" + want})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin) //nolint:gosec // bin is built by this test.
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = []string{ // Minimal environment, like the other binary tests.
		"HTTP_ADDR=127.0.0.1:0",
		"DATABASE_URL=postgres://enron:enron@127.0.0.1:1/enron?sslmode=disable&connect_timeout=1",
		// Bounds startup retries where supported (#27); the test does not rely on it.
		"DB_STARTUP_TIMEOUT=1s",
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting binary: %v", err)
	}

	var logs bytes.Buffer // written by the scanner, read after <-scanned
	found := make(chan map[string]any, 1)
	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			logs.Write(append(sc.Bytes(), '\n'))
			var rec map[string]any
			if json.Unmarshal(sc.Bytes(), &rec) == nil && rec["msg"] == "starting" {
				found <- rec
				return
			}
		}
	}()

	var rec map[string]any
	select {
	case rec = <-found:
	case <-scanned: // stdout closed without a "starting" line.
	case <-ctx.Done():
	}
	cancel() // Kills the process: nothing after "starting" matters here.
	_ = cmd.Wait()
	<-scanned
	if rec == nil { // select picks randomly when found and scanned are both ready.
		select {
		case rec = <-found:
		default:
		}
	}
	if rec == nil || rec["version"] != want {
		t.Errorf("starting record = %v, want version %q; logs:\n%s", rec, want, logs.String())
	}
}
