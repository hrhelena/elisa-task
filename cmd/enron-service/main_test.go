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
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"

	"github.com/hrhelena/elisa-task/internal/config"
	"github.com/hrhelena/elisa-task/internal/httpapi"
)

func TestMain(m *testing.M) {
	code := m.Run()
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
	code := run(ctx, envLookup(nil), &out, func(net.Addr) { cancel() })

	lines := decodeLogLines(t, out.Bytes())
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
	code := run(t.Context(), envLookup(map[string]string{"INGEST_WORKERS": "zero"}), &out,
		func(net.Addr) { listened = true })
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
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
	code := run(t.Context(), envLookup(map[string]string{"HTTP_ADDR": busy.Addr().String()}), &out, nil)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if findLog(decodeLogLines(t, out.Bytes()), "listen failed") == nil {
		t.Errorf(`no "listen failed" log line in:\n%s`, out.String())
	}
}

func TestRunServesProbesAndMetrics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	addrCh := make(chan net.Addr, 1)
	done := make(chan int, 1)
	var out syncBuffer
	go func() {
		done <- run(ctx, envLookup(map[string]string{"HTTP_ADDR": "127.0.0.1:0", "LOG_LEVEL": "debug"}), &out,
			func(a net.Addr) { addrCh <- a })
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
		cl.add(name, func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			h.closeLogs = append(h.closeLogs, name)
			return nil
		})
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
	<-slow // The forced close ends the request.
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
	bin := filepath.Join(dir, "enron-service")
	build := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", bin, ".") //nolint:gosec // fixed command; bin is a temp path.
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

// TestBinaryServesAndExitsCleanlyOnSIGTERM runs the real binary: it logs
// JSON ("starting", then "listening"), serves /healthz, and on a real SIGTERM
// shuts down and exits 0. It also covers the run half of "Build produces a
// static binary" (#1), which now serves until signaled.
func TestBinaryServesAndExitsCleanlyOnSIGTERM(t *testing.T) {
	t.Parallel()
	bin := hostBinary(t)

	cmd := exec.CommandContext(t.Context(), bin) //nolint:gosec // bin is built by this test run.
	cmd.Env = append(os.Environ(), "HTTP_ADDR=127.0.0.1:0")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting binary: %v", err)
	}

	var (
		logs     syncBuffer
		listenCh = make(chan string, 1)
		readDone = make(chan struct{})
	)
	go func() {
		defer close(readDone)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Bytes()
			_, _ = logs.Write(append(line, '\n'))
			var rec map[string]any
			if json.Unmarshal(line, &rec) == nil && rec["msg"] == "listening" {
				addr, _ := rec["listen_addr"].(string)
				listenCh <- addr
			}
		}
	}()

	select {
	case addr := <-listenCh:
		if code, body := get(t, newClient(), "http://"+addr+"/healthz"); code != http.StatusOK || !strings.Contains(body, `"ok"`) {
			t.Errorf("GET /healthz = %d %q", code, body)
		}
	case <-readDone:
		t.Fatalf("binary exited before listening:\n%s", logs.String())
	case <-time.After(10 * time.Second):
		t.Fatalf("binary did not log listening:\n%s", logs.String())
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("sending SIGTERM: %v", err)
	}
	<-readDone
	if err := cmd.Wait(); err != nil {
		t.Errorf("binary exited with %v, want status 0:\n%s", err, logs.String())
	}
	lines := decodeLogLines(t, []byte(logs.String()))
	for _, msg := range []string{"starting", "listening", "shutting down", "stopped"} {
		if findLog(lines, msg) == nil {
			t.Errorf("no %q log line in:\n%s", msg, logs.String())
		}
	}
}

// Scenario: Invalid configuration fails fast (process level).
func TestBinaryInvalidConfigurationExitsWithStatus1(t *testing.T) {
	t.Parallel()
	bin := hostBinary(t)

	cmd := exec.CommandContext(t.Context(), bin) //nolint:gosec // bin is built by this test run.
	cmd.Env = append(os.Environ(), "INGEST_WORKERS=zero")
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("exit = %v, want status 1", err)
	}
	if !bytes.Contains(out, []byte("INGEST_WORKERS")) {
		t.Errorf("output does not mention INGEST_WORKERS:\n%s", out)
	}
}
