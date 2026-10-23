//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hrhelena/elisa-task/internal/store/storetest"
)

// The real-binary tests need a reachable database (the service opens the
// store before it listens), so they run under the integration tag against a
// Postgres container shared by the package.
var sharedDB *storetest.Postgres

func init() {
	testEnvSetup = func() (func(), error) {
		pg, err := storetest.Start(context.Background())
		if err != nil {
			return nil, err
		}
		sharedDB = pg
		return func() { _ = pg.Terminate() }, nil
	}
}

// runningBinary is the service process started by startBinary.
type runningBinary struct {
	cmd      *exec.Cmd
	logs     syncBuffer
	listen   chan string   // listen_addr from the "listening" line
	shutting chan struct{} // closed on the "shutting down" line
	readDone chan struct{} // closed when stdout is closed
}

// startBinary starts the built service with a minimal environment (only env),
// so settings from the developer's shell cannot change the outcome.
func startBinary(t *testing.T, env ...string) *runningBinary {
	t.Helper()
	b := &runningBinary{
		cmd:      exec.CommandContext(t.Context(), hostBinary(t)), //nolint:gosec // the binary is built by this test run.
		listen:   make(chan string, 1),
		shutting: make(chan struct{}),
		readDone: make(chan struct{}),
	}
	b.cmd.Env = env
	stdout, err := b.cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := b.cmd.Start(); err != nil {
		t.Fatalf("starting binary: %v", err)
	}
	go func() {
		defer close(b.readDone)
		var once sync.Once
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Bytes()
			_, _ = b.logs.Write(append(line, '\n'))
			var rec map[string]any
			if json.Unmarshal(line, &rec) != nil {
				continue
			}
			msg, _ := rec["msg"].(string)
			switch {
			case msg == "listening":
				addr, _ := rec["listen_addr"].(string)
				b.listen <- addr
			case strings.HasPrefix(msg, "shutting down"):
				once.Do(func() { close(b.shutting) })
			}
		}
	}()
	return b
}

// waitListening returns the bound address, failing if the binary does not
// listen within 30s (it migrates the database first).
func (b *runningBinary) waitListening(t *testing.T) string {
	t.Helper()
	select {
	case addr := <-b.listen:
		return addr
	case <-b.readDone:
		t.Fatalf("binary exited before listening:\n%s", b.logs.String())
	case <-time.After(30 * time.Second):
		t.Fatalf("binary did not log listening:\n%s", b.logs.String())
	}
	return ""
}

func (b *runningBinary) signal(t *testing.T) {
	t.Helper()
	if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("sending SIGTERM: %v", err)
	}
}

// waitExit0 waits for the binary to exit and fails unless it exited 0.
func (b *runningBinary) waitExit0(t *testing.T) {
	t.Helper()
	<-b.readDone
	if err := b.cmd.Wait(); err != nil {
		t.Errorf("binary exited with %v, want status 0:\n%s", err, b.logs.String())
	}
}

// TestBinaryServesAndExitsCleanlyOnSIGTERM runs the real binary: it logs
// JSON ("starting", then "listening"), serves /healthz and a ready /readyz
// once the database is migrated, and on a real SIGTERM shuts down and exits
// 0. It also covers the run half of "Build produces a static binary" (#1).
func TestBinaryServesAndExitsCleanlyOnSIGTERM(t *testing.T) {
	t.Parallel()

	b := startBinary(t, "HTTP_ADDR=127.0.0.1:0", "DATABASE_URL="+sharedDB.URL)
	addr := b.waitListening(t)
	client := newClient()
	if code, body := get(t, client, "http://"+addr+"/healthz"); code != http.StatusOK || !strings.Contains(body, `"ok"`) {
		t.Errorf("GET /healthz = %d %q", code, body)
	}
	if code, body := get(t, client, "http://"+addr+"/readyz"); code != http.StatusOK || strings.TrimSpace(body) != `{"status":"ready"}` {
		t.Errorf("GET /readyz = %d %q, want 200 ready", code, body)
	}

	b.signal(t)
	b.waitExit0(t)
	lines := decodeLogLines(t, []byte(b.logs.String()))
	for _, msg := range []string{"starting", "database migrations applied", "listening", "stopped"} {
		if findLog(lines, msg) == nil {
			t.Errorf("no %q log line in:\n%s", msg, b.logs.String())
		}
	}
	select {
	case <-b.shutting:
	default:
		t.Errorf("no shutting down log line in:\n%s", b.logs.String())
	}
}

// TestBinarySecondSignalForcesExit: during a long drain, a second SIGTERM
// kills the process at once instead of waiting for the graceful shutdown.
// The "shutting down" log line is the handover point: the binary restores
// default signal handling before it logs that line (notifyContext), so no
// sleep is needed and a second signal sent after it cannot be swallowed (#25).
func TestBinarySecondSignalForcesExit(t *testing.T) {
	t.Parallel()

	b := startBinary(t, "HTTP_ADDR=127.0.0.1:0", "SHUTDOWN_DRAIN_DELAY=1m", "DATABASE_URL="+sharedDB.URL)
	b.waitListening(t)

	b.signal(t)
	select {
	case <-b.shutting:
	case <-time.After(10 * time.Second):
		t.Fatalf("binary did not start shutting down:\n%s", b.logs.String())
	}
	start := time.Now()
	b.signal(t)

	select {
	case <-b.readDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("second SIGTERM did not end the process:\n%s", b.logs.String())
	}
	err := b.cmd.Wait()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("exit = %v, want termination by signal", err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
		t.Errorf("exit = %v, want killed by SIGTERM", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("exit took %v after the second signal", elapsed)
	}
	if findLog(decodeLogLines(t, []byte(b.logs.String())), "stopped") != nil {
		t.Error(`forced exit logged "stopped" as if the shutdown were graceful`)
	}
}

// Scenario: Readiness reflects the database (#4), end to end: the real
// binary against a dedicated Postgres container that is then stopped.
func TestBinaryReadinessReflectsDatabase(t *testing.T) {
	t.Parallel()
	pg, err := storetest.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Terminate() })

	b := startBinary(t, "HTTP_ADDR=127.0.0.1:0", "DATABASE_URL="+pg.URL)
	base := "http://" + b.waitListening(t)
	client := newClient()
	if code, body := get(t, client, base+"/readyz"); code != http.StatusOK {
		t.Fatalf("GET /readyz with database up = %d %q, want 200", code, body)
	}

	// Given the Postgres container is stopped.
	if err := pg.Stop(t.Context()); err != nil {
		t.Fatalf("stopping postgres: %v", err)
	}
	// When GET /readyz is called, then it returns 503 with a "postgres" check error.
	code, body := get(t, client, base+"/readyz")
	var resp struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("decoding /readyz body %q: %v", body, err)
	}
	if code != http.StatusServiceUnavailable || resp.Status != "not_ready" || resp.Checks["postgres"] == "" {
		t.Errorf("GET /readyz with database stopped = %d %s, want 503 not_ready with a postgres check error", code, body)
	}

	// Shutdown stays clean with the database gone.
	b.signal(t)
	b.waitExit0(t)
}
