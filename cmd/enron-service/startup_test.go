package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hrhelena/elisa-task/internal/store"
)

// errNotUp is what store.Open returns while the server is down.
var errNotUp = fmt.Errorf("connecting to postgres: %w: dial tcp 127.0.0.1:5432: connection refused", store.ErrUnreachable)

// noWait is a backoff without delays, so retry tests never sleep.
var noWait = backoff{jitter: func(time.Duration) time.Duration { return 0 }}

// retryWarnings returns the attempt numbers of the "database not ready,
// retrying" records, failing on any that is not at WARN level.
func retryWarnings(t *testing.T, lines []map[string]any) []int {
	t.Helper()
	var attempts []int
	for _, rec := range lines {
		if rec["msg"] != "database not ready, retrying" {
			continue
		}
		if rec["level"] != "WARN" {
			t.Errorf("retry logged at %v, want WARN: %v", rec["level"], rec)
		}
		if e, _ := rec["err"].(string); e == "" {
			t.Errorf("retry record without the error: %v", rec)
		}
		n, _ := rec["attempt"].(float64)
		attempts = append(attempts, int(n))
	}
	return attempts
}

func TestBackoffDelay(t *testing.T) {
	t.Parallel()

	b := backoff{initial: 250 * time.Millisecond, max: 5 * time.Second}
	tests := []struct {
		attempt int
		want    time.Duration // d, before jitter
	}{
		{1, 250 * time.Millisecond},
		{2, 500 * time.Millisecond},
		{3, time.Second},
		{5, 4 * time.Second},
		{6, 5 * time.Second},
		{1000, 5 * time.Second}, // capped, no overflow
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.attempt), func(t *testing.T) {
			t.Parallel()
			low, high := b, b
			low.jitter = func(time.Duration) time.Duration { return 0 }
			high.jitter = func(d time.Duration) time.Duration { return d }
			if got := low.delay(tt.attempt); got != tt.want/2 {
				t.Errorf("delay(%d) with no jitter = %v, want %v", tt.attempt, got, tt.want/2)
			}
			if got := high.delay(tt.attempt); got != tt.want {
				t.Errorf("delay(%d) with full jitter = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestRandomJitterStaysInRange(t *testing.T) {
	t.Parallel()

	if got := randomJitter(0); got != 0 {
		t.Errorf("randomJitter(0) = %v, want 0", got)
	}
	for range 1000 {
		if got := randomJitter(time.Second); got < 0 || got > time.Second {
			t.Fatalf("randomJitter(1s) = %v, want within [0, 1s]", got)
		}
	}
}

// Scenario: Database comes up late (retry loop): every failed attempt is
// logged at WARN with its number, and the store from the first successful
// attempt is returned.
func TestRetryOpenRetriesUntilReachable(t *testing.T) {
	t.Parallel()

	var out syncBuffer
	st := &fakeStore{}
	calls := 0
	got, err := retryOpen(t.Context(), func(context.Context) (dbStore, error) {
		calls++
		if calls <= 3 {
			return nil, errNotUp
		}
		return st, nil
	}, noWait, newLogger(&out, slog.LevelInfo))
	if err != nil || got != st {
		t.Fatalf("retryOpen = %v, %v; want the store", got, err)
	}
	if calls != 4 {
		t.Errorf("open called %d times, want 4", calls)
	}
	if got := retryWarnings(t, decodeLogLines(t, []byte(out.String()))); fmt.Sprint(got) != "[1 2 3]" {
		t.Errorf("retry warnings for attempts %v, want [1 2 3]:\n%s", got, out.String())
	}
}

// A malformed URL or a failed migration does not get better with time.
func TestRetryOpenStopsOnPermanentError(t *testing.T) {
	t.Parallel()

	var out syncBuffer
	permanent := errors.New("applying migrations: syntax error")
	calls := 0
	_, err := retryOpen(t.Context(), func(context.Context) (dbStore, error) {
		calls++
		return nil, permanent
	}, noWait, newLogger(&out, slog.LevelInfo))
	if !errors.Is(err, permanent) || calls != 1 {
		t.Errorf("retryOpen = %v after %d call(s), want the permanent error after 1", err, calls)
	}
	if strings.Contains(out.String(), "retrying") {
		t.Errorf("permanent error was retried:\n%s", out.String())
	}
}

// Scenario: Database never comes up (retry loop): the loop ends with the
// budget and reports the last connection error.
func TestRetryOpenGivesUpWhenTheBudgetEnds(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	slow := backoff{initial: 10 * time.Millisecond, max: 10 * time.Millisecond, jitter: noWait.jitter}
	_, err := retryOpen(ctx, func(context.Context) (dbStore, error) { return nil, errNotUp }, slow,
		slog.New(slog.DiscardHandler))
	if !errors.Is(err, store.ErrUnreachable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("retryOpen = %v, want it to wrap ErrUnreachable and DeadlineExceeded", err)
	}
	if !strings.Contains(fmt.Sprint(err), "giving up after") {
		t.Errorf("error %q does not report the attempts", err)
	}
}

// A shutdown during the wait between attempts ends the loop at once.
func TestRetryOpenStopsWhenCanceledDuringBackoff(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	hour := backoff{initial: time.Hour, max: time.Hour, jitter: noWait.jitter}
	var start time.Time
	_, err := retryOpen(ctx, func(context.Context) (dbStore, error) {
		// Cancel once retryOpen waits for the next attempt.
		start = time.Now()
		time.AfterFunc(20*time.Millisecond, cancel)
		return nil, errNotUp
	}, hour, slog.New(slog.DiscardHandler))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("retryOpen = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("retryOpen took %v after cancel", elapsed)
	}
}

// scriptedOpener returns errNotUp for the first fails calls, then a fake
// store. With fails < 0 it never succeeds. onCall, if set, runs first.
type scriptedOpener struct {
	fails  int
	calls  atomic.Int32
	st     fakeStore
	onCall func(ctx context.Context)
}

func (o *scriptedOpener) open(ctx context.Context, _ string, _ *slog.Logger) (dbStore, error) {
	n := int(o.calls.Add(1))
	if o.onCall != nil {
		o.onCall(ctx)
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("scripted opener: open context has no deadline")
	}
	if o.fails < 0 || n <= o.fails {
		return nil, errNotUp
	}
	return &o.st, nil
}

// Scenario: Database comes up late (run level).
func TestRunDatabaseComesUpLate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var out syncBuffer
	opener := &scriptedOpener{fails: 1}
	code := run(ctx, envLookup(map[string]string{"HTTP_ADDR": "127.0.0.1:0", "DB_STARTUP_TIMEOUT": "60s"}),
		&out, opener.open, func(net.Addr) { cancel() })
	if code != 0 {
		t.Fatalf("exit code = %d, want 0:\n%s", code, out.String())
	}
	lines := decodeLogLines(t, []byte(out.String()))
	if got := retryWarnings(t, lines); fmt.Sprint(got) != "[1]" {
		t.Errorf("retry warnings for attempts %v, want [1]:\n%s", got, out.String())
	}
	if findLog(lines, "listening") == nil {
		t.Errorf("no listening line once the database was reachable:\n%s", out.String())
	}
	if !opener.st.closed.Load() {
		t.Error("store was not closed on shutdown")
	}
}

// Scenario: Database never comes up (run level, with a short budget).
func TestRunDatabaseNeverComesUp(t *testing.T) {
	t.Parallel()

	var out syncBuffer
	listened := false
	opener := &scriptedOpener{fails: -1}
	start := time.Now()
	code := run(t.Context(), envLookup(map[string]string{"HTTP_ADDR": "127.0.0.1:0", "DB_STARTUP_TIMEOUT": "300ms"}),
		&out, opener.open, func(net.Addr) { listened = true })
	elapsed := time.Since(start)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if listened {
		t.Error("service listened without a database")
	}
	if elapsed < 300*time.Millisecond || elapsed > 2*time.Second {
		t.Errorf("gave up after %v, want about the 300ms budget", elapsed)
	}
	lines := decodeLogLines(t, []byte(out.String()))
	if len(retryWarnings(t, lines)) == 0 {
		t.Errorf("no retry warnings:\n%s", out.String())
	}
	rec := findLog(lines, "opening database failed")
	if rec == nil || rec["level"] != "ERROR" || !strings.Contains(fmt.Sprint(rec["err"]), "connection refused") {
		t.Errorf(`want an ERROR "opening database failed" with the last error, got:\n%s`, out.String())
	}
}

// Scenario: Shutdown during startup (run level). The signal can arrive
// while an attempt is in flight (a connect that hangs until canceled), while
// waiting between attempts, or just after the database opened.
func TestRunShutdownDuringStartupExitsCleanly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		opener func(cancel context.CancelFunc) *scriptedOpener
		opened bool // whether the store gets opened
	}{
		{"during an attempt", func(cancel context.CancelFunc) *scriptedOpener {
			return &scriptedOpener{fails: -1, onCall: func(ctx context.Context) { cancel(); <-ctx.Done() }}
		}, false},
		{"between attempts", func(cancel context.CancelFunc) *scriptedOpener {
			// The first retry waits at least 125ms; cancel within that wait.
			return &scriptedOpener{fails: -1, onCall: func(context.Context) { time.AfterFunc(20*time.Millisecond, cancel) }}
		}, false},
		{"after the database opened", func(cancel context.CancelFunc) *scriptedOpener {
			return &scriptedOpener{fails: 0, onCall: func(context.Context) { cancel() }}
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var out syncBuffer
			listened := false
			opener := tt.opener(cancel)
			start := time.Now()
			code := run(ctx, envLookup(map[string]string{"HTTP_ADDR": "127.0.0.1:0", "DB_STARTUP_TIMEOUT": "1m"}),
				&out, opener.open, func(net.Addr) { listened = true })
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Errorf("run took %v after the signal, want under 1s", elapsed)
			}
			if code != 0 {
				t.Errorf("exit code = %d, want 0:\n%s", code, out.String())
			}
			if listened {
				t.Error("service listened after a shutdown signal")
			}
			if tt.opened && !opener.st.closed.Load() {
				t.Error("store opened during the signal was not closed")
			}
			lines := decodeLogLines(t, []byte(out.String()))
			if findLog(lines, "opening database failed") != nil {
				t.Errorf("shutdown during startup logged a failure:\n%s", out.String())
			}
			if findLog(lines, "stopped during startup") == nil {
				t.Errorf(`no "stopped during startup" line:\n%s`, out.String())
			}
		})
	}
}

// Scenario: Shutdown during startup (process level): a real SIGTERM while
// the binary retries an unreachable database exits 0 within 1 second.
func TestBinarySIGTERMDuringStartupExitsCleanly(t *testing.T) {
	t.Parallel()
	bin := hostBinary(t)

	cmd := exec.CommandContext(t.Context(), bin) //nolint:gosec // bin is built by this test run.
	cmd.Env = []string{                          // Minimal environment, like the other binary tests.
		"HTTP_ADDR=127.0.0.1:0",
		"DB_STARTUP_TIMEOUT=1m",
		// Port 1 on loopback refuses connections immediately.
		"DATABASE_URL=postgres://enron:s3cret@127.0.0.1:1/enron?sslmode=disable",
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting binary: %v", err)
	}

	// Read the log until the first retry warning, then keep the rest.
	var logs bytes.Buffer
	retrying := make(chan struct{})
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		sc := bufio.NewScanner(stdout)
		seen := false
		for sc.Scan() {
			logs.Write(append(sc.Bytes(), '\n'))
			var rec map[string]any
			if !seen && json.Unmarshal(sc.Bytes(), &rec) == nil && rec["msg"] == "database not ready, retrying" {
				seen = true
				close(retrying)
			}
		}
	}()
	select {
	case <-retrying:
	case <-readDone:
		t.Fatalf("binary exited before retrying:\n%s", logs.String())
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-readDone
		t.Fatalf("binary did not log a retry:\n%s", logs.String())
	}

	start := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("sending SIGTERM: %v", err)
	}
	select {
	case <-readDone:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-readDone
		t.Fatalf("SIGTERM did not end the process:\n%s", logs.String())
	}
	err = cmd.Wait()
	elapsed := time.Since(start)
	if err != nil {
		t.Errorf("exit = %v, want status 0:\n%s", err, logs.String())
	}
	if elapsed > time.Second {
		t.Errorf("exit took %v after SIGTERM, want under 1s", elapsed)
	}
	if strings.Contains(logs.String(), "s3cret") {
		t.Errorf("logs leak the database password:\n%s", logs.String())
	}
}
