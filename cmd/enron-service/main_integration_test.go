//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hrhelena/elisa-task/internal/store"
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

	dataDir := t.TempDir()
	validMaildir(t, dataDir, 1) // so that POST /start gets past the dataset check
	b := startBinary(t, "HTTP_ADDR=127.0.0.1:0", "DATABASE_URL="+pg.URL, "DATA_DIR="+dataDir)
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

	// Scenario: Database unavailable (#9). When GET /top-senders is called,
	// then it returns 503 with error code "unavailable".
	code, body = get(t, client, base+"/top-senders")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, `"code":"unavailable"`) {
		t.Errorf("GET /top-senders with database stopped = %d %s, want 503 unavailable", code, body)
	}

	// Scenario: Database down (#8). GET /status and POST /start return 503
	// with error code "unavailable".
	code, body = get(t, client, base+"/status")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, `"code":"unavailable"`) {
		t.Errorf("GET /status with database stopped = %d %s, want 503 unavailable", code, body)
	}
	if code, _, started := postStart(t, client, base); code != http.StatusServiceUnavailable ||
		started.Error == nil || started.Error.Code != "unavailable" {
		t.Errorf("POST /start with database stopped = %d %+v, want 503 unavailable", code, started.Error)
	}

	// Shutdown stays clean with the database gone.
	b.signal(t)
	b.waitExit0(t)
}

// senderMessages returns n messages from sender (nil: no valid sender) stored
// under mailbox/folder/1. .. n.
func senderMessages(mailbox, folder string, sender *string, n int) []store.Message {
	msgs := make([]store.Message, n)
	for i := range msgs {
		msgs[i] = store.Message{
			SourcePath: fmt.Sprintf("%s/%s/%d.", mailbox, folder, i+1),
			Mailbox:    mailbox,
			Folder:     folder,
			FromAddr:   sender,
		}
	}
	return msgs
}

func ptr[T any](v T) *T { return &v }

// getSenders calls GET /top-senders and decodes a 200 response.
func getSenders(t *testing.T, client *http.Client, url string) []store.SenderCount {
	t.Helper()
	code, body := get(t, client, url)
	if code != http.StatusOK {
		t.Fatalf("GET %s = %d %q, want 200", url, code, body)
	}
	var resp struct {
		Senders []struct {
			Email string `json:"email"`
			Count int64  `json:"count"`
		} `json:"senders"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("decoding %q: %v", body, err)
	}
	if resp.Senders == nil {
		t.Fatalf("senders is null or missing in %q", body)
	}
	out := make([]store.SenderCount, len(resp.Senders))
	for i, s := range resp.Senders {
		out[i] = store.SenderCount{Email: s.Email, Count: s.Count}
	}
	return out
}

// GET /top-senders end to end: the real binary against Postgres, on a
// database of its own in the shared container. It covers the Gherkin
// scenarios of #9 with real ranking, and that results reflect rows committed
// while the service runs.
func TestBinaryTopSenders(t *testing.T) {
	t.Parallel()
	dbURL, err := sharedDB.CreateDatabase(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	b := startBinary(t, "HTTP_ADDR=127.0.0.1:0", "DATABASE_URL="+dbURL)
	base := "http://" + b.waitListening(t) + "/top-senders"
	client := newClient()

	// Scenario: Empty database.
	if code, body := get(t, client, base); code != http.StatusOK || strings.TrimSpace(body) != `{"senders":[]}` {
		t.Errorf("GET /top-senders on an empty database = %d %q, want 200 {\"senders\":[]}", code, body)
	}

	// Seed while the service runs (the binary has migrated the database).
	st, err := store.Open(t.Context(), dbURL, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer st.Close()
	var msgs []store.Message
	msgs = append(msgs, senderMessages("mann-k", "inbox", ptr("kay.mann@enron.com"), 5)...)
	// Inserted b before a, so insertion order cannot explain the tie order.
	msgs = append(msgs, senderMessages("b", "inbox", ptr("b@enron.com"), 3)...)
	msgs = append(msgs, senderMessages("a", "inbox", ptr("a@enron.com"), 3)...)
	// The same email in two folders counts twice (ADR 0003).
	msgs = append(msgs, senderMessages("allen-p", "sent", ptr("phillip.allen@enron.com"), 1)...)
	msgs = append(msgs, senderMessages("allen-p", "_sent_mail", ptr("phillip.allen@enron.com"), 1)...)
	msgs = append(msgs, senderMessages("dasovich-j", "inbox", ptr("jeff.dasovich@enron.com"), 2)...)
	// Messages without a valid sender are stored with a NULL sender (ADR 0012).
	msgs = append(msgs, senderMessages("invalid", "inbox", nil, 4)...)
	msgs = append(msgs, senderMessages("invalid", "empty", ptr(""), 1)...)
	// Seven senders with one message each, whose order differs between byte
	// order and the database's ICU collation (storetest.CreateDatabase): the
	// ranking must not depend on the server locale (ADR 0011, 0012).
	singles := []string{"ab@enron.com", "a_z@enron.com", ".ward@enron.com", "a.b@enron.com", "c@enron.com", "single1@enron.com", "single0@enron.com"}
	for i, addr := range singles {
		msgs = append(msgs, senderMessages(fmt.Sprintf("single-%d", i), "inbox", ptr(addr), 1)...)
	}
	if _, err := st.InsertMessages(t.Context(), msgs); err != nil {
		t.Fatalf("seeding messages: %v", err)
	}

	ranking := []store.SenderCount{
		{Email: "kay.mann@enron.com", Count: 5},
		{Email: "a@enron.com", Count: 3},
		{Email: "b@enron.com", Count: 3},
		{Email: "jeff.dasovich@enron.com", Count: 2},
		{Email: "phillip.allen@enron.com", Count: 2},
	}
	slices.Sort(singles) // byte order: ".ward@" < "a.b@" < "a_z@" < "ab@" < "c@" < "single0@" < "single1@"
	for _, addr := range singles {
		ranking = append(ranking, store.SenderCount{Email: addr, Count: 1})
	}

	// Scenario: Top 10 by default (12 distinct senders, kay.mann has 5).
	got := getSenders(t, client, base)
	if !slices.Equal(got, ranking[:10]) {
		t.Errorf("GET /top-senders =\n  %v\nwant\n  %v", got, ranking[:10])
	}
	if !slices.IsSortedFunc(got, func(x, y store.SenderCount) int { return int(y.Count - x.Count) }) {
		t.Errorf("counts are not in non-increasing order: %v", got)
	}

	// Scenario: Ties are broken alphabetically (a and b both have 3; the
	// limit cuts between them).
	if got := getSenders(t, client, base+"?limit=2"); !slices.Equal(got, ranking[:2]) {
		t.Errorf("GET /top-senders?limit=2 = %v, want %v", got, ranking[:2])
	}

	// Scenario: Every file counts, including copies in several folders; and
	// Scenario: Messages without a valid sender are excluded (5 NULL-sender
	// rows, no entry for them, every ranked sender listed).
	if got := getSenders(t, client, base+"?limit=100"); !slices.Equal(got, ranking) {
		t.Errorf("GET /top-senders?limit=100 =\n  %v\nwant\n  %v", got, ranking)
	}

	// Scenario Outline: Invalid limit (the full table is in the httpapi unit tests).
	if code, body := get(t, client, base+"?limit=101"); code != http.StatusBadRequest ||
		strings.TrimSpace(body) != `{"error":{"code":"invalid_limit","message":"limit must be an integer between 1 and 100"}}` {
		t.Errorf("GET /top-senders?limit=101 = %d %q, want 400 invalid_limit", code, body)
	}

	b.signal(t)
	b.waitExit0(t)
}
