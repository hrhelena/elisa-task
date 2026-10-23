//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	"github.com/hrhelena/elisa-task/internal/httpapi"
	"github.com/hrhelena/elisa-task/internal/ingest"
	"github.com/hrhelena/elisa-task/internal/store"
)

// statusBody is the GET /status response (ADR 0011).
type statusBody struct {
	State             string           `json:"state"`
	Finished          bool             `json:"finished"`
	MessagesProcessed int64            `json:"messages_processed"`
	TotalMessages     int64            `json:"total_messages"`
	MessagesSkipped   int64            `json:"messages_skipped"`
	FilesSeen         int64            `json:"files_seen"`
	SkipReasons       map[string]int64 `json:"skip_reasons"`
	FieldErrors       map[string]int64 `json:"field_errors"`
	RunID             *int64           `json:"run_id"`
	StartedAt         *time.Time       `json:"started_at"`
	FinishedAt        *time.Time       `json:"finished_at"`
	UpdatedAt         *time.Time       `json:"updated_at"`
	Error             *string          `json:"error"`
	Dataset           *struct {
		State string `json:"state"`
		Path  string `json:"path"`
	} `json:"dataset"`
}

type startBody struct {
	RunID     *int64     `json:"run_id"`
	State     string     `json:"state"`
	StartedAt *time.Time `json:"started_at"`
	Error     *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// postStart calls POST /start and decodes the response.
func postStart(t *testing.T, client *http.Client, base string) (int, string, startBody) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/start", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST /start: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var body startBody
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decoding POST /start body %q: %v", raw, err)
	}
	return resp.StatusCode, resp.Header.Get("Location"), body
}

// getStatus calls GET /status and decodes a 200 response.
func getStatus(t *testing.T, client *http.Client, base string) statusBody {
	t.Helper()
	code, raw := get(t, client, base+"/status")
	if code != http.StatusOK {
		t.Fatalf("GET /status = %d %s, want 200", code, raw)
	}
	var st statusBody
	if err := json.Unmarshal([]byte(raw), &st); err != nil {
		t.Fatalf("decoding /status %q: %v", raw, err)
	}
	if st.SkipReasons == nil || st.FieldErrors == nil {
		t.Fatalf("null map in /status %s", raw)
	}
	return st
}

// waitRunEnded polls GET /status until run runID is no longer running
// (30s timeout, ticket #8 test plan).
func waitRunEnded(t *testing.T, client *http.Client, base string, runID int64) statusBody {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		st := getStatus(t, client, base)
		if st.RunID != nil && *st.RunID == runID && st.State != httpapi.StateRunning {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %d did not end within 30s; last status %+v", runID, st)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// writeMessage writes a maildir message file under root.
func writeMessage(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mailMessage(n int, from string) string {
	return fmt.Sprintf("Message-ID: <%d.1075855687451.JavaMail.evans@thyme>\n"+
		"Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)\n"+
		"From: %s\n"+
		"To: tim.belden@enron.com\n"+
		"Subject: Report %d\n"+
		"\n"+
		"Body of message %d.\n", n, from, n, n)
}

// validMaildir writes n valid messages into dataDir/maildir: mailbox allen-p
// from phillip.allen, mailbox lay-k from kenneth.lay (every third file).
func validMaildir(t *testing.T, dataDir string, n int) {
	t.Helper()
	for i := range n {
		mailbox, from := "allen-p", "phillip.allen@enron.com"
		if i%3 == 2 {
			mailbox, from = "lay-k", "kenneth.lay@enron.com"
		}
		writeMessage(t, filepath.Join(dataDir, "maildir"), fmt.Sprintf("%s/inbox/%d.", mailbox, i+1), mailMessage(i, from))
	}
}

// malformedMaildir writes the ticket's malformed fixture into
// dataDir/maildir: 4 valid messages, an empty file, a binary file, one with
// an invalid sender and one with an invalid date (both still messages).
// Generated at test time: an empty and a binary file do not survive the
// pre-commit hooks as checked-in fixtures.
func malformedMaildir(t *testing.T, dataDir string) {
	t.Helper()
	root := filepath.Join(dataDir, "maildir")
	for i := range 4 {
		writeMessage(t, root, fmt.Sprintf("allen-p/inbox/%d.", i+1), mailMessage(i, "phillip.allen@enron.com"))
	}
	writeMessage(t, root, "allen-p/inbox/empty.", "")
	writeMessage(t, root, "allen-p/inbox/binary.", "\x00\x01\x02\xff\xfe\x89PNG\r\n\x1a\n\x00\x00")
	writeMessage(t, root, "allen-p/inbox/sender.", strings.Replace(mailMessage(5, "x@y.z"), "From: x@y.z",
		"From: pep <performance.>", 1))
	writeMessage(t, root, "allen-p/inbox/date.", strings.Replace(mailMessage(6, "phillip.allen@enron.com"),
		"Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)", "Date: garbage", 1))
}

// startService starts the real binary on a database of its own with
// DATA_DIR=dataDir and returns its base URL.
func startService(t *testing.T, dataDir string) (*runningBinary, string, string) {
	t.Helper()
	dbURL, err := sharedDB.CreateDatabase(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	b := startBinary(t, "HTTP_ADDR=127.0.0.1:0", "DATABASE_URL="+dbURL, "DATA_DIR="+dataDir,
		"INGEST_PROGRESS_INTERVAL=100ms")
	return b, "http://" + b.waitListening(t), dbURL
}

// POST /start and GET /status end to end on the real binary with the
// malformed fixture, then a re-ingest and /top-senders.
func TestBinaryStartAndStatus(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	malformedMaildir(t, dataDir)
	b, base, _ := startService(t, dataDir)
	client := newClient()

	// Scenario: Status before any ingestion.
	st := getStatus(t, client, base)
	if st.State != "idle" || st.Finished || st.MessagesProcessed != 0 || st.RunID != nil || st.Error != nil {
		t.Errorf("status before any ingestion = %+v, want idle", st)
	}
	if st.Dataset == nil || st.Dataset.State != "ready" || st.Dataset.Path != filepath.Join(dataDir, "maildir") {
		t.Errorf("dataset section = %+v, want ready at %s", st.Dataset, filepath.Join(dataDir, "maildir"))
	}

	// Scenario: Start an ingestion.
	code, location, started := postStart(t, client, base)
	if code != http.StatusAccepted || location != "/status" || started.State != "running" ||
		started.RunID == nil || started.StartedAt == nil {
		t.Fatalf("POST /start = %d Location %q %+v, want 202 /status running with a run_id", code, location, started)
	}

	// Scenario: Malformed files are reported.
	st = waitRunEnded(t, client, base, *started.RunID)
	if st.State != "completed" || !st.Finished || st.MessagesProcessed != 6 || st.MessagesSkipped != 2 ||
		st.FilesSeen != 8 || st.TotalMessages != 6 || st.Error != nil || st.FinishedAt == nil {
		t.Errorf("status after completion = %+v, want completed, 6 processed, 2 skipped, 8 seen, 6 stored", st)
	}
	if want := map[string]int64{"empty_file": 1, "not_an_email": 1}; !maps.Equal(st.SkipReasons, want) {
		t.Errorf("skip_reasons = %v, want %v", st.SkipReasons, want)
	}
	if st.FieldErrors["invalid_sender"] != 1 || st.FieldErrors["invalid_date"] != 1 {
		t.Errorf("field_errors = %v, want invalid_sender 1 and invalid_date 1", st.FieldErrors)
	}

	// The ingested data backs /top-senders: 4 valid plus the one with a bad
	// date; the bad sender is excluded.
	if got := getSenders(t, client, base+"/top-senders"); len(got) != 1 ||
		got[0] != (store.SenderCount{Email: "phillip.allen@enron.com", Count: 5}) {
		t.Errorf("GET /top-senders = %v, want phillip.allen 5", got)
	}

	// Scenario: Start after completion re-ingests idempotently.
	code, _, again := postStart(t, client, base)
	if code != http.StatusAccepted || again.RunID == nil || *again.RunID == *started.RunID {
		t.Fatalf("second POST /start = %d %+v, want 202 with a new run_id", code, again)
	}
	st = waitRunEnded(t, client, base, *again.RunID)
	if st.State != "completed" || st.MessagesProcessed != 6 || st.TotalMessages != 6 {
		t.Errorf("status after re-ingest = %+v, want completed with 6 processed and still 6 stored", st)
	}

	b.signal(t)
	b.waitExit0(t)
}

// seedFailedRun records a failed run as a shutdown would leave it.
func seedFailedRun(t *testing.T, dbURL, errMsg string) int64 {
	t.Helper()
	st, err := store.Open(t.Context(), dbURL, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	defer st.Close()
	lock, err := st.AcquireIngestLock(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Release(context.WithoutCancel(t.Context())) }()
	run, err := st.BeginRun(t.Context(), lock)
	if err != nil {
		t.Fatal(err)
	}
	// Two files were stored before the interruption.
	if _, err := st.InsertMessages(t.Context(), []store.Message{
		{SourcePath: "allen-p/inbox/1.", Mailbox: "allen-p", Folder: "inbox"},
		{SourcePath: "allen-p/inbox/2.", Mailbox: "allen-p", Folder: "inbox"},
	}); err != nil {
		t.Fatal(err)
	}
	c := store.Counters{FilesSeen: 2, MessagesProcessed: 2, MessagesInserted: 2}
	if err := st.FinishRun(t.Context(), lock, run.ID, store.RunFailed, c, &errMsg); err != nil {
		t.Fatal(err)
	}
	return run.ID
}

// Scenarios: Failed run is not finished; Start after a failed run.
func TestBinaryStartAfterFailedRun(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	validMaildir(t, dataDir, 5)
	b, base, dbURL := startService(t, dataDir)
	client := newClient()
	failedID := seedFailedRun(t, dbURL, ingest.ErrorShutdown)

	st := getStatus(t, client, base)
	if st.State != "failed" || st.Finished || st.Error == nil || *st.Error != "interrupted: shutdown" ||
		st.RunID == nil || *st.RunID != failedID || st.TotalMessages != 2 {
		t.Errorf("status after a failed run = %+v, want failed, not finished, error \"interrupted: shutdown\"", st)
	}

	code, _, started := postStart(t, client, base)
	if code != http.StatusAccepted || started.RunID == nil || *started.RunID == failedID {
		t.Fatalf("POST /start after a failed run = %d %+v, want 202 with a new run_id", code, started)
	}
	st = waitRunEnded(t, client, base, *started.RunID)
	// The two files stored by the failed run are not counted twice.
	if st.State != "completed" || !st.Finished || st.MessagesProcessed != 5 || st.TotalMessages != 5 {
		t.Errorf("status after the new run = %+v, want completed with 5 processed and 5 stored", st)
	}

	b.signal(t)
	b.waitExit0(t)
}

// Scenario: Dataset missing (no maildir, then a maildir that is a symlink).
func TestBinaryStartDatasetMissing(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	b, base, _ := startService(t, dataDir)
	client := newClient()
	maildir := filepath.Join(dataDir, "maildir")

	check := func(what string) {
		t.Helper()
		code, _, body := postStart(t, client, base)
		if code != http.StatusConflict || body.Error == nil || body.Error.Code != "dataset_missing" ||
			body.Error.Message != "maildir not found at "+maildir+"; call POST /fetch first" {
			t.Errorf("POST /start with %s = %d %+v, want 409 dataset_missing naming %s", what, code, body.Error, maildir)
		}
	}
	check("no maildir")

	target := t.TempDir()
	validMaildir(t, target, 1)
	if err := os.Symlink(filepath.Join(target, "maildir"), maildir); err != nil {
		t.Fatal(err)
	}
	check("a symlinked maildir")

	if st := getStatus(t, client, base); st.State != "idle" {
		t.Errorf("status after rejected starts = %+v, want idle", st)
	}
	b.signal(t)
	b.waitExit0(t)
}

// gatedSource yields the first `after` entries of a maildir, then waits for
// open to be closed (or the run to be canceled) before yielding the rest.
type gatedSource struct {
	ingest.Source
	after int
	open  <-chan struct{}
}

func (s gatedSource) Walk(ctx context.Context, fn func(string, ingest.OpenFunc) error) error {
	n := 0
	return s.Source.Walk(ctx, func(rel string, open ingest.OpenFunc) error { //nolint:wrapcheck // test double
		if n == s.after {
			select {
			case <-s.open:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		n++
		return fn(rel, open)
	})
}

// replica is one in-process service instance: its own pool, ingestion
// service and HTTP handler, wired like run does.
type replica struct {
	base string
	svc  *ingest.Service
	st   *store.Postgres
	srv  *httptest.Server
}

func newReplica(t *testing.T, dbURL string, src ingest.Source, maildir string) *replica {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	st, err := store.Open(t.Context(), dbURL, logger)
	if err != nil {
		t.Fatal(err)
	}
	svc := ingest.NewService(src, st, ingest.Config{ProgressInterval: 50 * time.Millisecond}, logger, nil)
	h := httpapi.New(httpapi.Deps{
		Ingester: ingestion{svc: svc, lock: st, maildir: maildir, logger: logger},
		Senders:  senderStats{st},
	}, logger)
	srv := httptest.NewServer(h)
	r := &replica{base: srv.URL, svc: svc, st: st, srv: srv}
	t.Cleanup(r.close)
	return r
}

func (r *replica) close() {
	r.srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = r.svc.Close(ctx)
	r.st.Close()
}

// Scenarios: Concurrent starts on two replicas; Status while running; Start
// while running; Status consistent across replicas. Two service instances
// share one database; the run is held open after 2 of 5 files so that the
// running state is observable without timing assumptions.
func TestReplicasStartAndStatus(t *testing.T) {
	t.Parallel()
	dbURL, err := sharedDB.CreateDatabase(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	validMaildir(t, dataDir, 5)
	maildir := filepath.Join(dataDir, "maildir")
	gate := make(chan struct{})
	src := gatedSource{Source: ingest.NewMaildirSource(maildir), after: 2, open: gate}
	a := newReplica(t, dbURL, src, maildir)
	b := newReplica(t, dbURL, src, maildir)
	client := newClient()

	// Both receive POST /start at the same time: exactly one 202.
	type result struct {
		code int
		body startBody
	}
	results := make([]result, 2)
	var wg sync.WaitGroup
	for i, r := range []*replica{a, b} {
		wg.Go(func() {
			code, _, body := postStart(t, client, r.base)
			results[i] = result{code, body}
		})
	}
	wg.Wait()
	var runID int64
	accepted, conflicts := 0, 0
	for _, res := range results {
		switch {
		case res.code == http.StatusAccepted && res.body.RunID != nil:
			accepted++
			runID = *res.body.RunID
		case res.code == http.StatusConflict && res.body.Error != nil && res.body.Error.Code == "ingestion_running":
			conflicts++
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("concurrent POST /start results = %+v, want one 202 and one 409 ingestion_running", results)
	}

	// Status while running, on both replicas.
	for name, r := range map[string]*replica{"A": a, "B": b} {
		st := getStatus(t, client, r.base)
		if st.State != "running" || st.Finished || st.RunID == nil || *st.RunID != runID ||
			st.MessagesProcessed < 0 || st.MessagesProcessed > 5 || st.FinishedAt != nil {
			t.Errorf("replica %s status while running = %+v, want running run %d with 0..5 processed", name, st, runID)
		}
	}
	// Start while running, on both replicas.
	for name, r := range map[string]*replica{"A": a, "B": b} {
		if code, _, body := postStart(t, client, r.base); code != http.StatusConflict ||
			body.Error == nil || body.Error.Code != "ingestion_running" {
			t.Errorf("replica %s POST /start while running = %d %+v, want 409 ingestion_running", name, code, body)
		}
	}

	close(gate)
	stA := waitRunEnded(t, client, a.base, runID)
	stB := getStatus(t, client, b.base)
	if stA.State != "completed" || stA.MessagesProcessed != 5 || stA.TotalMessages != 5 {
		t.Errorf("replica A status = %+v, want completed with 5 processed", stA)
	}
	if stB.State != stA.State || stB.MessagesProcessed != stA.MessagesProcessed || *stB.RunID != *stA.RunID ||
		stB.TotalMessages != stA.TotalMessages {
		t.Errorf("replica B status = %+v, want the same as replica A %+v", stB, stA)
	}
}
