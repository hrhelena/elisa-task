//go:build integration

package ingest

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hrhelena/elisa-task/internal/store"
	"github.com/hrhelena/elisa-task/internal/store/storetest"
)

var itEnv struct {
	baseURL string
	admin   *pgxpool.Pool
	dbSeq   atomic.Int64
}

// runTests starts one Postgres container for the package; each test gets
// its own database, so the per-database advisory lock never crosses tests.
func runTests(m *testing.M) int {
	ctx := context.Background()
	pg, err := storetest.Start(ctx)
	if err != nil {
		log.Print(err)
		return 1
	}
	defer func() {
		if err := pg.Terminate(); err != nil {
			log.Printf("terminating postgres container: %v", err)
		}
	}()
	itEnv.baseURL = pg.URL
	if itEnv.admin, err = pgxpool.New(ctx, pg.URL); err != nil {
		log.Printf("admin pool: %v", err)
		return 1
	}
	defer itEnv.admin.Close()
	return m.Run()
}

// newDatabase creates an empty database and returns its name and URL.
func newDatabase(t *testing.T) (name, dbURL string) {
	t.Helper()
	name = fmt.Sprintf("ingest_%d", itEnv.dbSeq.Add(1))
	ctx := context.Background()
	if _, err := itEnv.admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("creating database %s: %v", name, err)
	}
	t.Cleanup(func() {
		if _, err := itEnv.admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("dropping database %s: %v", name, err)
		}
	})
	u, err := url.Parse(itEnv.baseURL)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return name, u.String()
}

func openPostgres(t *testing.T, dbURL string) *store.Postgres {
	t.Helper()
	st, err := store.Open(context.Background(), dbURL, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

// newPostgres returns a store on a fresh database and the database name.
func newPostgres(t *testing.T) (*store.Postgres, string) {
	t.Helper()
	name, dbURL := newDatabase(t)
	return openPostgres(t, dbURL), name
}

func latest(t *testing.T, st *store.Postgres) *store.Run {
	t.Helper()
	run, err := st.LatestRun(t.Context())
	if err != nil {
		t.Fatalf("LatestRun: %v", err)
	}
	return run
}

func count(t *testing.T, st *store.Postgres) int64 {
	t.Helper()
	n, err := st.CountMessages(t.Context())
	if err != nil {
		t.Fatalf("CountMessages: %v", err)
	}
	return n
}

func lockHeld(t *testing.T, st *store.Postgres) bool {
	t.Helper()
	held, err := st.IngestLockHeld(t.Context())
	if err != nil {
		t.Fatalf("IngestLockHeld: %v", err)
	}
	return held
}

func TestIntegrationIngestASmallMaildir(t *testing.T) {
	t.Parallel()
	name, dbURL := newDatabase(t)
	st := openPostgres(t, dbURL)
	s := newTestService(t, NewMaildirSource(smallMaildir(t)), st)
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 5, 5, 5, 0)
	if n := count(t, st); n != 5 {
		t.Errorf("CountMessages = %d, want 5", n)
	}

	conn, err := pgx.Connect(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var mailbox, folder, from string
	var runID int64
	if err := conn.QueryRow(t.Context(),
		"SELECT mailbox, folder, from_addr, run_id FROM messages WHERE source_path = $1",
		"allen-p/_sent_mail/1.").Scan(&mailbox, &folder, &from, &runID); err != nil {
		t.Fatalf("reading row in %s: %v", name, err)
	}
	if mailbox != "allen-p" || folder != "_sent_mail" || from != "phillip.allen@enron.com" || runID != run.ID {
		t.Errorf("row = %q/%q/%q/run %d", mailbox, folder, from, runID)
	}
	if lockHeld(t, st) {
		t.Error("ingest lock still held after the run")
	}
}

func TestIntegrationMalformedEntriesAreSkippedOrFlaggedNeverFatal(t *testing.T) {
	t.Parallel()
	st, _ := newPostgres(t)
	s := newTestService(t, NewMaildirSource(malformedMaildir(t)), st)
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 8, 6, 6, 2)
	if want := map[string]int64{"empty_file": 1, "not_an_email": 1}; !maps.Equal(run.SkipReasons, want) {
		t.Errorf("skip_reasons = %v, want %v", run.SkipReasons, want)
	}
	if run.FieldErrors["invalid_sender"] != 1 || run.FieldErrors["invalid_date"] != 1 {
		t.Errorf("field_errors = %v", run.FieldErrors)
	}
	if n := count(t, st); n != 6 {
		t.Errorf("CountMessages = %d, want 6", n)
	}
}

func TestIntegrationDatasetMissing(t *testing.T) {
	t.Parallel()
	st, _ := newPostgres(t)
	s := newTestService(t, NewMaildirSource(t.TempDir()+"/maildir"), st)
	if _, err := s.Start(t.Context()); !errors.Is(err, ErrDatasetMissing) {
		t.Fatalf("Start = %v, want ErrDatasetMissing", err)
	}
	if run := latest(t, st); run != nil {
		t.Errorf("a run was created: %+v", run)
	}
}

// Two replicas: two services, each with its own store (connection pool), on
// one database.
func TestIntegrationConcurrentStartIsSingleFlightAcrossReplicas(t *testing.T) {
	t.Parallel()
	_, dbURL := newDatabase(t)
	src := &memSource{files: memMessages(4), blockAfter: 2}
	a := newTestService(t, src, openPostgres(t, dbURL))
	b := newTestService(t, src, openPostgres(t, dbURL))

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, s := range []*Service{a, b} {
		go func() {
			<-start
			_, err := s.Start(t.Context())
			errs <- err
		}()
	}
	close(start)
	var ok, busy int
	for range 2 {
		switch err := <-errs; {
		case err == nil:
			ok++
		case errors.Is(err, ErrAlreadyRunning):
			busy++
		default:
			t.Errorf("Start: %v", err)
		}
	}
	if ok != 1 || busy != 1 {
		t.Errorf("started %d, already running %d; want 1 and 1", ok, busy)
	}
}

func TestIntegrationReRunIsIdempotent(t *testing.T) {
	t.Parallel()
	st, _ := newPostgres(t)
	s := newTestService(t, NewMaildirSource(smallMaildir(t)), st)
	mustStart(t, s)
	requireCompleted(t, waitDone(t, s))

	second := mustStart(t, s)
	run := waitDone(t, s)
	if run.ID != second.ID {
		t.Fatalf("latest run %d, want %d", run.ID, second.ID)
	}
	requireCompleted(t, run)
	requireCounts(t, run, 5, 5, 0, 0)
	if n := count(t, st); n != 5 {
		t.Errorf("CountMessages = %d, want 5", n)
	}
}

func TestIntegrationReRunAfterAFailedRunResumesWithoutDoubleCounting(t *testing.T) {
	t.Parallel()
	st, _ := newPostgres(t)
	ctx := t.Context()
	// A failed run that stored 3 of the 5 files.
	lock, err := st.AcquireIngestLock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := st.BeginRun(ctx, lock)
	if err != nil {
		t.Fatal(err)
	}
	var msgs []store.Message
	for _, rel := range smallFiles[:3] {
		mailbox, folder, _ := splitPath(rel)
		msgs = append(msgs, store.Message{SourcePath: rel, Mailbox: mailbox, Folder: folder, RunID: failed.ID})
	}
	if _, err := st.InsertMessages(ctx, msgs); err != nil {
		t.Fatal(err)
	}
	msg := "batch insert of 2000 messages failed after 4 attempts"
	if err := st.FinishRun(ctx, lock, failed.ID, store.RunFailed, store.Counters{FilesSeen: 3}, &msg); err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatal(err)
	}

	s := newTestService(t, NewMaildirSource(smallMaildir(t)), st)
	mustStart(t, s)
	run := waitDone(t, s)
	requireCompleted(t, run)
	requireCounts(t, run, 5, 5, 2, 0)
	if n := count(t, st); n != 5 {
		t.Errorf("CountMessages = %d, want 5", n)
	}
}

func TestIntegrationShutdownDuringARun(t *testing.T) {
	t.Parallel()
	st, _ := newPostgres(t)
	s := newTestService(t, &memSource{files: memMessages(10), blockAfter: 5}, st)
	mustStart(t, s)
	waitFor(t, "progress to be persisted", func() bool {
		run := latest(t, st)
		return run.MessagesProcessed == 5
	})

	if err := s.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	run := latest(t, st)
	if run.State != store.RunFailed || deref(run.Error) != ErrorShutdown {
		t.Fatalf("run state = %q, error = %q; want failed %q", run.State, deref(run.Error), ErrorShutdown)
	}
	if run.FilesSeen != 5 || run.MessagesProcessed != 5 || run.FinishedAt == nil {
		t.Errorf("run = %+v; want the flushed counters 5/5 and a finish time", run.Counters)
	}
	if lockHeld(t, st) {
		t.Error("ingest lock still held after Close")
	}
}

func TestIntegrationCrashedOwnerIsDetected(t *testing.T) {
	t.Parallel()
	_, dbURL := newDatabase(t)
	crashed, err := store.Open(t.Context(), dbURL, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	lock, err := crashed.AcquireIngestLock(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crashed.BeginRun(t.Context(), lock); err != nil {
		t.Fatal(err)
	}
	crashed.Close() // the owner's sessions end without finishing the run

	st := openPostgres(t, dbURL)
	s := newTestService(t, &memSource{}, st)
	waitFor(t, "the dead session's lock to be released", func() bool { return !lockHeld(t, st) })
	status, err := s.Status(t.Context())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != StateFailed || deref(status.Run.Error) != store.ErrorInterrupted {
		t.Errorf("state = %q, error = %q; want failed interrupted", status.State, deref(status.Run.Error))
	}
}

// The ingest lock's session is killed mid-run, as an idle-connection killer
// or a failover would: the engine stops its workers and records lock_lost.
func TestIntegrationLockLossStopsTheRun(t *testing.T) {
	t.Parallel()
	st, dbName := newPostgres(t)
	s := newTestService(t, &memSource{files: memMessages(10), blockAfter: 5}, st)
	mustStart(t, s)
	waitFor(t, "progress to be persisted", func() bool { return latest(t, st).MessagesProcessed == 5 })

	var terminated bool
	if err := itEnv.admin.QueryRow(t.Context(), `SELECT pg_terminate_backend(pid) FROM pg_locks
WHERE locktype = 'advisory' AND granted AND objid::bigint = $1
  AND database = (SELECT oid FROM pg_database WHERE datname = $2)`,
		store.IngestLockKey&0xffffffff, dbName).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("pg_terminate_backend = %v, %v", terminated, err)
	}

	run := waitDone(t, s) // the walk is blocked: only lock-loss detection ends the run
	if run.State != store.RunFailed || deref(run.Error) != ErrorLockLost {
		t.Fatalf("run state = %q, error = %q; want failed %q", run.State, deref(run.Error), ErrorLockLost)
	}
	if run.MessagesProcessed != 5 {
		t.Errorf("messages_processed = %d, want 5", run.MessagesProcessed)
	}
	if lockHeld(t, st) {
		t.Error("ingest lock still held")
	}
	// A new run can start.
	s2 := newTestService(t, NewMaildirSource(smallMaildir(t)), st)
	mustStart(t, s2)
	requireCompleted(t, waitDone(t, s2))
}

// Inserts fail on the database side for longer than the retry budget, while
// the run-state writes keep working: the run fails with the insert error and
// the service keeps answering.
func TestIntegrationDatabaseErrorDuringARunFailsIt(t *testing.T) {
	t.Parallel()
	_, dbURL := newDatabase(t)
	st := openPostgres(t, dbURL)
	conn, err := pgx.Connect(t.Context(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(t.Context(), "ALTER TABLE messages RENAME TO messages_gone"); err != nil {
		t.Fatal(err)
	}

	s := newTestService(t, &memSource{files: memMessages(4)}, st)
	mustStart(t, s)
	waitFor(t, "the run to fail", func() bool { return latest(t, st).State != store.RunRunning })
	run := latest(t, st)
	if run.State != store.RunFailed || !strings.Contains(deref(run.Error), "batch insert") {
		t.Fatalf("run state = %q, error = %q; want failed mentioning the batch insert", run.State, deref(run.Error))
	}
	// The database recovers; the service still answers.
	if _, err := conn.Exec(t.Context(), "ALTER TABLE messages_gone RENAME TO messages"); err != nil {
		t.Fatal(err)
	}
	waitDone(t, s)
	if lockHeld(t, st) {
		t.Error("ingest lock still held")
	}
}

// TestIntegrationFullCorpus ingests a local copy of the full dataset and
// logs the timing. It is skipped unless ENRON_MAILDIR is set, for example:
//
//	ENRON_MAILDIR=$PWD/Dataset/maildir go test -tags integration -run FullCorpus -v -timeout 30m ./internal/ingest
func TestIntegrationFullCorpus(t *testing.T) {
	root := os.Getenv("ENRON_MAILDIR")
	if root == "" {
		t.Skip("ENRON_MAILDIR not set")
	}
	st, _ := newPostgres(t)
	cfg := Config{Workers: runtime.NumCPU(), BatchSize: 2000, ProgressInterval: 5 * time.Second}
	s := NewService(NewMaildirSource(root), st, cfg, slog.New(slog.DiscardHandler), nil)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	started := time.Now()
	mustStart(t, s)
	run := waitDoneWithin(t, s, 30*time.Minute)
	elapsed := time.Since(started)
	requireCompleted(t, run)

	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	maxRSS := ru.Maxrss // bytes on darwin, KiB on linux
	if runtime.GOOS == "linux" {
		maxRSS *= 1024
	}
	top, err := st.TopSenders(t.Context(), 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("full corpus: %s, %.0f files/s, peak RSS %.0f MiB, workers %d, batch %d",
		elapsed.Round(time.Millisecond), float64(run.FilesSeen)/elapsed.Seconds(), float64(maxRSS)/(1<<20),
		cfg.Workers, cfg.BatchSize)
	t.Logf("counters: files_seen=%d processed=%d inserted=%d skipped=%d skip_reasons=%v field_errors=%v",
		run.FilesSeen, run.MessagesProcessed, run.MessagesInserted, run.MessagesSkipped,
		run.SkipReasons, run.FieldErrors)
	t.Logf("total_messages=%d top senders: %v", count(t, st), top)
}

func waitDoneWithin(t *testing.T, s *Service, d time.Duration) *store.Run {
	t.Helper()
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-time.After(d):
			t.Fatalf("run did not finish within %s", d)
		}
	}
	return waitDone(t, s)
}
