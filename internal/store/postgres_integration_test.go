//go:build integration

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/goleak"

	"github.com/hrhelena/elisa-task/internal/store/storetest"
)

// testEnv is the package-wide Postgres container shared by all tests. Each
// test gets its own database in it (newDatabase), so tests run in parallel
// and every test starts from an empty, unmigrated database.
var testEnv struct {
	pg      *storetest.Postgres
	baseURL string        // connection string to the container's default database
	admin   *pgxpool.Pool // used for CREATE/DROP DATABASE
	dbSeq   atomic.Int64
}

// leakIgnores lists background goroutines allowed to outlive the
// integration tests. It is empty: the leak check runs after runTests has
// terminated the container, and by then testcontainers-go (v0.44) leaves no
// goroutine behind, its reaper included. Add a targeted
// goleak.IgnoreTopFunction here if a future version does.
var leakIgnores []goleak.Option

// runTests starts the shared Postgres container, runs the tests and
// terminates the container; TestMain then checks for goroutine leaks.
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

	testEnv.pg = pg
	testEnv.baseURL = pg.URL
	testEnv.admin, err = pgxpool.New(ctx, testEnv.baseURL)
	if err != nil {
		log.Printf("admin pool: %v", err)
		return 1
	}
	defer testEnv.admin.Close()
	return m.Run()
}

// newDatabase creates an empty database for one test and returns its URL.
//
// The database uses the ICU en-US collation, not the image's default. The
// alpine image (musl) sorts text in byte order, which would hide any query
// whose result depends on the database collation; glibc servers (Debian,
// RDS, Cloud SQL) and ICU sort punctuation by locale rules instead. Testing
// under a locale-aware collation makes such dependencies fail here.
func newDatabase(tb testing.TB) string {
	tb.Helper()
	name := fmt.Sprintf("test_%d", testEnv.dbSeq.Add(1))
	ctx := context.Background()
	create := "CREATE DATABASE " + pgx.Identifier{name}.Sanitize() +
		" TEMPLATE template0 ENCODING 'UTF8' LOCALE_PROVIDER icu ICU_LOCALE 'en-US' LOCALE 'C'"
	if _, err := testEnv.admin.Exec(ctx, create); err != nil {
		tb.Fatalf("creating database %s: %v", name, err)
	}
	tb.Cleanup(func() {
		if _, err := testEnv.admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			tb.Errorf("dropping database %s: %v", name, err)
		}
	})
	u, err := url.Parse(testEnv.baseURL)
	if err != nil {
		tb.Fatalf("parsing base URL: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// openStore opens a store on a fresh database and closes it at cleanup.
func openStore(tb testing.TB) *Postgres {
	tb.Helper()
	return openStoreAt(tb, newDatabase(tb))
}

func openStoreAt(tb testing.TB, dbURL string) *Postgres {
	tb.Helper()
	s, err := Open(context.Background(), dbURL, discardLogger())
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(s.Close)
	return s
}

func msg(path, from string) Message {
	m := Message{SourcePath: path, Mailbox: path[:bytes.IndexByte([]byte(path), '/')], Folder: "inbox"}
	if from != "" {
		m.FromAddr = &from
	}
	return m
}

func mustInsert(t *testing.T, s *Postgres, msgs []Message) int64 {
	t.Helper()
	n, err := s.InsertMessages(t.Context(), msgs)
	if err != nil {
		t.Fatalf("InsertMessages: %v", err)
	}
	return n
}

func mustCount(t *testing.T, s *Postgres) int64 {
	t.Helper()
	n, err := s.CountMessages(t.Context())
	if err != nil {
		t.Fatalf("CountMessages: %v", err)
	}
	return n
}

func mustTopSenders(t *testing.T, s *Postgres, limit int) []SenderCount {
	t.Helper()
	got, err := s.TopSenders(t.Context(), limit)
	if err != nil {
		t.Fatalf("TopSenders(%d): %v", limit, err)
	}
	return got
}

func regclassExists(t *testing.T, s *Postgres, name string) bool {
	t.Helper()
	var exists bool
	if err := s.pool.QueryRow(t.Context(), "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	return exists
}

var threeMessages = []Message{
	msg("allen-p/inbox/1.", "phillip.allen@enron.com"),
	msg("allen-p/inbox/2.", "phillip.allen@enron.com"),
	msg("lay-k/sent/1.", "kenneth.lay@enron.com"),
}

// Scenario: Migrations run on an empty database.
func TestMigrationsRunOnEmptyDatabase(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	for _, name := range []string{"public.messages", "public.messages_from_addr_idx"} {
		if !regclassExists(t, s, name) {
			t.Errorf("%s does not exist after Open", name)
		}
	}
}

// Scenario: Migrations are idempotent and safe in parallel.
func TestMigrationsAreIdempotentAndSafeInParallel(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)

	const instances = 2
	start := make(chan struct{})
	stores := make([]*Postgres, instances)
	errs := make([]error, instances)
	var wg sync.WaitGroup
	for i := range instances {
		wg.Go(func() {
			<-start
			stores[i], errs[i] = Open(t.Context(), dbURL, discardLogger())
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("instance %d: Open: %v", i, err)
		}
		t.Cleanup(stores[i].Close)
	}

	// A later start (rolling restart) finds nothing to apply.
	again := openStoreAt(t, dbURL)

	var applied int
	if err := again.pool.QueryRow(t.Context(),
		"SELECT count(*) FROM goose_db_version WHERE version_id = 1 AND is_applied").Scan(&applied); err != nil {
		t.Fatalf("reading goose_db_version: %v", err)
	}
	if applied != 1 {
		t.Errorf("migration 1 recorded %d times, want exactly 1", applied)
	}
	if n := mustCount(t, again); n != 0 {
		t.Errorf("CountMessages = %d, want 0", n)
	}
}

// Scenario: Batch insert.
func TestBatchInsert(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	if n := mustInsert(t, s, threeMessages); n != 3 {
		t.Errorf("inserted = %d, want 3", n)
	}
	if n := mustCount(t, s); n != 3 {
		t.Errorf("CountMessages = %d, want 3", n)
	}
}

// Scenario: Re-inserting the same files is idempotent.
func TestReinsertingSameFilesIsIdempotent(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	mustInsert(t, s, threeMessages)

	if n := mustInsert(t, s, threeMessages); n != 0 {
		t.Errorf("re-insert inserted = %d, want 0", n)
	}
	if n := mustCount(t, s); n != 3 {
		t.Errorf("CountMessages = %d, want 3", n)
	}

	// A batch mixing stored and new paths inserts only the new ones.
	mixed := append(slices.Clone(threeMessages), msg("lay-k/sent/2.", "kenneth.lay@enron.com"))
	if n := mustInsert(t, s, mixed); n != 1 {
		t.Errorf("mixed batch inserted = %d, want 1", n)
	}
	if n := mustCount(t, s); n != 4 {
		t.Errorf("CountMessages = %d, want 4", n)
	}
}

func TestDuplicatePathsWithinOneBatch(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	batch := []Message{msg("allen-p/inbox/1.", "a@enron.com"), msg("allen-p/inbox/1.", "b@enron.com")}
	if n := mustInsert(t, s, batch); n != 1 {
		t.Errorf("inserted = %d, want 1", n)
	}
	if n := mustCount(t, s); n != 1 {
		t.Errorf("CountMessages = %d, want 1", n)
	}
}

// Concurrent writers whose batches overlap, in opposite orders, must neither
// fail nor double-count. Single-flight ingestion (ADR 0010) makes this rare,
// but the merge inserts in source_path order, so overlapping batches take
// their unique-index locks in the same order and cannot deadlock.
func TestConcurrentInsertsWithOverlappingPaths(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	batch := make([]Message, 500)
	for i := range batch {
		batch[i] = msg(fmt.Sprintf("allen-p/inbox/%03d.", i), "phillip.allen@enron.com")
	}
	reversed := slices.Clone(batch)
	slices.Reverse(reversed)

	const writers = 6
	var total atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	start := make(chan struct{})
	for w := range writers {
		mine := batch
		if w%2 == 1 {
			mine = reversed
		}
		wg.Go(func() {
			<-start
			n, err := s.InsertMessages(t.Context(), mine)
			if err != nil {
				errs <- err
				return
			}
			total.Add(n)
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("InsertMessages: %v", err)
	}
	if got := total.Load(); got != int64(len(batch)) {
		t.Errorf("sum of inserted = %d, want %d", got, len(batch))
	}
	if n := mustCount(t, s); n != int64(len(batch)) {
		t.Errorf("CountMessages = %d, want %d", n, len(batch))
	}
}

// Scenario: Text with NUL bytes and invalid UTF-8 is stored safely.
func TestTextWithNULAndInvalidUTF8IsStoredSafely(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	m := msg("allen-p/inbox/1.", "")
	m.Subject = ptr("Q3\x00 report \xff")
	m.FromRaw = ptr("bad\x00 <\xfe@enron.com>")
	m.To = []string{"ok@enron.com", "nul\x00@enron.com"}
	mustInsert(t, s, []Message{m})

	var subject, fromRaw string
	var to []string
	if err := s.pool.QueryRow(t.Context(),
		"SELECT subject, from_raw, to_addrs FROM messages WHERE source_path = $1", m.SourcePath,
	).Scan(&subject, &fromRaw, &to); err != nil {
		t.Fatalf("reading row: %v", err)
	}
	if want := "Q3 report �"; subject != want {
		t.Errorf("subject = %q, want %q", subject, want)
	}
	if want := "bad <�@enron.com>"; fromRaw != want {
		t.Errorf("from_raw = %q, want %q", fromRaw, want)
	}
	if want := []string{"ok@enron.com", "nul@enron.com"}; !slices.Equal(to, want) {
		t.Errorf("to_addrs = %q, want %q", to, want)
	}
}

func TestInsertRoundTripsAllFields(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	sent := time.Date(2001, 5, 14, 23, 39, 0, 0, time.UTC)
	hash := bytes.Repeat([]byte{0x5a}, 32)
	full := Message{
		SourcePath: "allen-p/_sent_mail/1.", Mailbox: "allen-p", Folder: "_sent_mail",
		MessageID: ptr("<18782981.1075855378110.JavaMail.evans@thyme>"),
		DateRaw:   ptr("Mon, 14 May 2001 16:39:00 -0700 (PDT)"),
		FromAddr:  ptr("phillip.allen@enron.com"), FromRaw: ptr("phillip.allen@enron.com"), XFrom: ptr("Phillip K Allen"),
		Subject: ptr("Re: forecast"), SentAt: &sent,
		To: []string{"tim.belden@enron.com"}, Cc: []string{"a@enron.com", "b@enron.com"},
		ContentSHA256: hash,
	}
	bare := Message{SourcePath: "lay-k/1.", Mailbox: "lay-k", Folder: ""}
	mustInsert(t, s, []Message{full, bare})

	type row struct {
		mailbox, folder                              string
		messageID, dateRaw, fromAddr, fromRaw, xFrom *string
		subject                                      *string
		sentAt                                       *time.Time
		to, cc, bcc                                  []string
		hash                                         []byte
		runID                                        *int64
		ingestedRecently, arraysNotNull              bool
	}
	read := func(path string) row {
		t.Helper()
		var r row
		err := s.pool.QueryRow(t.Context(), `SELECT mailbox, folder, message_id, date_raw, from_addr, from_raw, x_from,
			subject, sent_at, to_addrs, cc_addrs, bcc_addrs, content_sha256, run_id,
			ingested_at > now() - interval '1 minute',
			to_addrs IS NOT NULL AND cc_addrs IS NOT NULL AND bcc_addrs IS NOT NULL
			FROM messages WHERE source_path = $1`, path).Scan(
			&r.mailbox, &r.folder, &r.messageID, &r.dateRaw, &r.fromAddr, &r.fromRaw, &r.xFrom,
			&r.subject, &r.sentAt, &r.to, &r.cc, &r.bcc, &r.hash, &r.runID, &r.ingestedRecently, &r.arraysNotNull)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		return r
	}

	f := read(full.SourcePath)
	if f.mailbox != "allen-p" || f.folder != "_sent_mail" {
		t.Errorf("mailbox/folder = %q/%q", f.mailbox, f.folder)
	}
	for name, pair := range map[string][2]*string{
		"message_id": {f.messageID, full.MessageID}, "date_raw": {f.dateRaw, full.DateRaw},
		"from_addr": {f.fromAddr, full.FromAddr}, "from_raw": {f.fromRaw, full.FromRaw},
		"x_from": {f.xFrom, full.XFrom}, "subject": {f.subject, full.Subject},
	} {
		if pair[0] == nil || *pair[0] != *pair[1] {
			t.Errorf("%s = %v, want %q", name, pair[0], *pair[1])
		}
	}
	if f.sentAt == nil || !f.sentAt.Equal(sent) {
		t.Errorf("sent_at = %v, want %v", f.sentAt, sent)
	}
	if !slices.Equal(f.to, full.To) || !slices.Equal(f.cc, full.Cc) || len(f.bcc) != 0 {
		t.Errorf("to/cc/bcc = %q/%q/%q", f.to, f.cc, f.bcc)
	}
	if !bytes.Equal(f.hash, hash) {
		t.Errorf("content_sha256 = %x, want %x", f.hash, hash)
	}
	if f.runID != nil {
		t.Errorf("run_id = %d, want NULL for RunID 0", *f.runID)
	}
	if !f.ingestedRecently {
		t.Error("ingested_at not set to now()")
	}

	b := read(bare.SourcePath)
	if b.messageID != nil || b.dateRaw != nil || b.fromAddr != nil || b.fromRaw != nil || b.xFrom != nil ||
		b.subject != nil || b.sentAt != nil || b.hash != nil || b.runID != nil {
		t.Errorf("optional fields of a bare message should all be NULL: %+v", b)
	}
	if !b.arraysNotNull || len(b.to)+len(b.cc)+len(b.bcc) != 0 {
		t.Errorf("address arrays should be '{}' not NULL: notNull=%v to=%q cc=%q bcc=%q", b.arraysNotNull, b.to, b.cc, b.bcc)
	}
}

// Scenario: Missing sender is stored as NULL and excluded from top senders.
func TestMissingSenderIsStoredAsNullAndExcludedFromTopSenders(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	mustInsert(t, s, []Message{
		msg("allen-p/inbox/1.", "phillip.allen@enron.com"),
		msg("allen-p/inbox/2.", "phillip.allen@enron.com"),
		msg("allen-p/inbox/3.", ""),
	})

	want := []SenderCount{{Email: "phillip.allen@enron.com", Count: 2}}
	if got := mustTopSenders(t, s, 10); !slices.Equal(got, want) {
		t.Errorf("TopSenders(10) = %v, want %v", got, want)
	}
	if n := mustCount(t, s); n != 3 {
		t.Errorf("CountMessages = %d, want 3", n)
	}
	var nulls int
	if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM messages WHERE from_addr IS NULL").Scan(&nulls); err != nil {
		t.Fatalf("counting NULL senders: %v", err)
	}
	if nulls != 1 {
		t.Errorf("rows with NULL from_addr = %d, want 1", nulls)
	}
}

// An empty sender, or one that is empty after sanitizing, is "no sender":
// stored as NULL and never ranked as "".
func TestEmptySenderIsExcludedFromTopSenders(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	empty, nul := msg("allen-p/inbox/1.", ""), msg("allen-p/inbox/2.", "")
	empty.FromAddr, nul.FromAddr = ptr(""), ptr("\x00")
	mustInsert(t, s, []Message{empty, nul, msg("allen-p/inbox/3.", "a@enron.com")})

	want := []SenderCount{{Email: "a@enron.com", Count: 1}}
	if got := mustTopSenders(t, s, 10); !slices.Equal(got, want) {
		t.Errorf("TopSenders(10) = %v, want %v", got, want)
	}
	var nulls int
	if err := s.pool.QueryRow(t.Context(), "SELECT count(*) FROM messages WHERE from_addr IS NULL").Scan(&nulls); err != nil {
		t.Fatalf("counting NULL senders: %v", err)
	}
	if nulls != 2 {
		t.Errorf("rows with NULL from_addr = %d, want 2", nulls)
	}
}

// Scenario: Ties are ordered by address.
func TestTopSendersTiesAreOrderedByAddress(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	mustInsert(t, s, []Message{
		msg("b/inbox/1.", "b@enron.com"),
		msg("a/inbox/1.", "a@enron.com"),
	})

	if got, want := mustTopSenders(t, s, 1), []SenderCount{{"a@enron.com", 1}}; !slices.Equal(got, want) {
		t.Errorf("TopSenders(1) = %v, want %v", got, want)
	}

	// Count dominates, address breaks ties, limit caps the result.
	mustInsert(t, s, []Message{
		msg("c/inbox/1.", "c@enron.com"),
		msg("c/inbox/2.", "c@enron.com"),
		msg("d/inbox/1.", "d@enron.com"),
	})
	want := []SenderCount{{"c@enron.com", 2}, {"a@enron.com", 1}, {"b@enron.com", 1}}
	if got := mustTopSenders(t, s, 3); !slices.Equal(got, want) {
		t.Errorf("TopSenders(3) = %v, want %v", got, want)
	}
}

// Ties break in byte order (as Go's slices.Sort), whatever the database
// collation: punctuation-leading addresses kept by ADR 0012 (".ward@") must
// rank the same on alpine/musl, glibc and ICU servers. newDatabase uses ICU
// en-US, under which locale-aware order would differ from byte order.
func TestTopSendersTiesUseByteOrderRegardlessOfCollation(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	var provider string
	if err := s.pool.QueryRow(t.Context(),
		"SELECT datlocprovider::text FROM pg_database WHERE datname = current_database()").Scan(&provider); err != nil {
		t.Fatalf("reading locale provider: %v", err)
	}
	if provider != "i" {
		t.Fatalf("test database locale provider = %q, want ICU (i); the test could not detect collation dependence", provider)
	}

	addrs := []string{"ab@enron.com", "a_z@enron.com", "a@enron.com", ".ward@enron.com", "a.b@enron.com", "B@enron.com"}
	msgs := make([]Message, len(addrs))
	for i, a := range addrs {
		msgs[i] = msg(fmt.Sprintf("x/inbox/%d.", i), a)
	}
	mustInsert(t, s, msgs)

	sorted := slices.Clone(addrs)
	slices.Sort(sorted) // byte order: ".ward@" < "B@" < "a.b@" < "a@" < "a_z@" < "ab@"
	want := make([]SenderCount, len(sorted))
	for i, a := range sorted {
		want[i] = SenderCount{Email: a, Count: 1}
	}
	if got := mustTopSenders(t, s, 10); !slices.Equal(got, want) {
		t.Errorf("TopSenders(10) =\n  %v\nwant byte order\n  %v", got, want)
	}
	if got := mustTopSenders(t, s, 1); !slices.Equal(got, want[:1]) {
		t.Errorf("TopSenders(1) = %v, want %v", got, want[:1])
	}
}

func TestTopSendersOnEmptyDatabaseIsEmptyNotNil(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	got := mustTopSenders(t, s, 10)
	if got == nil || len(got) != 0 {
		t.Errorf("TopSenders on empty DB = %#v, want empty non-nil slice", got)
	}
}

// Scenario: Invalid limit (also covered without a database in
// TestTopSendersInvalidLimit).
func TestTopSendersInvalidLimitAgainstDatabase(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	if _, err := s.TopSenders(t.Context(), 0); !errors.Is(err, ErrInvalidLimit) {
		t.Errorf("TopSenders(0) error = %v, want ErrInvalidLimit", err)
	}
}

// The partial index must be usable by the top-senders query: its predicate
// matches the query's WHERE clause. Sequential scans are disabled so the
// planner's choice does not depend on the (tiny) table size.
func TestTopSendersQueryCanUseFromAddrIndex(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	mustInsert(t, s, threeMessages)

	conn, err := s.pool.Acquire(t.Context())
	if err != nil {
		t.Fatalf("acquiring connection: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(t.Context(), "ANALYZE messages"); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}
	if _, err := conn.Exec(t.Context(), "SET enable_seqscan = off"); err != nil {
		t.Fatalf("SET enable_seqscan: %v", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), "RESET enable_seqscan") }()

	rows, err := conn.Query(t.Context(), "EXPLAIN "+topSendersSQL, 10)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("reading plan: %v", err)
	}
	plan := fmt.Sprint(lines)
	if !bytes.Contains([]byte(plan), []byte("messages_from_addr_idx")) {
		t.Errorf("top-senders plan does not use messages_from_addr_idx:\n%s", plan)
	}
}

// Scenario: Readiness reflects the database (store half; the /readyz HTTP
// half is in cmd/enron-service TestBinaryReadinessReflectsDatabase). It
// stops a dedicated container, so it does not disturb the shared one.
func TestReadinessReflectsDatabase(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	pg, err := storetest.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pg.Terminate(); err != nil {
			t.Errorf("terminating container: %v", err)
		}
	})
	s := openStoreAt(t, pg.URL)

	if got := s.Name(); got != "postgres" {
		t.Errorf("Name() = %q, want %q", got, "postgres")
	}
	if err := s.Check(ctx); err != nil {
		t.Fatalf("Check with database up: %v", err)
	}

	if err := pg.Stop(ctx); err != nil {
		t.Fatalf("stopping container: %v", err)
	}
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second) // the /readyz per-check timeout (#3)
	defer cancel()
	if err := s.Check(checkCtx); err == nil {
		t.Fatal("Check with database stopped returned nil, want an error")
	}
}

// BenchmarkInsertMessages inserts 100k synthetic rows in batches of 2000
// and reports rows/s. Not run in CI:
//
//	go test -tags integration -run '^$' -bench InsertMessages -benchtime 1x ./internal/store/
func BenchmarkInsertMessages(b *testing.B) {
	const (
		rowsPerOp = 100_000
		batchSize = 2_000
	)
	s := openStore(b)
	sent := time.Date(2001, 5, 14, 23, 39, 0, 0, time.UTC)
	hash := bytes.Repeat([]byte{0x11}, 32)
	senders := []string{"phillip.allen@enron.com", "kenneth.lay@enron.com", "jeff.skilling@enron.com"}
	batch := make([]Message, batchSize)

	op := 0
	for b.Loop() {
		for start := 0; start < rowsPerOp; start += batchSize {
			for i := range batch {
				n := start + i
				batch[i] = Message{
					SourcePath: fmt.Sprintf("bench-%d/inbox/%d.", op, n), Mailbox: fmt.Sprintf("bench-%d", op), Folder: "inbox",
					MessageID: ptr(fmt.Sprintf("<%d.%d.JavaMail.evans@thyme>", op, n)),
					DateRaw:   ptr("Mon, 14 May 2001 16:39:00 -0700 (PDT)"), SentAt: &sent,
					FromAddr: &senders[n%len(senders)], FromRaw: &senders[n%len(senders)], XFrom: ptr("Phillip K Allen"),
					Subject: ptr("Re: forecast"), To: []string{"tim.belden@enron.com", "john.arnold@enron.com"},
					ContentSHA256: hash,
				}
			}
			if _, err := s.InsertMessages(context.Background(), batch); err != nil {
				b.Fatalf("InsertMessages: %v", err)
			}
		}
		op++
	}
	b.ReportMetric(float64(op*rowsPerOp)/b.Elapsed().Seconds(), "rows/s")
}
