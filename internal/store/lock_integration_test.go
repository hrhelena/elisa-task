//go:build integration

package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for the lock handle's behavior under cancellation and connection
// loss (review round 2 of #29).

// requireDone fails unless lock.Done is closed within d and Err wraps
// ErrLockNotHeld.
func requireDone(t *testing.T, lock IngestLock, d time.Duration) {
	t.Helper()
	select {
	case <-lock.Done():
	case <-time.After(d):
		t.Fatalf("lock.Done not closed within %v", d)
	}
	if err := lock.Err(); !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("lock.Err() = %v, want ErrLockNotHeld", err)
	}
}

// requireHeld fails unless lock is still held by its own session: Done open,
// Err nil, and another replica cannot take the lock.
func requireHeld(t *testing.T, lock IngestLock, other *Postgres) {
	t.Helper()
	select {
	case <-lock.Done():
		t.Fatalf("lock.Done closed: %v", lock.Err())
	default:
	}
	if err := lock.Err(); err != nil {
		t.Errorf("lock.Err() = %v, want nil", err)
	}
	if _, err := other.AcquireIngestLock(t.Context()); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("another replica's AcquireIngestLock error = %v, want ErrLockHeld", err)
	}
	if !mustLockHeld(t, other) {
		t.Error("IngestLockHeld = false, want true")
	}
}

// lockRow holds a row lock on ingestion_runs row id in a transaction of s
// until the returned function is called, so writes to that row block.
func lockRow(t *testing.T, s *Postgres, id int64) (unlock func()) {
	t.Helper()
	tx, err := s.pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(t.Context(), "SELECT 1 FROM ingestion_runs WHERE id = $1 FOR UPDATE", id); err != nil {
		t.Fatalf("locking run %d: %v", id, err)
	}
	var once sync.Once
	unlock = func() { once.Do(func() { _ = tx.Rollback(context.Background()) }) }
	t.Cleanup(unlock)
	return unlock
}

// A run write whose context times out while its query runs (slow database,
// per-write timeout, SIGTERM) cancels the query on the server and keeps the
// lock, so the run can still be finished (ADR 0010 shutdown path).
func TestTimedOutRunWriteKeepsTheLock(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	lock := mustAcquire(t, a)
	run := mustBeginRun(t, a, lock)

	unlockRow := lockRow(t, b, run.ID) // the progress write blocks on the row
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := a.UpdateRunProgress(ctx, lock, run.ID, Counters{FilesSeen: 5})
	if err == nil || errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("timed-out UpdateRunProgress error = %v, want a cancellation error, not ErrLockNotHeld", err)
	}
	if elapsed := time.Since(start); elapsed >= cancelDeadlineDelay {
		t.Errorf("canceled write took %v: stopped by the socket deadline, not a server-side cancel", elapsed)
	}
	requireHeld(t, lock, b)

	unlockRow()
	msg := "interrupted: shutdown"
	final := Counters{FilesSeen: 7, MessagesProcessed: 7}
	if err := a.FinishRun(t.Context(), lock, run.ID, RunFailed, final, &msg); err != nil {
		t.Fatalf("FinishRun after a timed-out write: %v", err)
	}
	got := storedRun(t, b, run.ID)
	if got.State != RunFailed || got.Error == nil || *got.Error != msg || got.FilesSeen != 7 {
		t.Errorf("stored run = state %q error %v files_seen %d, want failed %q with the final counters",
			got.State, got.Error, got.FilesSeen, msg)
	}
}

// A BeginRun canceled inside its transaction rolls back and keeps the lock.
func TestCanceledBeginRunKeepsTheLock(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)

	// A stale running row (its owner died) whose row lock makes BeginRun's
	// "mark interrupted" update wait.
	var staleID int64
	if err := b.pool.QueryRow(t.Context(),
		"INSERT INTO ingestion_runs (state) VALUES ('running') RETURNING id").Scan(&staleID); err != nil {
		t.Fatalf("inserting a stale run: %v", err)
	}
	unlockRow := lockRow(t, b, staleID)

	lock := mustAcquire(t, a)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, err := a.BeginRun(ctx, lock); err == nil || errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("timed-out BeginRun error = %v, want a cancellation error, not ErrLockNotHeld", err)
	}
	requireHeld(t, lock, b)
	if got := storedRun(t, b, staleID); got.State != RunRunning {
		t.Errorf("stale run after the rolled-back BeginRun = %q, want still running", got.State)
	}

	unlockRow()
	run := mustBeginRun(t, a, lock)
	if got := storedRun(t, b, staleID); got.State != RunFailed || run.ID <= staleID {
		t.Errorf("after BeginRun: stale run %q, new run %d; want the stale run failed and a newer run", got.State, run.ID)
	}
}

// An already canceled context is returned without touching the lock's
// connection (pgx closes a connection whose BEGIN fails that way).
func TestAlreadyCanceledRunWritesKeepTheLock(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	lock := mustAcquire(t, a)
	run := mustBeginRun(t, a, lock)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.BeginRun(ctx, lock); !errors.Is(err, context.Canceled) {
		t.Errorf("BeginRun error = %v, want context.Canceled", err)
	}
	if err := a.UpdateRunProgress(ctx, lock, run.ID, Counters{}); !errors.Is(err, context.Canceled) {
		t.Errorf("UpdateRunProgress error = %v, want context.Canceled", err)
	}
	if err := a.FinishRun(ctx, lock, run.ID, RunCompleted, Counters{}, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("FinishRun error = %v, want context.Canceled", err)
	}
	requireHeld(t, lock, b)
	if err := a.FinishRun(t.Context(), lock, run.ID, RunCompleted, Counters{}, nil); err != nil {
		t.Errorf("FinishRun afterwards: %v", err)
	}
}

// Done is closed promptly when the lock's connection dies, without waiting
// for a run write.
func TestDoneIsClosedWhenTheLockConnectionDies(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	a.lockCheckInterval = 50 * time.Millisecond
	lock := mustAcquire(t, a)

	time.Sleep(200 * time.Millisecond) // several checks pass while it is held
	requireHeld(t, lock, b)

	killLockHolder(t, b)
	requireDone(t, lock, 5*time.Second)
	if err := lock.Err(); err == nil || !strings.Contains(err.Error(), "connection lost") {
		t.Errorf("lock.Err() = %v, want a lost connection", err)
	}
	if err := lock.Release(t.Context()); err != nil {
		t.Errorf("Release of a lost lock: %v", err)
	}
}

func TestDoneIsClosedOnRelease(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	lock := mustAcquire(t, s)
	if err := lock.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	requireDone(t, lock, time.Second)
	if err := lock.Err(); !errors.Is(err, ErrLockReleased) {
		t.Errorf("lock.Err() after Release = %v, want ErrLockReleased", err)
	}
}

// syncBuffer is a goroutine-safe log sink.
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

// openLoggedStore opens a store on dbURL that logs JSON at DEBUG into the
// returned buffer.
func openLoggedStore(t *testing.T, dbURL string) (*Postgres, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	s, err := Open(t.Context(), dbURL, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(s.Close)
	return s, logs
}

// Review round 3: a normal shutdown (Close, Release) while the liveness check
// runs is not reported as a lost lock; a real loss is, at ERROR.
func TestLockEndReasonsAndLogLevels(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	s, logs := openLoggedStore(t, dbURL)
	observer := openStoreAt(t, dbURL)
	s.lockCheckInterval = time.Millisecond // the check is almost always running

	released := mustAcquire(t, s)
	time.Sleep(50 * time.Millisecond)
	if err := released.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	lost := mustAcquire(t, s)
	killLockHolder(t, observer)
	requireDone(t, lost, 5*time.Second)
	if err := lost.Err(); errors.Is(err, ErrLockReleased) || errors.Is(err, ErrStoreClosed) {
		t.Errorf("lost lock Err = %v, want neither ErrLockReleased nor ErrStoreClosed", err)
	}
	waitFor(t, "the loss to be logged", func() bool { return strings.Contains(logs.String(), `"ingest lock lost"`) })
	lossLogs := strings.Count(logs.String(), `"msg":"ingest lock lost"`)

	closed := mustAcquire(t, s)
	time.Sleep(50 * time.Millisecond)
	closeWithin(t, s, 10*time.Second)
	requireDone(t, closed, time.Second)
	if err := closed.Err(); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("lock.Err() after Close = %v, want ErrStoreClosed", err)
	}
	time.Sleep(50 * time.Millisecond)

	out := logs.String()
	if got := strings.Count(out, `"msg":"ingest lock lost"`); got != lossLogs || lossLogs != 1 {
		t.Errorf("%q logged %d times in total (%d for the real loss), want exactly once for the real loss:\n%s",
			"ingest lock lost", got, lossLogs, out)
	}
	if !strings.Contains(out, `"level":"ERROR","msg":"ingest lock lost"`) {
		t.Errorf("the real loss was not logged at ERROR:\n%s", out)
	}
}

// A store built without Open (interval 0) must not panic in the watcher.
func TestZeroLockCheckIntervalUsesTheDefault(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	s, other := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	s.lockCheckInterval = 0
	lock := mustAcquire(t, s)
	requireHeld(t, lock, other)
	if err := lock.Release(t.Context()); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// Review round 3: a run write waits for the lock connection (busy with a
// liveness check or another write) no longer than its own context, and
// Release does not wait beyond its context either.
func TestWaitingForTheLockConnectionIsBoundedByTheContext(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	lock := mustAcquire(t, a)
	run := mustBeginRun(t, a, lock)
	l := lock.(*pgIngestLock)

	l.sem <- struct{}{} // the connection is busy (as during a slow check)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := a.UpdateRunProgress(ctx, lock, run.ID, Counters{FilesSeen: 1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("UpdateRunProgress behind a busy connection error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("UpdateRunProgress waited %v, want about its 100ms budget", elapsed)
	}
	<-l.sem
	requireHeld(t, lock, b)

	l.sem <- struct{}{} // busy again: Release must not wait past its context
	relCtx, relCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer relCancel()
	start = time.Now()
	if err := lock.Release(relCtx); err != nil {
		t.Errorf("Release behind a busy connection: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Release waited %v", elapsed)
	}
	<-l.sem
	waitFor(t, "the lock to be released", func() bool { return !mustLockHeld(t, b) })
	mustAcquire(t, b)
}

// freezeProxy forwards TCP connections to a Postgres server until frozen;
// then it stops forwarding in both directions without closing anything, like
// a server that hangs or a network path that blackholes packets.
type freezeProxy struct {
	ln        net.Listener
	upstream  string
	frozen    atomic.Bool
	stop      chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	conns     []net.Conn
}

func startFreezeProxy(t *testing.T, upstream string) *freezeProxy {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &freezeProxy{ln: ln, upstream: upstream, stop: make(chan struct{})}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *freezeProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		var d net.Dialer
		server, err := d.DialContext(context.Background(), "tcp", p.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, client, server)
		p.mu.Unlock()
		go p.pipe(server, client)
		go p.pipe(client, server)
	}
}

func (p *freezeProxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(gatedWriter{dst, p}, src)
	_ = dst.Close()
}

type gatedWriter struct {
	w io.Writer
	p *freezeProxy
}

func (g gatedWriter) Write(b []byte) (int, error) {
	if g.p.frozen.Load() {
		<-g.p.stop
		return 0, io.ErrClosedPipe
	}
	return g.w.Write(b)
}

func (p *freezeProxy) freeze() { p.frozen.Store(true) }

// close stops the proxy and closes every connection; it is idempotent.
func (p *freezeProxy) close() {
	p.closeOnce.Do(func() {
		close(p.stop)
		_ = p.ln.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.conns {
			_ = c.Close()
		}
	})
}

// openStoreViaProxy opens a store through p. Its cleanup closes the proxy
// before the store, so connections stuck behind a frozen proxy fail at once
// instead of holding up Close.
func openStoreViaProxy(t *testing.T, p *freezeProxy, proxied string) *Postgres {
	t.Helper()
	s := openStoreAt(t, proxied)
	t.Cleanup(p.close)
	return s
}

// viaProxy returns dbURL with its host replaced by a new freeze proxy.
func viaProxy(t *testing.T, dbURL string) (*freezeProxy, string) {
	t.Helper()
	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	p := startFreezeProxy(t, u.Host)
	u.Host = p.ln.Addr().String()
	return p, u.String()
}

// Review round 3: pool queries keep pgx's default cancellation, so on a
// server that stops answering they end at their own deadline (the
// server-side cancel used by the lock connection would add up to
// cancelDeadlineDelay).
func TestPoolQueriesRespectTheirTimeoutOnAFrozenServer(t *testing.T) {
	t.Parallel()
	proxy, proxied := viaProxy(t, newDatabase(t))
	s := openStoreViaProxy(t, proxy, proxied)
	if err := s.Check(t.Context()); err != nil {
		t.Fatalf("Check before freezing: %v", err)
	}

	proxy.freeze()
	const budget = 500 * time.Millisecond
	for name, query := range map[string]func(context.Context) error{
		"Check": s.Check,
		"TopSenders": func(ctx context.Context) error {
			_, err := s.TopSenders(ctx, 10)
			return err
		},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), budget)
		start := time.Now()
		err := query(ctx)
		elapsed := time.Since(start)
		cancel()
		if err == nil {
			t.Errorf("%s on a frozen server succeeded", name)
		}
		if elapsed > budget+time.Second {
			t.Errorf("%s took %v with a %v budget, want it to end at its deadline", name, elapsed, budget)
		}
	}
}

// On a server that stops answering, a run write on the lock connection ends
// the lock (cancel request unanswered, then the socket deadline) instead of
// hanging, and Done is closed.
func TestLockEndsWhenTheServerStopsAnswering(t *testing.T) {
	t.Parallel()
	proxy, proxied := viaProxy(t, newDatabase(t))
	s := openStoreViaProxy(t, proxy, proxied)
	lock := mustAcquire(t, s)
	run := mustBeginRun(t, s, lock)

	proxy.freeze()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := s.UpdateRunProgress(ctx, lock, run.ID, Counters{FilesSeen: 1})
	if !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("UpdateRunProgress on a frozen server error = %v, want ErrLockNotHeld", err)
	}
	if elapsed := time.Since(start); elapsed > cancelDeadlineDelay+3*time.Second {
		t.Errorf("UpdateRunProgress took %v, want at most about cancelDeadlineDelay", elapsed)
	}
	requireDone(t, lock, time.Second)
}
