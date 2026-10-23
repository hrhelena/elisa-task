//go:build integration

package store

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// cancelRequestCode is the protocol code of a CancelRequest packet.
const cancelRequestCode = 80877102

// cancelGate holds back the cancel requests sent by a store's lock
// connections until the test delivers them: a cancel request that the
// server processes late, as under CPU starvation or a slow network path.
type cancelGate struct {
	sent      chan struct{} // closed when the first cancel request is written by the client
	deliver   chan struct{} // closed by the test (deliverNow) to let it reach the server
	processed chan struct{} // closed when the server has processed it (closed the cancel connection)

	sentOnce, deliverOnce sync.Once
	wg                    sync.WaitGroup
}

// holdCancelRequests makes s dial its lock connections through a cancelGate.
// It must be called before AcquireIngestLock.
func holdCancelRequests(t *testing.T, s *Postgres) *cancelGate {
	t.Helper()
	g := &cancelGate{sent: make(chan struct{}), deliver: make(chan struct{}), processed: make(chan struct{})}
	dial := s.lockConnConfig.DialFunc
	s.lockConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &gatedConn{Conn: c, g: g}, nil
	}
	t.Cleanup(func() { // no leaked goroutine if the test failed early
		g.deliverNow()
		g.wg.Wait()
	})
	return g
}

func (g *cancelGate) deliverNow() { g.deliverOnce.Do(func() { close(g.deliver) }) }

// gatedConn passes everything through, except a cancel request: Write
// reports it written at once, but it reaches the server only when the gate
// delivers it. Until then the client's wait for the server's answer blocks
// (or is cut short by a deadline), and closing the connection is deferred,
// since the packet is already "on the wire".
type gatedConn struct {
	net.Conn
	g *cancelGate

	mu      sync.Mutex
	held    bool
	closed  bool
	release sync.Once
}

func isCancelRequest(b []byte) bool {
	return len(b) >= 12 && binary.BigEndian.Uint32(b[4:8]) == cancelRequestCode
}

func (c *gatedConn) Write(b []byte) (int, error) {
	if !isCancelRequest(b) {
		return c.Conn.Write(b)
	}
	pkt := append([]byte(nil), b...)
	c.mu.Lock()
	c.held = true
	c.mu.Unlock()
	c.g.sentOnce.Do(func() { close(c.g.sent) })
	c.g.wg.Go(func() {
		defer c.release.Do(func() { _ = c.Conn.Close() })
		<-c.g.deliver
		// The client may have given up waiting (a deadline in the past);
		// the server still gets the packet and answers by closing.
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := c.Conn.Write(pkt); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, c.Conn) // until the server closes the connection
		close(c.g.processed)
	})
	return len(b), nil
}

func (c *gatedConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.held {
		return nil // closed by the delivery goroutine
	}
	var err error
	c.release.Do(func() { err = c.Conn.Close() })
	return err
}

// waitsOnALock reports whether backend pid waits for a lock (here: a row
// lock held by another session).
func waitsOnALock(t *testing.T, via *Postgres, pid uint32) bool {
	t.Helper()
	var waiting bool
	if err := via.pool.QueryRow(t.Context(), "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)",
		int64(pid)).Scan(&waiting); err != nil {
		t.Fatalf("reading pg_locks: %v", err)
	}
	return waiting
}

// #51. A progress write in flight on the lock connection is canceled (as on
// shutdown) but completes on the server before the cancel request arrives.
// The cancel request is processed late, while the final write
// "interrupted: shutdown" runs on the same connection. That final write must
// not be the one canceled: a canceled write on the lock connection returns
// only once the server has processed its cancel request.
func TestShutdownWithAProgressWriteInFlightSurvivesALateCancel(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)
	a, b := openStoreAt(t, dbURL), openStoreAt(t, dbURL)
	gate := holdCancelRequests(t, a)
	lock := mustAcquire(t, a)
	run := mustBeginRun(t, a, lock)
	pid := lock.(*pgIngestLock).conn.PgConn().PID()

	// A progress write is in flight (blocked on a row lock) when the run
	// context is canceled.
	unlockRow := lockRow(t, b, run.ID)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	updated := make(chan error, 1)
	go func() {
		updated <- a.UpdateRunProgress(ctx, lock, run.ID, Counters{FilesSeen: 5, MessagesProcessed: 5})
	}()
	waitFor(t, "the progress write to wait on the row lock", func() bool { return waitsOnALock(t, b, pid) })
	cancel()
	select {
	case <-gate.sent:
	case <-time.After(5 * time.Second):
		t.Fatal("no cancel request was sent for the canceled progress write")
	}
	// The write completes on the server before its cancel request arrives.
	unlockRow()
	waitFor(t, "the progress write to complete on the server", func() bool {
		return storedRun(t, b, run.ID).FilesSeen == 5
	})

	// The final write runs, and waits on a row lock again, so it is in flight
	// when the late cancel request is processed. A canceled write that does
	// not wait for its cancel request returns about 100ms after its query
	// (pgx's CancelRequestContextWatcherHandler); give it time to.
	var returnedEarly bool
	select {
	case err := <-updated:
		returnedEarly = true
		updated <- err
	case <-time.After(time.Second):
	}
	unlockRow = lockRow(t, b, run.ID)
	msg := "interrupted: shutdown"
	final := Counters{FilesSeen: 7, MessagesProcessed: 7}
	finished := make(chan error, 1)
	go func() { finished <- a.FinishRun(t.Context(), lock, run.ID, RunFailed, final, &msg) }()
	if returnedEarly {
		waitFor(t, "the final write to wait on the row lock", func() bool { return waitsOnALock(t, b, pid) })
	}

	gate.deliverNow()
	select {
	case <-gate.processed:
	case <-time.After(10 * time.Second):
		t.Fatal("the server did not process the cancel request")
	}
	if err := <-updated; errors.Is(err, ErrLockNotHeld) {
		t.Errorf("canceled UpdateRunProgress error = %v, want the lock kept", err)
	}
	// A final write hit by the cancel fails at once; otherwise it is (or
	// soon will be) waiting on the row lock.
	select {
	case err := <-finished:
		finished <- err
	case <-time.After(time.Second):
	}
	waitFor(t, "the final write to finish or wait on the row lock", func() bool {
		return len(finished) > 0 || waitsOnALock(t, b, pid)
	})
	unlockRow()
	if err := <-finished; err != nil {
		t.Fatalf("final write after a late cancel request: %v", err)
	}
	got := storedRun(t, b, run.ID)
	if got.State != RunFailed || got.Error == nil || *got.Error != msg || got.FilesSeen != 7 {
		t.Errorf("stored run = state %q error %v files_seen %d, want failed %q with the final counters",
			got.State, got.Error, got.FilesSeen, msg)
	}
	requireHeld(t, lock, b)
}

// #51. A canceled write waits for its cancel request to be acknowledged, but
// not after the store closes the lock's connection: on a server that stops
// answering, Close ends that wait at once instead of after
// cancelDeadlineDelay.
func TestCloseEndsTheWaitForAnUnansweredCancelRequest(t *testing.T) {
	t.Parallel()
	proxy, proxied := viaProxy(t, newDatabase(t))
	s := openStoreViaProxy(t, proxy, proxied)
	lock := mustAcquire(t, s)
	run := mustBeginRun(t, s, lock)

	proxy.freeze()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	updated := make(chan error, 1)
	go func() { updated <- s.UpdateRunProgress(ctx, lock, run.ID, Counters{FilesSeen: 1}) }()
	<-ctx.Done()
	time.Sleep(200 * time.Millisecond) // the cancel request is sent and goes unanswered

	start := time.Now()
	closeWithin(t, s, 2*time.Second)
	select {
	case err := <-updated:
		if !errors.Is(err, ErrLockNotHeld) {
			t.Errorf("UpdateRunProgress after Close error = %v, want ErrLockNotHeld", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("UpdateRunProgress still waiting %v after Close", time.Since(start))
	}
	if elapsed := time.Since(start); elapsed >= cancelDeadlineDelay {
		t.Errorf("the write ended %v after Close: by the deadline, not by Close", elapsed)
	}
	requireDone(t, lock, time.Second)
}
