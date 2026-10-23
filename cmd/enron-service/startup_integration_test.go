//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hrhelena/elisa-task/internal/store/storetest"
)

// lateProxy is a TCP endpoint for a database that does not exist yet: until
// forward is called it accepts connections and closes them at once (the
// server is not up), afterwards it relays them to the target.
type lateProxy struct {
	ln     net.Listener
	target atomic.Pointer[string]
	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  []net.Conn // relayed connections, closed by close
	closed bool
}

func startLateProxy(t *testing.T) *lateProxy {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &lateProxy{ln: ln}
	p.wg.Go(p.accept)
	t.Cleanup(p.close)
	return p
}

func (p *lateProxy) addr() string { return p.ln.Addr().String() }

func (p *lateProxy) forward(target string) { p.target.Store(&target) }

func (p *lateProxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return // listener closed
		}
		target := p.target.Load()
		if target == nil {
			_ = c.Close()
			continue
		}
		var d net.Dialer
		up, err := d.DialContext(context.Background(), "tcp", *target)
		if err != nil {
			_ = c.Close()
			continue
		}
		if !p.track(c, up) {
			return
		}
		p.wg.Go(func() { _, _ = io.Copy(up, c); _ = up.Close() })
		p.wg.Go(func() { _, _ = io.Copy(c, up); _ = c.Close() })
	}
}

// track records conns for close; it reports false (and closes them) once the
// proxy is closing.
func (p *lateProxy) track(conns ...net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		for _, c := range conns {
			_ = c.Close()
		}
		return false
	}
	p.conns = append(p.conns, conns...)
	return true
}

// close stops the proxy and waits for its goroutines (goleak).
func (p *lateProxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	p.closed = true
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
	p.mu.Unlock()
	p.wg.Wait()
}

// Scenario: Database comes up late (#27), end to end: the real binary
// starts first and retries; Postgres is then started in a new container,
// and the binary migrates and starts listening once it can reach it.
func TestBinaryDatabaseComesUpLate(t *testing.T) {
	t.Parallel()

	proxy := startLateProxy(t)
	b := startBinary(t,
		"HTTP_ADDR=127.0.0.1:0",
		// Generous, because a container start can be slow on CI runners;
		// the binary listens as soon as the database answers.
		"DB_STARTUP_TIMEOUT=5m",
		"DATABASE_URL=postgres://enron:enron@"+proxy.addr()+"/enron?sslmode=disable",
	)

	// Given the service is retrying...
	waitFor(t, "two retry warnings", func() bool {
		return strings.Count(b.logs.String(), `"msg":"database not ready, retrying"`) >= 2
	})
	// ...when Postgres becomes reachable.
	pg, err := storetest.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Terminate() })
	u, err := url.Parse(pg.URL)
	if err != nil {
		t.Fatalf("parsing container URL: %v", err)
	}
	proxy.forward(u.Host)

	// Then it starts listening and is ready.
	addr := b.waitListening(t)
	if code, body := get(t, newClient(), "http://"+addr+"/readyz"); code != http.StatusOK {
		t.Errorf("GET /readyz = %d %q, want 200", code, body)
	}
	b.signal(t)
	b.waitExit0(t)

	lines := decodeLogLines(t, []byte(b.logs.String()))
	attempts := retryWarnings(t, lines)
	for i, n := range attempts {
		if n != i+1 {
			t.Errorf("retry warnings for attempts %v, want 1, 2, 3, ...", attempts)
			break
		}
	}
	for _, msg := range []string{"database migrations applied", "listening", "stopped"} {
		if findLog(lines, msg) == nil {
			t.Errorf("no %q log line in:\n%s", msg, b.logs.String())
		}
	}
	if findLog(lines, "opening database failed") != nil {
		t.Errorf("startup logged a failure:\n%s", b.logs.String())
	}
}

// A wrong password in DATABASE_URL (a Secret typo) is a permanent error
// (PR #31 review): the real store is opened exactly once, startup exits 1
// at once with the authentication error, nothing is logged as a retry, and
// the password stays out of the logs.
func TestRunWrongPasswordFailsWithoutRetrying(t *testing.T) {
	t.Parallel()
	u, err := url.Parse(sharedDB.URL)
	if err != nil {
		t.Fatalf("parsing container URL: %v", err)
	}
	u.User = url.UserPassword("enron", "s3cret-typo")
	assertFailsWithoutRetrying(t, u.String(), "28P01", "s3cret-typo")
}

// Scenario: Missing CONNECT privilege fails fast (#37). A role without
// CONNECT on the database gets SQLSTATE 42501 from the server: startup exits
// 1 after a single attempt, logging the SQLSTATE.
func TestRunMissingConnectPrivilegeFailsFast(t *testing.T) {
	t.Parallel()
	dbURL, err := sharedDB.CreateDatabaseWithoutConnect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dbURL)
	if err != nil {
		t.Fatalf("parsing URL: %v", err)
	}
	password, _ := u.User.Password()
	assertFailsWithoutRetrying(t, dbURL, "42501", password)
}

// assertFailsWithoutRetrying runs the service against databaseURL and checks
// that the store is opened once, the exit status is 1 without listening, no
// retry is logged, "opening database failed" carries sqlstate, and secret
// does not appear in the logs.
func assertFailsWithoutRetrying(t *testing.T, databaseURL, sqlstate, secret string) {
	t.Helper()
	var attempts atomic.Int32
	counting := func(ctx context.Context, databaseURL string, logger *slog.Logger) (dbStore, error) {
		attempts.Add(1)
		return openPostgres(ctx, databaseURL, logger)
	}
	var out syncBuffer
	listened := false
	start := time.Now()
	code := run(t.Context(), envLookup(map[string]string{
		"HTTP_ADDR": "127.0.0.1:0", "DATABASE_URL": databaseURL, "DB_STARTUP_TIMEOUT": "60s",
	}), &out, counting, func(net.Addr) { listened = true })
	elapsed := time.Since(start)

	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if listened {
		t.Error("service listened with a rejected configuration")
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("store opened %d times, want exactly 1", n)
	}
	if elapsed > 10*time.Second {
		t.Errorf("startup took %v, want an immediate failure", elapsed)
	}
	lines := decodeLogLines(t, []byte(out.String()))
	if got := retryWarnings(t, lines); len(got) != 0 {
		t.Errorf("rejected configuration was retried (attempts %v):\n%s", got, out.String())
	}
	rec := findLog(lines, "opening database failed")
	if rec == nil || !strings.Contains(fmt.Sprint(rec["err"]), sqlstate) {
		t.Errorf(`want "opening database failed" with SQLSTATE %s, got:\n%s`, sqlstate, out.String())
	}
	if strings.Contains(out.String(), secret) {
		t.Errorf("logs leak the database password:\n%s", out.String())
	}
}
