// Command enron-service ingests the Enron maildir dataset into Postgres and
// serves email metrics over HTTP.
//
// This is the composition root: it is the only place that knows concrete
// types (ADR 0009). It loads the configuration, wires the HTTP server and
// owns the process lifecycle, including graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/hrhelena/elisa-task/internal/config"
	"github.com/hrhelena/elisa-task/internal/fetch"
	"github.com/hrhelena/elisa-task/internal/httpapi"
	"github.com/hrhelena/elisa-task/internal/store"
)

// storeOpenTimeout bounds connecting to Postgres and applying migrations at
// startup. Past it the process exits 1 and the orchestrator restarts it,
// which also covers a database that is not up yet.
const storeOpenTimeout = time.Minute

// HTTP server timeouts (ticket #3).
const (
	// closeBudget is the fresh time budget for releasing resources after the
	// drain: waiting for force-closed handlers to return, then the closers.
	// It is separate from SHUTDOWN_TIMEOUT so that a drain timeout never
	// leaves the closers an expired context.
	closeBudget = 5 * time.Second

	readHeaderTimeout = 5 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 120 * time.Second
)

func main() {
	// The first SIGINT/SIGTERM starts the graceful shutdown; default handling
	// is restored before ctx is canceled, so a second one kills the process
	// at once instead of waiting for the graceful shutdown.
	ctx, stop := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.LookupEnv, os.Stdout, openPostgres, nil)
	stop()
	os.Exit(code)
}

// dbStore is what run needs from the store so far: a readiness check, the
// top-senders ranking (#9) and Close. Later tickets widen it as handlers start
// using the store.
type dbStore interface {
	httpapi.Checker
	senderRanker
	Close()
}

// openStoreFunc opens the store, applying migrations. Tests substitute it.
type openStoreFunc func(ctx context.Context, databaseURL string, logger *slog.Logger) (dbStore, error)

// openPostgres opens the Postgres store (ADR 0001, 0013).
func openPostgres(ctx context.Context, databaseURL string, logger *slog.Logger) (dbStore, error) {
	s, err := store.Open(ctx, databaseURL, logger)
	if err != nil {
		return nil, err // not a typed nil inside a non-nil interface
	}
	return s, nil
}

// closeStore closes st, giving up when ctx ends. Pool close waits for
// acquired connections; by the time closers run no handler holds one, but a
// stuck connection must not hold the exit beyond the close budget.
func closeStore(ctx context.Context, st dbStore) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		st.Close()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// The goroutine ends with the process, which exits right after.
		return fmt.Errorf("closing database pool: %w", ctx.Err())
	}
}

// newLogger returns the service's structured JSON logger writing to w.
func newLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})).
		With(slog.String("service", "enron-service"))
}

// newRegistry returns the service's dedicated Prometheus registry (never the
// global default one) with the Go runtime and process collectors.
func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// newServer returns the HTTP server with the service's timeouts.
func newServer(handler http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
}

// run is the testable body of main and returns the process exit code. It
// serves until ctx is canceled (by SIGINT or SIGTERM in main), then shuts down
// gracefully. openStore opens the database (connect and migrate) before the
// server listens; failure exits 1. onListen, if not nil, is called with the
// bound address once the listener is open.
func run(ctx context.Context, lookup config.LookupFunc, out io.Writer, openStore openStoreFunc, onListen func(net.Addr)) int {
	cfg, err := config.LoadFrom(lookup)
	if err != nil {
		newLogger(out, slog.LevelInfo).ErrorContext(ctx, "invalid configuration", slog.Any("err", err))
		return 1
	}
	logger := newLogger(out, cfg.LogLevel)
	logger.InfoContext(ctx, "starting")

	openCtx, cancelOpen := context.WithTimeout(ctx, storeOpenTimeout)
	st, err := openStore(openCtx, cfg.DatabaseURL, logger)
	cancelOpen()
	if err != nil {
		logger.ErrorContext(ctx, "opening database failed", slog.Any("err", err))
		return 1
	}

	// Closers run in reverse registration order after the server has drained:
	// the DB pool first registered, so closed last; then the fetcher (#10),
	// closed first; #7 adds the ingestion cancel.
	var cl closers
	cl.add("postgres", func(ctx context.Context) error { return closeStore(ctx, st) })

	reg := newRegistry()
	// The fetcher runs downloads under its own context; its closer cancels an
	// in-flight fetch and waits for the temporary directory to be removed.
	// New also removes temporary directories left by an interrupted fetch.
	fetcher := fetch.New(cfg.DataDir, cfg.DatasetURL, nil, logger, reg)
	cl.add("fetch", fetcher.Close)

	readiness := httpapi.NewReadiness(st)
	handler := httpapi.New(httpapi.Deps{
		Registry: reg, Readiness: readiness, Fetcher: fetcher, Senders: senderStats{st},
	}, logger)
	srv := newServer(handler, logger)

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		logger.ErrorContext(ctx, "listen failed", slog.String("addr", cfg.HTTPAddr), slog.Any("err", err))
		st.Close()
		return 1
	}
	logger.InfoContext(ctx, "listening",
		slog.String("addr", cfg.HTTPAddr), slog.String("listen_addr", ln.Addr().String()))
	if onListen != nil {
		onListen(ln.Addr())
	}

	opts := shutdownOpts{
		timeout:     cfg.ShutdownTimeout,
		drainDelay:  cfg.ShutdownDrainDelay,
		closeBudget: closeBudget,
	}
	if err := serve(ctx, srv, ln, readiness, cl, opts, logger); err != nil {
		logger.Error("shutdown failed", slog.Any("err", err))
		return 1
	}
	logger.Info("stopped")
	return 0
}

// shutdownOpts tune graceful shutdown.
type shutdownOpts struct {
	// timeout bounds draining in-flight requests.
	timeout time.Duration
	// drainDelay is how long to keep serving, reporting not ready, before
	// the listener closes.
	drainDelay time.Duration
	// closeBudget bounds what follows the drain, with a fresh deadline:
	// waiting for force-closed handlers to return, then running the closers.
	closeBudget time.Duration
}

// serve runs srv on ln until ctx is done or the server fails. On a signal it
// marks the service not ready and keeps serving for opts.drainDelay (so
// /readyz answers 503 and load balancers stop routing to it), then stops
// accepting connections and drains in-flight requests within opts.timeout.
// If the drain times out, it closes the remaining connections and waits for
// their handlers to return. Closers run only after that, under a fresh
// opts.closeBudget deadline, so they never release a resource (such as the DB
// pool) that a handler is still using, unless a handler ignores its
// canceled context for the whole budget; that is logged and reported. It
// returns nil only for a clean, timely shutdown.
func serve(
	ctx context.Context,
	srv *http.Server,
	ln net.Listener,
	readiness *httpapi.Readiness,
	cl closers,
	opts shutdownOpts,
	logger *slog.Logger,
) error {
	var handlers inFlight
	srv.Handler = handlers.track(srv.Handler)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	var errs []error
	served := false // whether serveErr has been received
	select {
	case err := <-serveErr:
		// The server stopped on its own (e.g. the listener failed); still
		// release resources below.
		served = true
		errs = append(errs, fmt.Errorf("serving: %w", err))
	case <-ctx.Done():
		logger.Info("shutting down (send the signal again to exit immediately)",
			slog.Duration("drain_delay", opts.drainDelay), slog.Duration("timeout", opts.timeout))
	}

	readiness.SetShuttingDown()
	if !served && opts.drainDelay > 0 {
		drain := time.NewTimer(opts.drainDelay)
		select {
		case <-drain.C:
		case err := <-serveErr:
			served = true
			errs = append(errs, fmt.Errorf("serving: %w", err))
		}
		drain.Stop()
	}
	// The parent context is already canceled; the drain gets its own deadline.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opts.timeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("draining HTTP server: %w", err))
		// Shutdown gave up on active connections; cut them. This cancels
		// their request contexts but does not wait for their handlers.
		_ = srv.Close()
	}
	if !served {
		// Serve returns ErrServerClosed as soon as Shutdown closes the listener.
		if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, fmt.Errorf("serving: %w", err))
		}
	}

	closeCtx, cancelClose := context.WithTimeout(context.WithoutCancel(ctx), opts.closeBudget)
	defer cancelClose()
	if err := handlers.wait(closeCtx); err != nil {
		logger.Error("handlers still running; closing resources anyway",
			slog.Int64("handlers", handlers.active.Load()), slog.Any("err", err))
		errs = append(errs, fmt.Errorf("waiting for handlers: %w", err))
	}
	if err := cl.closeAll(closeCtx, logger); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// inFlight counts running handlers. http.Server.Close does not wait for
// handlers, so serve uses it to know when resources are no longer in use.
type inFlight struct {
	active atomic.Int64
}

func (f *inFlight) track(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.active.Add(1)
		defer f.active.Add(-1)
		h.ServeHTTP(w, r)
	})
}

// wait polls until no handler runs or ctx ends. Polling keeps it simple; it
// only runs once, during shutdown.
func (f *inFlight) wait(ctx context.Context) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for f.active.Load() > 0 {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%d handler(s) still running: %w", f.active.Load(), ctx.Err())
		case <-tick.C:
		}
	}
	return nil
}

// closer releases one resource on shutdown.
type closer struct {
	name  string
	close func(context.Context) error
}

// closers are run in reverse order of registration, like defers, once no
// HTTP handler runs any more. They share one fresh context bounded by
// closeBudget, even when the drain timed out.
type closers []closer

// add registers a closer; used by the DB (#4) and ingestion (#7) wiring.
func (cs *closers) add(name string, fn func(context.Context) error) {
	*cs = append(*cs, closer{name: name, close: fn})
}

// closeAll runs every closer, last registered first, even if earlier ones
// fail, and joins their errors.
func (cs closers) closeAll(ctx context.Context, logger *slog.Logger) error {
	var errs []error
	for i := len(cs) - 1; i >= 0; i-- {
		c := cs[i]
		if err := c.close(ctx); err != nil {
			logger.ErrorContext(ctx, "closing failed", slog.String("closer", c.name), slog.Any("err", err))
			errs = append(errs, fmt.Errorf("closing %s: %w", c.name, err))
		}
	}
	return errors.Join(errs...)
}
