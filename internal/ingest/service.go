package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hrhelena/elisa-task/internal/mailparse"
	"github.com/hrhelena/elisa-task/internal/store"
)

var (
	// ErrAlreadyRunning is returned by Start while a run is in progress on
	// any replica (ADR 0011: 409 ingestion_running).
	ErrAlreadyRunning = errors.New("ingestion already running")
	// ErrClosed is returned by Start after Close.
	ErrClosed = errors.New("ingestion service closed")
)

// Error messages stored on failed runs. The engine never fails a run because
// of the data (ADR 0007), only for these reasons or a database error.
const (
	// ErrorShutdown: the process shut down during the run (ADR 0010).
	ErrorShutdown = "interrupted: shutdown"
	// ErrorLockLost: the ingest lock was lost during the run, for example
	// because its database session was terminated.
	ErrorLockLost = "lock_lost"
)

// Status states (ADR 0011). "idle" means no run has ever started.
const (
	StateIdle      = "idle"
	StateRunning   = string(store.RunRunning)
	StateCompleted = string(store.RunCompleted)
	StateFailed    = string(store.RunFailed)
)

// Causes of canceling a run.
var (
	errShutdown      = errors.New(ErrorShutdown)
	errLockLost      = errors.New(ErrorLockLost)
	errLockReleased  = errors.New("interrupted: ingest lock released")
	errRunNotRunning = errors.New("run is no longer running")
)

// Store is what the engine needs from storage; *store.Postgres implements it.
type Store interface {
	InsertMessages(ctx context.Context, msgs []store.Message) (int64, error)
	CountMessages(ctx context.Context) (int64, error)
	AcquireIngestLock(ctx context.Context) (store.IngestLock, error)
	BeginRun(ctx context.Context, lock store.IngestLock) (store.Run, error)
	UpdateRunProgress(ctx context.Context, lock store.IngestLock, id int64, c store.Counters) error
	FinishRun(ctx context.Context, lock store.IngestLock, id int64, state store.RunState, c store.Counters,
		errMsg *string) error
	// LatestRun reports a running run whose owner died as failed with error
	// "interrupted" (ADR 0010).
	LatestRun(ctx context.Context) (*store.Run, error)
}

// Config tunes the engine. Zero fields take their defaults.
type Config struct {
	Workers          int           // parser goroutines; default 4
	BatchSize        int           // rows per insert; default 2000
	ProgressInterval time.Duration // progress writes; default 5s (ADR 0010)

	FlushInterval       time.Duration // a partial batch is written after this; default 1s
	ProgressLogInterval time.Duration // INFO progress log; default 30s
	MaxRetries          int           // retries of a failed batch insert; default 3
	RetryBackoff        time.Duration // first retry delay, doubled each time; default 500ms
	// InsertTimeout bounds one batch insert attempt, so an unresponsive
	// database fails the run after the retries instead of hanging; default 30s.
	InsertTimeout time.Duration
	// WriteTimeout bounds each run-state write, and the final write of a run
	// that ends on its own; default 10s.
	WriteTimeout time.Duration
	// ShutdownWriteTimeout bounds the final "interrupted: shutdown" write
	// and the lock release after Close. It must fit in the caller's close
	// budget (main gives all closers 5s); default 3s.
	ShutdownWriteTimeout time.Duration
}

func (c Config) withDefaults() Config {
	setDefault(&c.Workers, 4)
	setDefault(&c.BatchSize, 2000)
	setDefault(&c.ProgressInterval, 5*time.Second)
	setDefault(&c.FlushInterval, time.Second)
	setDefault(&c.ProgressLogInterval, 30*time.Second)
	setDefault(&c.MaxRetries, 3)
	setDefault(&c.RetryBackoff, 500*time.Millisecond)
	setDefault(&c.InsertTimeout, 30*time.Second)
	setDefault(&c.WriteTimeout, 10*time.Second)
	setDefault(&c.ShutdownWriteTimeout, 3*time.Second)
	return c
}

func setDefault[T int | time.Duration](v *T, def T) {
	if *v <= 0 {
		*v = def
	}
}

// Status is the ingestion state reported by GET /status (#8).
type Status struct {
	State         string     // StateIdle, StateRunning, StateCompleted or StateFailed
	Run           *store.Run // the current or last run; nil when idle
	TotalMessages int64      // rows stored
}

// Service runs ingestions. At most one runs at a time in the cluster,
// enforced by the store's ingest lock (ADR 0010). A run is owned by the
// Service, not by the request that started it.
type Service struct {
	src    Source
	st     Store
	cfg    Config
	logger *slog.Logger
	m      *metrics
	parse  parseFunc

	// life is canceled by Close; every run's context derives from it.
	life     context.Context
	stopLife context.CancelCauseFunc

	// mu guards closed and done. It is never held across a database call,
	// so a slow Start never delays Close.
	mu     sync.Mutex
	closed bool
	// done is non-nil from the moment Start claims the service until its
	// run (or its failed start) has ended; it is closed then.
	done chan struct{}
}

// NewService returns a Service. reg may be nil (metrics are not exported).
func NewService(src Source, st Store, cfg Config, logger *slog.Logger, reg prometheus.Registerer) *Service {
	life, stop := context.WithCancelCause(context.Background())
	return &Service{
		src:      src,
		st:       st,
		cfg:      cfg.withDefaults(),
		logger:   logger.With(slog.String("component", "ingest")),
		m:        newMetrics(reg, logger),
		parse:    mailparse.Parse,
		life:     life,
		stopLife: stop,
	}
}

// Start starts a run and returns it while the work continues in the
// background. It returns an error wrapping ErrDatasetMissing if the dataset
// is not there, ErrAlreadyRunning if a run is in progress on any replica,
// and ErrClosed after Close. A new run after a finished one re-walks the
// dataset; files already stored are not inserted again (ADR 0011).
func (s *Service) Start(ctx context.Context) (store.Run, error) {
	done, err := s.claim()
	if err != nil {
		return store.Run{}, err
	}
	run, lock, err := s.begin(ctx)
	if err != nil {
		s.unclaim(done)
		if s.life.Err() != nil {
			return store.Run{}, errors.Join(ErrClosed, err)
		}
		return store.Run{}, err
	}
	// The run outlives the request: its context derives from s.life.
	go s.execute(run, lock, done) //nolint:contextcheck // deliberately not the request context
	return run, nil
}

// claim reserves the service for one Start, so that concurrent Starts in
// this process do not race for the lock, and Close waits for the Start.
func (s *Service) claim() (chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.life.Err() != nil {
		return nil, ErrClosed
	}
	if s.done != nil {
		return nil, ErrAlreadyRunning
	}
	s.done = make(chan struct{})
	return s.done, nil
}

// unclaim ends a claim when its Start failed or its run ended.
func (s *Service) unclaim(done chan struct{}) {
	s.mu.Lock()
	s.done = nil
	s.mu.Unlock()
	close(done)
}

// begin checks the dataset, takes the cluster-wide lock and starts a run.
// Close cancels its database calls too.
func (s *Service) begin(ctx context.Context) (store.Run, store.IngestLock, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.life, cancel) //nolint:contextcheck // Close must also cancel a pending Start
	defer stop()

	if err := s.src.Available(); err != nil {
		return store.Run{}, nil, err
	}
	lock, err := s.st.AcquireIngestLock(ctx)
	if errors.Is(err, store.ErrLockHeld) {
		return store.Run{}, nil, ErrAlreadyRunning
	}
	if err != nil {
		return store.Run{}, nil, fmt.Errorf("acquiring ingest lock: %w", err)
	}
	run, err := s.st.BeginRun(ctx, lock)
	if err != nil {
		s.release(ctx, lock)
		return store.Run{}, nil, fmt.Errorf("beginning run: %w", err)
	}
	return run, lock, nil
}

// Status returns the state of the current or last run and the number of
// stored messages.
func (s *Service) Status(ctx context.Context) (Status, error) {
	run, err := s.st.LatestRun(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("ingestion status: %w", err)
	}
	total, err := s.st.CountMessages(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("ingestion status: %w", err)
	}
	st := Status{State: StateIdle, Run: run, TotalMessages: total}
	if run != nil {
		st.State = string(run.State)
	}
	return st, nil
}

// Close cancels a run in progress (or a Start in flight) and waits, bounded
// by ctx, until it has stopped, recorded itself as failed ("interrupted:
// shutdown") and released the lock. Later calls to Start return ErrClosed.
//
// The workers stop at once (inserts in flight are canceled); the final write
// and the lock release then normally take at most
// Config.ShutdownWriteTimeout, so a ctx of ShutdownWriteTimeout plus about a
// second is normally enough. A slow database can extend this by up to 5s
// (the store's bound on closing the lock's connection); the final write is
// then cut off, and the run is reported as interrupted by the crashed-owner
// path (ADR 0010).
func (s *Service) Close(ctx context.Context) error {
	s.stopLife(errShutdown)
	s.mu.Lock()
	s.closed = true
	done := s.done
	s.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for the ingestion run to stop: %w", ctx.Err())
	}
}

// execute runs the pipeline of one run and records its outcome. It owns lock
// and releases it before closing done.
func (s *Service) execute(run store.Run, lock store.IngestLock, done chan struct{}) {
	defer s.unclaim(done)
	s.m.running.Set(1)
	defer s.m.running.Set(0)

	started := time.Now()
	log := s.logger.With(slog.Int64("run_id", run.ID))
	log.Info("ingestion started", slog.Int("workers", s.cfg.Workers), slog.Int("batch_size", s.cfg.BatchSize))

	runCtx, cancel := context.WithCancelCause(s.life)
	defer cancel(nil)
	c := newCounters()
	stopMonitor := make(chan struct{})
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		s.monitor(runCtx, stopMonitor, cancel, run.ID, lock, c, started, log)
	}()

	p := &pipeline{s: s, runID: run.ID, c: c, log: newSampler()}
	workErr := p.run(runCtx)
	close(stopMonitor)

	state, msg := outcome(runCtx, workErr)
	final := c.snapshot()
	// The run context may be canceled; the last writes get a fresh budget,
	// a short one on shutdown so that it fits in the caller's close budget.
	finishCtx, cancelFinish := context.WithTimeout(context.WithoutCancel(runCtx), s.finalWriteBudget())
	defer cancelFinish()
	// A progress write in flight was canceled with runCtx; the final write
	// waits for it (on the lock's connection) within the same budget.
	select {
	case <-monitorDone:
	case <-finishCtx.Done():
	}
	if err := s.finish(finishCtx, lock, run.ID, state, final, msg); err != nil {
		log.Error("recording the end of the ingestion run failed", slog.Any("err", err))
	}
	// Release honors finishCtx: when it ends, the lock's socket is closed,
	// which also ends a write still in flight, so the monitor returns.
	if err := lock.Release(finishCtx); err != nil {
		log.Warn("releasing the ingest lock failed", slog.Any("err", err))
	}
	<-monitorDone

	attrs := append(counterAttrs(final),
		slog.String("state", string(state)), slog.Duration("duration", time.Since(started)))
	if msg != nil {
		attrs = append(attrs, slog.String("error", *msg))
	}
	if workErr != nil {
		attrs = append(attrs, slog.Any("err", workErr))
	}
	log.Info("ingestion finished", attrs...)
}

// outcome maps the pipeline result to the run's final state and error.
func outcome(runCtx context.Context, workErr error) (store.RunState, *string) {
	if workErr == nil {
		return store.RunCompleted, nil
	}
	msg := workErr.Error()
	if runCtx.Err() != nil {
		switch cause := context.Cause(runCtx); {
		case errors.Is(cause, errShutdown), errors.Is(cause, errLockLost), errors.Is(cause, errLockReleased),
			errors.Is(cause, errRunNotRunning):
			msg = cause.Error()
		}
	}
	return store.RunFailed, &msg
}

// monitor writes progress every ProgressInterval, logs it every
// ProgressLogInterval, and cancels the run with a cause when the lock is
// lost or the run row stops accepting progress. It returns when stop is
// closed or ctx ends.
func (s *Service) monitor(ctx context.Context, stop <-chan struct{}, cancel context.CancelCauseFunc, id int64,
	lock store.IngestLock, c *counters, started time.Time, log *slog.Logger,
) {
	flush := time.NewTicker(s.cfg.ProgressInterval)
	defer flush.Stop()
	report := time.NewTicker(s.cfg.ProgressLogInterval)
	defer report.Stop()
	lost := lockLost(lock)
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-lost:
			err := lock.Err()
			logLockEnd(log, runStopped(ctx, stop), "ingest lock ended, stopping the run", err)
			cancel(lockEndCause(err))
			return
		case <-report.C:
			log.Info("ingestion progress", append(counterAttrs(c.snapshot()),
				slog.Duration("elapsed", time.Since(started)))...)
		case <-flush.C:
			if cause := s.writeProgress(ctx, stop, lock, id, c, log); cause != nil {
				cancel(cause)
				return
			}
		}
	}
}

// lockLost returns a channel that is closed when the ingest lock is no
// longer held. It is the single lock-loss signal of a run; a lost lock is
// also reported by the next run write as ErrLockNotHeld. lockEndCause tells
// a real loss from a normal ending.
func lockLost(lock store.IngestLock) <-chan struct{} {
	return lock.Done()
}

// lockEndCause maps why the lock is no longer held (lock.Err, or an
// ErrLockNotHeld from a run write) to the run's cancel cause. Only a real
// loss, such as a dead session, is lock_lost. A closed store means the
// process is shutting down (in main, the store closes after the engine, so
// this only happens if the order is broken). A released lock means
// someone released this run's lock early; neither is recorded as a loss.
func lockEndCause(err error) error {
	switch {
	case errors.Is(err, store.ErrStoreClosed):
		return errShutdown
	case errors.Is(err, store.ErrLockReleased):
		return errLockReleased
	default:
		return errLockLost
	}
}

// lockWasLost reports whether a run write failed because the lock's session
// was lost (as opposed to the lock being released or the store closed). Only
// then may the lock be taken again to record the outcome.
func lockWasLost(err error) bool {
	return errors.Is(err, store.ErrLockNotHeld) && errors.Is(lockEndCause(err), errLockLost)
}

// runStopped reports whether the run is already over: its pipeline returned
// (stop is closed) or its context was canceled. The lock is then expected to
// end soon, released by execute.
func runStopped(ctx context.Context, stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return ctx.Err() != nil
	}
}

// logLockEnd logs that the lock ended under the monitor. That is an ERROR
// while the run is going, or when the lock was really lost. Once the run
// has stopped, a normal ending (execute released the lock, or the store
// closed) is expected: a progress write that outlived the final-write
// deadline and was ended by the release lands here, so it is only logged at
// DEBUG, with the reason (#37).
func logLockEnd(log *slog.Logger, stopped bool, msg string, err error) {
	if stopped && !lockWasLost(err) {
		log.Debug("ingest lock ended after the run stopped", slog.Any("reason", err))
		return
	}
	log.Error(msg, slog.Any("err", err))
}

// writeProgress stores the counters. It returns a cancel cause if the run
// must stop, and only logs other (transient) errors. stop is the monitor's
// stop channel, closed once the pipeline has returned.
func (s *Service) writeProgress(ctx context.Context, stop <-chan struct{}, lock store.IngestLock, id int64,
	c *counters, log *slog.Logger,
) error {
	// Canceling a run write keeps the lock (#5): on shutdown the update in
	// flight is dropped at once, and the final write carries the counters.
	wctx, cancel := context.WithTimeout(ctx, s.cfg.WriteTimeout)
	defer cancel()
	err := s.st.UpdateRunProgress(wctx, lock, id, c.snapshot())
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrLockNotHeld):
		logLockEnd(log, runStopped(ctx, stop), "ingest lock not held, stopping the run", err)
		return lockEndCause(err)
	case errors.Is(err, store.ErrRunNotRunning):
		log.Error("run no longer accepts progress, stopping it", slog.Any("err", err))
		return errRunNotRunning
	default:
		log.Warn("writing ingestion progress failed", slog.Any("err", err))
		return nil
	}
}

// finish records the final state. If the lock was lost (not released, and
// not ended by closing the store), Postgres released it with the session:
// unless another replica took it meanwhile, it is taken again just to record
// the outcome, so the run does not stay "running".
func (s *Service) finish(ctx context.Context, lock store.IngestLock, id int64, state store.RunState,
	c store.Counters, msg *string,
) error {
	err := s.st.FinishRun(ctx, lock, id, state, c, msg)
	if !errors.Is(err, store.ErrLockNotHeld) || !lockWasLost(lockErrOr(lock, err)) {
		return err
	}
	again, aerr := s.st.AcquireIngestLock(ctx)
	if aerr != nil {
		return errors.Join(err, fmt.Errorf("taking the ingest lock again to finish run %d: %w", id, aerr))
	}
	defer s.release(ctx, again)
	return s.st.FinishRun(ctx, again, id, state, c, msg)
}

// lockErrOr returns lock.Err() if the lock has ended, else err. The lock's
// own reason is the more precise one.
func lockErrOr(lock store.IngestLock, err error) error {
	if lerr := lock.Err(); lerr != nil {
		return lerr
	}
	return err
}

// finalWriteBudget is the time allowed for the writes that end a run or a
// failed start (the final run write, the lock release): WriteTimeout, or
// ShutdownWriteTimeout once Close has been called, so that they fit in the
// caller's close budget.
func (s *Service) finalWriteBudget() time.Duration {
	if s.life.Err() != nil {
		return s.cfg.ShutdownWriteTimeout
	}
	return s.cfg.WriteTimeout
}

// release releases lock with a fresh context bounded by finalWriteBudget:
// ctx may already be canceled (for example a Start canceled by Close).
func (s *Service) release(ctx context.Context, lock store.IngestLock) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.finalWriteBudget())
	defer cancel()
	if err := lock.Release(ctx); err != nil {
		s.logger.Warn("releasing the ingest lock failed", slog.Any("err", err))
	}
}

func counterAttrs(c store.Counters) []any {
	return []any{
		slog.Int64("files_seen", c.FilesSeen),
		slog.Int64("messages_processed", c.MessagesProcessed),
		slog.Int64("messages_inserted", c.MessagesInserted),
		slog.Int64("messages_skipped", c.MessagesSkipped),
		slog.Any("skip_reasons", c.SkipReasons),
		slog.Any("field_errors", c.FieldErrors),
	}
}
