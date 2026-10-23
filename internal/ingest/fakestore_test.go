package ingest

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/hrhelena/elisa-task/internal/store"
)

// fakeStore is an in-memory Store with the run and lock semantics of
// store.Postgres (#5), plus failure injection.
type fakeStore struct {
	mu       sync.Mutex
	msgs     map[string]store.Message
	runs     []store.Run
	lock     *fakeLock // the held lock, nil if free
	updates  int       // successful UpdateRunProgress calls
	inserts  int       // InsertMessages calls
	insertFn func(call int) error
	// insertGate, if set, is received from before every insert, ignoring
	// the context (a stuck database). insertStarted gets a value (if room)
	// when an insert begins.
	insertGate    chan struct{}
	insertStarted chan struct{}
	// insertBlocks makes every insert wait for its context (an unresponsive
	// database that honors cancellation).
	insertBlocks bool
	// acquireBlocks makes AcquireIngestLock wait for its context.
	acquireBlocks bool
	// updateBlocks makes UpdateRunProgress wait for its context.
	updateBlocks bool
	// updateUntilLockEnds makes UpdateRunProgress ignore its context and
	// wait until the lock ends, then return updateEndErr, or the lock's Err
	// if that is nil: a write on a slow database that only the release (which
	// closes the lock's connection) ends. updateStarted gets a value (if
	// room) when such a write begins.
	updateUntilLockEnds bool
	updateEndErr        error
	updateStarted       chan struct{}
	// beginBlocks makes BeginRun wait for its context. beginStarted gets a
	// value (if room) when BeginRun begins.
	beginBlocks  bool
	beginStarted chan struct{}
	// releaseBlocks makes Release wait for its context, like a release
	// stuck behind a slow write. releaseBudgets records the time each
	// Release had left on its context (-1 without a deadline).
	releaseBlocks  bool
	releaseBudgets []time.Duration

	closed   bool
	acquires int            // successful AcquireIngestLock calls
	finishes []finishRecord // every FinishRun call, accepted or not
}

func (f *fakeStore) releaseRecords() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.releaseBudgets...)
}

type finishRecord struct {
	state store.RunState
	msg   string
	err   error
}

func newFakeStore() *fakeStore { return &fakeStore{msgs: map[string]store.Message{}} }

type fakeLock struct {
	fs   *fakeStore
	done chan struct{}
	once sync.Once
	err  error
}

func (l *fakeLock) Done() <-chan struct{} { return l.done }

func (l *fakeLock) Err() error {
	select {
	case <-l.done:
		return l.err
	default:
		return nil
	}
}

func (l *fakeLock) end(err error) {
	l.once.Do(func() {
		l.err = err
		close(l.done)
	})
	l.fs.mu.Lock()
	if l.fs.lock == l {
		l.fs.lock = nil
	}
	l.fs.mu.Unlock()
}

// Release ends the lock like store.Postgres: its Err wraps ErrLockReleased.
func (l *fakeLock) Release(ctx context.Context) error {
	budget := time.Duration(-1)
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}
	l.fs.mu.Lock()
	l.fs.releaseBudgets = append(l.fs.releaseBudgets, budget)
	blocks := l.fs.releaseBlocks
	l.fs.mu.Unlock()
	if blocks {
		<-ctx.Done()
	}
	l.end(fmt.Errorf("%w: %w", store.ErrLockNotHeld, store.ErrLockReleased))
	return nil
}

// lose simulates the lock's session dying: a real loss.
func (l *fakeLock) lose() { l.end(fmt.Errorf("%w: lock connection lost", store.ErrLockNotHeld)) }

// closeStore simulates store.Postgres.Close: the held lock ends with
// ErrStoreClosed and the store refuses new locks.
func (f *fakeStore) closeStore() {
	f.mu.Lock()
	f.closed = true
	l := f.lock
	f.mu.Unlock()
	if l != nil {
		l.end(fmt.Errorf("%w: %w", store.ErrLockNotHeld, store.ErrStoreClosed))
	}
}

func (f *fakeStore) acquireCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acquires
}

func (f *fakeStore) finishRecords() []finishRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]finishRecord(nil), f.finishes...)
}

func (f *fakeStore) heldLock() *fakeLock {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lock
}

func (f *fakeStore) AcquireIngestLock(ctx context.Context) (store.IngestLock, error) {
	if f.acquireBlocks {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, fmt.Errorf("acquiring ingest lock: %w", store.ErrStoreClosed)
	}
	if f.lock != nil {
		return nil, store.ErrLockHeld
	}
	f.acquires++
	f.lock = &fakeLock{fs: f, done: make(chan struct{})}
	return f.lock, nil
}

// held must be called with f.mu held. Like store.Postgres, an ended lock
// reports why it ended.
func (f *fakeStore) held(lock store.IngestLock) error {
	l, ok := lock.(*fakeLock)
	if ok && l != f.lock {
		if err := l.Err(); err != nil {
			return err
		}
	}
	if !ok || l != f.lock {
		return fmt.Errorf("%w: not the held lock", store.ErrLockNotHeld)
	}
	return nil
}

func (f *fakeStore) BeginRun(ctx context.Context, lock store.IngestLock) (store.Run, error) {
	if f.beginBlocks {
		select {
		case f.beginStarted <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return store.Run{}, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.held(lock); err != nil {
		return store.Run{}, err
	}
	now := time.Now()
	for i := range f.runs {
		if f.runs[i].State == store.RunRunning {
			msg := store.ErrorInterrupted
			f.runs[i].State, f.runs[i].Error, f.runs[i].FinishedAt = store.RunFailed, &msg, &now
		}
	}
	run := store.Run{ID: int64(len(f.runs) + 1), State: store.RunRunning, StartedAt: now, UpdatedAt: now,
		Counters: store.Counters{SkipReasons: map[string]int64{}, FieldErrors: map[string]int64{}}}
	f.runs = append(f.runs, run)
	return run, nil
}

func (f *fakeStore) run(id int64) (*store.Run, error) {
	if id < 1 || int(id) > len(f.runs) || f.runs[id-1].State != store.RunRunning {
		return nil, fmt.Errorf("run %d: %w", id, store.ErrRunNotRunning)
	}
	return &f.runs[id-1], nil
}

func (f *fakeStore) UpdateRunProgress(ctx context.Context, lock store.IngestLock, id int64, c store.Counters) error {
	if f.updateBlocks {
		<-ctx.Done()
	}
	if l, ok := lock.(*fakeLock); ok && f.updateUntilLockEnds {
		select {
		case f.updateStarted <- struct{}{}:
		default:
		}
		<-l.Done()
		if f.updateEndErr != nil {
			return f.updateEndErr
		}
		return l.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.held(lock); err != nil {
		return err
	}
	r, err := f.run(id)
	if err != nil {
		return err
	}
	r.Counters = cloneCounters(c)
	f.updates++
	return nil
}

func (f *fakeStore) FinishRun(ctx context.Context, lock store.IngestLock, id int64, state store.RunState,
	c store.Counters, errMsg *string,
) (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer func() {
		msg := ""
		if errMsg != nil {
			msg = *errMsg
		}
		f.finishes = append(f.finishes, finishRecord{state: state, msg: msg, err: err})
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.held(lock); err != nil {
		return err
	}
	r, err := f.run(id)
	if err != nil {
		return err
	}
	now := time.Now()
	r.Counters, r.State, r.Error, r.FinishedAt = cloneCounters(c), state, errMsg, &now
	return nil
}

// LatestRun reports a running run without a lock holder as interrupted,
// like store.Postgres.
func (f *fakeStore) LatestRun(context.Context) (*store.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.runs) == 0 {
		return nil, nil
	}
	r := f.runs[len(f.runs)-1]
	r.Counters = cloneCounters(r.Counters)
	if r.State == store.RunRunning && f.lock == nil {
		msg := store.ErrorInterrupted
		r.State, r.Error = store.RunFailed, &msg
	}
	return &r, nil
}

// failRun marks a run failed behind the engine's back, as another replica
// would after taking a lost lock.
func (f *fakeStore) failRun(id int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	msg := store.ErrorInterrupted
	f.runs[id-1].State, f.runs[id-1].Error = store.RunFailed, &msg
}

func (f *fakeStore) InsertMessages(ctx context.Context, msgs []store.Message) (int64, error) {
	select {
	case f.insertStarted <- struct{}{}:
	default:
	}
	if f.insertGate != nil {
		<-f.insertGate
	}
	if f.insertBlocks {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inserts++
	if f.insertFn != nil {
		if err := f.insertFn(f.inserts); err != nil {
			return 0, err
		}
	}
	var n int64
	for _, m := range msgs {
		if _, ok := f.msgs[m.SourcePath]; !ok {
			f.msgs[m.SourcePath] = m
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) CountMessages(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.msgs)), nil
}

func (f *fakeStore) message(path string) (store.Message, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.msgs[path]
	return m, ok
}

func (f *fakeStore) progressWrites() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updates
}

func cloneCounters(c store.Counters) store.Counters {
	c.SkipReasons = maps.Clone(c.SkipReasons)
	c.FieldErrors = maps.Clone(c.FieldErrors)
	if c.SkipReasons == nil {
		c.SkipReasons = map[string]int64{}
	}
	if c.FieldErrors == nil {
		c.FieldErrors = map[string]int64{}
	}
	return c
}
