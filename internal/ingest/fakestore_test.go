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

func (l *fakeLock) Release(context.Context) error {
	l.end(fmt.Errorf("%w: released", store.ErrLockNotHeld))
	return nil
}

// lose simulates the lock's session dying.
func (l *fakeLock) lose() { l.end(fmt.Errorf("%w: lock connection lost", store.ErrLockNotHeld)) }

func (f *fakeStore) heldLock() *fakeLock {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lock
}

func (f *fakeStore) AcquireIngestLock(context.Context) (store.IngestLock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lock != nil {
		return nil, store.ErrLockHeld
	}
	f.lock = &fakeLock{fs: f, done: make(chan struct{})}
	return f.lock, nil
}

// held must be called with f.mu held.
func (f *fakeStore) held(lock store.IngestLock) error {
	if l, ok := lock.(*fakeLock); !ok || l != f.lock {
		return fmt.Errorf("%w: not the held lock", store.ErrLockNotHeld)
	}
	return nil
}

func (f *fakeStore) BeginRun(_ context.Context, lock store.IngestLock) (store.Run, error) {
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
) error {
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
