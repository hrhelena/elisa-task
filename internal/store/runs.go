package store

import (
	"context"
	"errors"
	"time"
)

// RunState is the lifecycle state of an ingestion run (ADR 0010). There is no
// "idle" state in storage: no run at all is reported as idle (ADR 0011).
type RunState string

// Run states, as stored in ingestion_runs.state.
const (
	RunRunning   RunState = "running"
	RunCompleted RunState = "completed"
	RunFailed    RunState = "failed"
)

// ErrorInterrupted is the error recorded for a run whose owner died while it
// was running (ADR 0010).
const ErrorInterrupted = "interrupted"

var (
	// ErrLockHeld is returned by AcquireIngestLock when another session holds
	// the ingest lock, that is, a run is in progress somewhere in the cluster.
	ErrLockHeld = errors.New("ingestion lock held by another session")
	// ErrLockNotHeld is returned by BeginRun when no session holds the ingest
	// lock: runs may only be started under the lock.
	ErrLockNotHeld = errors.New("ingestion lock not held")
	// ErrRunNotRunning is returned when updating or finishing a run that does
	// not exist or is no longer running.
	ErrRunNotRunning = errors.New("ingestion run is not running")
	// ErrInvalidRunState is returned by FinishRun for a state other than
	// completed or failed.
	ErrInvalidRunState = errors.New("invalid final run state")
)

// Counters are the progress counters of a run (ADR 0007). At the end of a run
// FilesSeen = MessagesProcessed + MessagesSkipped. SkipReasons counts skipped
// files per reason code and FieldErrors counts field-level problems per code;
// nil maps are stored as empty objects, and maps read back are never nil.
type Counters struct {
	FilesSeen         int64
	MessagesProcessed int64 // parsed OK and stored (inserted now or already present)
	MessagesInserted  int64 // new rows written by this run
	MessagesSkipped   int64 // file-level problems, not counted as messages
	SkipReasons       map[string]int64
	FieldErrors       map[string]int64
}

// Run is one ingestion run.
type Run struct {
	ID         int64
	State      RunState
	StartedAt  time.Time
	FinishedAt *time.Time // nil while running
	Counters
	Error     *string
	UpdatedAt time.Time
}

// Finished reports whether the run completed successfully. A failed run has
// not finished its job (ADR 0011).
func (r *Run) Finished() bool { return r.State == RunCompleted }

// IngestLocker is the cluster-wide single-flight lock for ingestion (ADR 0010).
type IngestLocker interface {
	// AcquireIngestLock takes the lock or returns ErrLockHeld. The lock is
	// held until release is called, or until the process dies.
	AcquireIngestLock(ctx context.Context) (release func(context.Context) error, err error)
	// IngestLockHeld reports whether any session holds the lock, without
	// taking it.
	IngestLockHeld(ctx context.Context) (bool, error)
}

// RunWriter records the lifecycle and progress of ingestion runs.
type RunWriter interface {
	// BeginRun must be called while holding the ingest lock. It marks runs
	// left running by a dead owner as failed ("interrupted") and starts a new
	// running run.
	BeginRun(ctx context.Context) (Run, error)
	UpdateRunProgress(ctx context.Context, id int64, c Counters) error
	// FinishRun sets the final state (completed or failed) and counters.
	FinishRun(ctx context.Context, id int64, state RunState, c Counters, errMsg *string) error
}

// RunReader reads ingestion run state for GET /status.
type RunReader interface {
	// LatestRun returns the most recent run, or (nil, nil) if there has never
	// been one (reported as idle, ADR 0011).
	LatestRun(ctx context.Context) (*Run, error)
}
