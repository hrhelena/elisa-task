package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/hrhelena/elisa-task/internal/fetch"
)

// Error codes of POST /start (ADR 0011).
const (
	CodeIngestionRunning = "ingestion_running"
	CodeDatasetMissing   = "dataset_missing"
)

// Ingestion states reported by GET /status (ADR 0011). StateIdle means no run
// has ever started.
const (
	StateIdle      = "idle"
	StateRunning   = "running"
	StateCompleted = "completed"
	StateFailed    = "failed"
)

// Errors an Ingester returns from Start, mapped to responses by startHandler.
// The adapter in main translates the engine's errors into these, so this
// package does not depend on the ingestion or storage packages (ADR 0009).
var (
	// ErrIngestionRunning: a run is in progress on any replica (409).
	ErrIngestionRunning = errors.New("an ingestion is already running")
	// ErrIngesterClosed: the service is shutting down (503). It takes
	// precedence over every other error it is joined with.
	ErrIngesterClosed = errors.New("ingestion service is shutting down")
)

// DatasetMissingError is returned by Ingester.Start when the maildir is
// absent or unusable (not a directory, a symlink, unreadable): 409
// dataset_missing.
type DatasetMissingError struct {
	// Path is where the maildir is expected; it is shown to the caller.
	Path string
	Err  error
}

func (e *DatasetMissingError) Error() string {
	return fmt.Sprintf("dataset missing at %s: %v", e.Path, e.Err)
}

func (e *DatasetMissingError) Unwrap() error { return e.Err }

// IngestionRun is a run that Start has just started.
type IngestionRun struct {
	ID        int64
	State     string
	StartedAt time.Time
}

// IngestionStatus is the ingestion state for GET /status. Run fields are
// zero (and RunID nil) when State is StateIdle.
type IngestionStatus struct {
	State             string
	RunID             *int64
	StartedAt         *time.Time
	FinishedAt        *time.Time
	UpdatedAt         *time.Time
	FilesSeen         int64
	MessagesProcessed int64
	MessagesSkipped   int64
	SkipReasons       map[string]int64
	FieldErrors       map[string]int64
	Error             *string
	// TotalMessages is the number of stored messages.
	TotalMessages int64
}

// Ingester starts ingestion runs and reports their state (ADR 0010, 0011).
type Ingester interface {
	// Start starts a run unless one is in progress anywhere in the cluster.
	// It returns ErrIngestionRunning, a *DatasetMissingError,
	// ErrIngesterClosed, or another error when the database fails.
	Start(ctx context.Context) (IngestionRun, error)
	// Status returns the state of the current or last run.
	Status(ctx context.Context) (IngestionStatus, error)
}

// DatasetReporter reports the dataset state for the dataset section of GET
// /status (#10); *fetch.Fetcher implements it.
type DatasetReporter interface {
	Progress() fetch.Progress
}

// Timeouts of the ingestion endpoints. Start may dial a connection for the
// ingest lock, so it gets more time than a plain read.
const (
	startTimeout  = 10 * time.Second
	statusTimeout = 5 * time.Second
)

const unavailableMessage = "the database is unavailable, retry later"

type startResponse struct {
	RunID     int64     `json:"run_id"`
	State     string    `json:"state"`
	StartedAt time.Time `json:"started_at"`
}

// startHandler serves POST /start. Any request body is ignored.
func startHandler(ing Ingester, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), startTimeout)
		defer cancel()
		run, err := ing.Start(ctx)
		var missing *DatasetMissingError
		switch {
		case err == nil:
		case errors.Is(err, ErrIngesterClosed):
			// First: a Start canceled by shutdown may join ErrIngesterClosed
			// with the error it was failing with; shutdown always wins.
			writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the service is shutting down")
			return
		case errors.Is(err, ErrIngestionRunning):
			writeError(w, http.StatusConflict, CodeIngestionRunning, "an ingestion is already running")
			return
		case errors.As(err, &missing):
			writeError(w, http.StatusConflict, CodeDatasetMissing,
				fmt.Sprintf("maildir not found at %s; call POST /fetch first", missing.Path))
			return
		default:
			logFailure(ctx, r, logger, "starting ingestion failed", err)
			writeError(w, http.StatusServiceUnavailable, CodeUnavailable, unavailableMessage)
			return
		}
		w.Header().Set("Location", "/status")
		writeJSON(w, http.StatusAccepted, startResponse{RunID: run.ID, State: run.State, StartedAt: run.StartedAt.UTC()})
	})
}

type statusResponse struct {
	State             string           `json:"state"`
	Finished          bool             `json:"finished"`
	MessagesProcessed int64            `json:"messages_processed"`
	TotalMessages     int64            `json:"total_messages"`
	MessagesSkipped   int64            `json:"messages_skipped"`
	FilesSeen         int64            `json:"files_seen"`
	SkipReasons       map[string]int64 `json:"skip_reasons"`
	FieldErrors       map[string]int64 `json:"field_errors"`
	RunID             *int64           `json:"run_id"`
	StartedAt         *time.Time       `json:"started_at"`
	FinishedAt        *time.Time       `json:"finished_at"`
	UpdatedAt         *time.Time       `json:"updated_at"`
	Error             *string          `json:"error"`
	Dataset           *datasetJSON     `json:"dataset,omitempty"`
}

func newStatusResponse(s IngestionStatus) statusResponse {
	resp := statusResponse{
		State:             s.State,
		Finished:          s.State == StateCompleted, // ADR 0011: a failed run has not finished
		MessagesProcessed: s.MessagesProcessed,
		TotalMessages:     s.TotalMessages,
		MessagesSkipped:   s.MessagesSkipped,
		FilesSeen:         s.FilesSeen,
		SkipReasons:       nonNil(s.SkipReasons),
		FieldErrors:       nonNil(s.FieldErrors),
		RunID:             s.RunID,
		StartedAt:         utc(s.StartedAt),
		FinishedAt:        utc(s.FinishedAt),
		UpdatedAt:         utc(s.UpdatedAt),
		Error:             s.Error,
	}
	if resp.State == "" {
		resp.State = StateIdle
	}
	return resp
}

// statusHandler serves GET /status: 200 whenever the database answers, 503
// otherwise. datasets may be nil (no dataset section).
func statusHandler(ing Ingester, datasets DatasetReporter, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		ctx, cancel := context.WithTimeout(r.Context(), statusTimeout)
		defer cancel()
		st, err := ing.Status(ctx)
		if err != nil {
			logFailure(ctx, r, logger, "reading ingestion status failed", err)
			writeError(w, http.StatusServiceUnavailable, CodeUnavailable, unavailableMessage)
			return
		}
		resp := newStatusResponse(st)
		if datasets != nil {
			d := newDatasetJSON(datasets.Progress())
			resp.Dataset = &d
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

// logFailure logs a dependency failure at ERROR, or at DEBUG when the client
// went away (nobody reads the response and the database is not at fault).
func logFailure(ctx context.Context, r *http.Request, logger *slog.Logger, msg string, err error) {
	level := slog.LevelError
	if r.Context().Err() != nil {
		level, msg = slog.LevelDebug, msg+" (canceled by the client)"
	}
	logger.LogAttrs(ctx, level, msg, slog.Any("err", err), slog.String("request_id", RequestIDFromContext(ctx)))
}

func nonNil(m map[string]int64) map[string]int64 {
	if m == nil {
		return map[string]int64{}
	}
	return m
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}
