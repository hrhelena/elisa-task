package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/hrhelena/elisa-task/internal/httpapi"
	"github.com/hrhelena/elisa-task/internal/ingest"
	"github.com/hrhelena/elisa-task/internal/mailparse"
	"github.com/hrhelena/elisa-task/internal/store"
)

// lockCheckTimeout bounds the IngestLockHeld pre-check of POST /start, so
// that a hung database leaves most of the request budget to the engine,
// whose dataset check then still answers dataset_missing.
const lockCheckTimeout = 2 * time.Second

// Every skip reason and field error the engine records (ADR 0007). A run's
// status lists them all, with 0 for those that did not occur, like the ADR
// 0011 example ("invalid_date": 0). The lists come from the packages that
// declare the constants, whose tests check that they are complete.
var (
	knownSkipReasons = ingest.AllSkipReasons()
	knownFieldErrors = issueKeys(mailparse.AllIssues())
)

func issueKeys(issues []mailparse.Issue) []string {
	out := make([]string, len(issues))
	for i, is := range issues {
		out[i] = string(is)
	}
	return out
}

// ingestService is the engine side of POST /start and GET /status (#7);
// *ingest.Service implements it.
type ingestService interface {
	Start(ctx context.Context) (store.Run, error)
	Status(ctx context.Context) (ingest.Status, error)
}

// lockChecker reports whether the cluster-wide ingest lock is held, without
// taking it; *store.Postgres implements it.
type lockChecker interface {
	IngestLockHeld(ctx context.Context) (bool, error)
}

// ingestion adapts the ingestion engine to httpapi.Ingester, so that httpapi
// does not depend on the ingestion or storage packages (ADR 0009).
type ingestion struct {
	svc     ingestService
	lock    lockChecker
	maildir string // shown in the dataset_missing message
	logger  *slog.Logger
}

var _ httpapi.Ingester = ingestion{}

// Start first checks the lock on the pool: taking it dials a dedicated
// connection, which a burst of POST /start during a run must not do (#29).
// The check races with other replicas, so ErrAlreadyRunning from the engine
// is still mapped. If the check itself fails, the engine decides: its own
// dataset check comes first, then taking the lock reports the database error.
func (a ingestion) Start(ctx context.Context) (httpapi.IngestionRun, error) {
	held, err := a.lockHeld(ctx)
	switch {
	case err != nil:
		// A client that went away is not a database problem.
		level := slog.LevelWarn
		if ctx.Err() != nil {
			level = slog.LevelDebug
		}
		a.logger.LogAttrs(ctx, level, "checking the ingest lock failed; trying to start anyway",
			slog.Any("err", err), slog.String("request_id", httpapi.RequestIDFromContext(ctx)))
	case held:
		return httpapi.IngestionRun{}, httpapi.ErrIngestionRunning
	}
	run, err := a.svc.Start(ctx)
	if err != nil {
		return httpapi.IngestionRun{}, mapStartErr(err, a.maildir)
	}
	return httpapi.IngestionRun{ID: run.ID, State: string(run.State), StartedAt: run.StartedAt}, nil
}

// lockHeld runs the pre-check under its own short timeout.
func (a ingestion) lockHeld(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, lockCheckTimeout)
	defer cancel()
	return a.lock.IngestLockHeld(ctx) //nolint:wrapcheck // the store error is already wrapped; it is only logged.
}

// mapStartErr translates the engine's Start errors. ErrClosed is checked
// first: a Start canceled by shutdown joins it with the error it failed with
// (for example a missing dataset), and shutdown must always win (503).
func mapStartErr(err error, maildir string) error {
	switch {
	case errors.Is(err, ingest.ErrClosed):
		return fmt.Errorf("%w: %w", httpapi.ErrIngesterClosed, err)
	case errors.Is(err, ingest.ErrAlreadyRunning):
		return fmt.Errorf("%w: %w", httpapi.ErrIngestionRunning, err)
	case errors.Is(err, ingest.ErrDatasetMissing):
		return &httpapi.DatasetMissingError{Path: maildir, Err: err}
	default:
		return err
	}
}

func (a ingestion) Status(ctx context.Context) (httpapi.IngestionStatus, error) {
	st, err := a.svc.Status(ctx)
	if err != nil {
		return httpapi.IngestionStatus{}, err //nolint:wrapcheck // the engine error is already wrapped; the adapter adds nothing.
	}
	out := httpapi.IngestionStatus{
		State:         st.State,
		TotalMessages: st.TotalMessages,
		SkipReasons:   map[string]int64{},
		FieldErrors:   map[string]int64{},
	}
	if r := st.Run; r != nil {
		out.RunID = &r.ID
		out.StartedAt = &r.StartedAt
		out.FinishedAt = r.FinishedAt
		out.UpdatedAt = &r.UpdatedAt
		out.FilesSeen = r.FilesSeen
		out.MessagesProcessed = r.MessagesProcessed
		out.MessagesSkipped = r.MessagesSkipped
		out.Error = r.Error
		out.SkipReasons = withKnownKeys(knownSkipReasons, r.SkipReasons)
		out.FieldErrors = withKnownKeys(knownFieldErrors, r.FieldErrors)
	}
	return out, nil
}

// withKnownKeys returns counts with every known key present (0 if absent).
// It never returns nil and does not modify counts.
func withKnownKeys(known []string, counts map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(known)+len(counts))
	for _, k := range known {
		out[k] = 0
	}
	maps.Copy(out, counts)
	return out
}
