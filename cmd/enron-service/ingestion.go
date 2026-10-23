package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/hrhelena/elisa-task/internal/httpapi"
	"github.com/hrhelena/elisa-task/internal/ingest"
	"github.com/hrhelena/elisa-task/internal/store"
)

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
	held, err := a.lock.IngestLockHeld(ctx)
	switch {
	case err != nil:
		a.logger.WarnContext(ctx, "checking the ingest lock failed; trying to start anyway", slog.Any("err", err))
	case held:
		return httpapi.IngestionRun{}, httpapi.ErrIngestionRunning
	}
	run, err := a.svc.Start(ctx)
	if err != nil {
		return httpapi.IngestionRun{}, mapStartErr(err, a.maildir)
	}
	return httpapi.IngestionRun{ID: run.ID, State: string(run.State), StartedAt: run.StartedAt}, nil
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
		if r.SkipReasons != nil {
			out.SkipReasons = r.SkipReasons
		}
		if r.FieldErrors != nil {
			out.FieldErrors = r.FieldErrors
		}
	}
	return out, nil
}
