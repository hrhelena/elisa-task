// Command enron-service ingests the Enron maildir dataset into Postgres and
// serves email metrics over HTTP.
//
// This is the composition root: it is the only place that knows concrete
// types (ADR 0009). Configuration, the HTTP server and graceful shutdown are
// wired in here by later tickets.
package main

import (
	"context"
	"io"
	"log/slog"
	"os"
)

func main() {
	logger := newLogger(os.Stdout)
	if err := run(context.Background(), logger); err != nil {
		logger.Error("exiting", slog.Any("err", err))
		os.Exit(1)
	}
}

// newLogger returns the service's structured JSON logger writing to w.
func newLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, nil)).With(slog.String("service", "enron-service"))
}

// run is the testable body of main. It returns instead of exiting so that
// deferred cleanup runs and tests can drive it.
func run(ctx context.Context, logger *slog.Logger) error {
	logger.InfoContext(ctx, "starting")
	return nil
}
