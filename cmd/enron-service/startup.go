package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/hrhelena/elisa-task/internal/config"
	"github.com/hrhelena/elisa-task/internal/store"
)

// openDatabase opens the store within cfg.DBStartupTimeout, retrying while
// the database is unreachable (#27). The budget also covers the migration
// lock wait and the migrations, which store.Open runs under the same
// context. It returns early, with an error, when ctx is canceled.
func openDatabase(ctx context.Context, cfg config.Config, openStore openStoreFunc, b backoff, logger *slog.Logger) (dbStore, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.DBStartupTimeout)
	defer cancel()
	return retryOpen(ctx, func(ctx context.Context) (dbStore, error) {
		return openStore(ctx, cfg.DatabaseURL, logger)
	}, b, logger)
}

// retryOpen calls open until it succeeds, fails with an error that does not
// wrap store.ErrUnreachable, or ctx ends. Each retry is logged at WARN with
// its attempt number and the error. The error never contains the database
// password: pgx redacts it from connection and parse errors.
func retryOpen(ctx context.Context, open func(context.Context) (dbStore, error), b backoff, logger *slog.Logger) (dbStore, error) {
	for attempt := 1; ; attempt++ {
		st, err := open(ctx)
		if err == nil {
			return st, nil
		}
		if !errors.Is(err, store.ErrUnreachable) {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, giveUp(ctx, attempt, err)
		}
		delay := b.delay(attempt)
		logger.WarnContext(ctx, "database not ready, retrying",
			slog.Int("attempt", attempt), slog.Duration("retry_in", delay), slog.Any("err", err))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, giveUp(ctx, attempt, err)
		case <-timer.C:
		}
	}
}

// giveUp is retryOpen's error once ctx has ended: it wraps both why the loop
// stopped (deadline or cancellation) and the last connection error.
func giveUp(ctx context.Context, attempts int, last error) error {
	return fmt.Errorf("giving up after %d attempt(s): %w (last error: %w)", attempts, ctx.Err(), last)
}

// backoff is a capped exponential backoff with "equal jitter": the delay
// before retry n is d/2 plus a random part of up to d/2, where
// d = min(initial * 2^(n-1), max). The fixed half keeps retries from
// hammering the server; the random half spreads replicas that started
// together.
type backoff struct {
	initial, max time.Duration
	// jitter returns a duration in [0, d]. Tests make it deterministic.
	jitter func(d time.Duration) time.Duration
}

// delay returns the wait before retry attempt (1-based).
func (b backoff) delay(attempt int) time.Duration {
	d := b.initial
	// Stop doubling at max, so long retry loops cannot overflow.
	for i := 1; i < attempt && d < b.max; i++ {
		d *= 2
	}
	d = min(d, b.max)
	half := d / 2
	return half + b.jitter(d-half)
}

// randomJitter returns a uniformly random duration in [0, d].
func randomJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return rand.N(d + 1) //nolint:gosec // jitter, not security sensitive.
}
