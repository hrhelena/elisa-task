package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// Migration session lock: retry every second (polled more often than goose's
// 5s default, so replicas that lose the race start sooner once the winner is
// done). The number of attempts follows the caller's deadline, see
// lockAttempts; without a deadline it is goose's default budget of 5 minutes.
const (
	migrationLockRetryPeriod          = time.Second
	migrationLockDefaultRetryAttempts = 300
)

// lockAttempts returns goose's lock retry count: after the first try, it
// retries every migrationLockRetryPeriod that many times, so the configured
// wait (attempts seconds) is the caller's remaining budget (the startup
// budget, DB_STARTUP_TIMEOUT) rounded down. With at least 1s left the wait
// therefore ends inside the budget with goose's "failed to acquire lock"
// error. goose requires at least one retry, so with under 1s left the wait
// would be longer than the budget; it never outlasts it, though, because
// the retry loop checks ctx: the deadline ends it and the error wraps
// context.DeadlineExceeded instead. Either way it is permanent (exit 1).
func lockAttempts(ctx context.Context, now time.Time) uint64 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return migrationLockDefaultRetryAttempts
	}
	remaining := deadline.Sub(now)
	if remaining <= migrationLockRetryPeriod {
		return 1
	}
	return uint64(remaining / migrationLockRetryPeriod)
}

// migrate applies all pending embedded migrations (ADR 0013). A Postgres
// session-level advisory lock serializes concurrent callers, so replicas
// starting together apply each migration exactly once.
//
// ctx carries the startup deadline and can expire while a migration runs.
// goose runs each migration and its version row in one transaction, so a
// canceled migration rolls back completely and is retried on the next
// start. A future "-- +goose NO TRANSACTION" migration (CREATE INDEX
// CONCURRENTLY, for example) has no such protection: it can be cut off
// halfway, so it must be safe to run again (IF NOT EXISTS, and so on).
func migrate(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	migrations, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		return fmt.Errorf("opening embedded migrations: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker(
		lock.WithLockTimeout(uint64(migrationLockRetryPeriod/time.Second), lockAttempts(ctx, time.Now())),
	)
	if err != nil {
		return fmt.Errorf("creating migration locker: %w", err)
	}

	// goose needs database/sql. Closing this *sql.DB does not close the pool.
	db := stdlib.OpenDBFromPool(pool)
	defer func() { _ = db.Close() }()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations,
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true),
		goose.WithSlog(logger),
	)
	if err != nil {
		return fmt.Errorf("creating migration provider: %w", err)
	}

	results, err := provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("applying migrations: %w", err)
	}
	version, err := provider.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("reading schema version: %w", err)
	}
	logger.InfoContext(ctx, "database migrations applied",
		slog.Int("applied", len(results)), slog.Int64("schema_version", version))
	return nil
}
