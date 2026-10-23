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

// lockAttempts returns how many times to try the migration lock, one
// migrationLockRetryPeriod apart, so that the wait ends with the caller's
// deadline (the startup budget, DB_STARTUP_TIMEOUT) rather than being
// configured for longer than the caller allows.
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
