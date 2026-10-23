package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// Migration session lock: retry every second for up to 5 minutes (goose's
// default budget, polled more often so replicas that lose the race start
// sooner once the winner is done).
const (
	migrationLockRetryPeriodSeconds = 1
	migrationLockRetryAttempts      = 300
)

// migrate applies all pending embedded migrations (ADR 0013). A Postgres
// session-level advisory lock serializes concurrent callers, so replicas
// starting together apply each migration exactly once.
func migrate(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	migrations, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		return fmt.Errorf("opening embedded migrations: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker(
		lock.WithLockTimeout(migrationLockRetryPeriodSeconds, migrationLockRetryAttempts),
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
