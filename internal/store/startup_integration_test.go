//go:build integration

package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3/lock"
)

// The migration lock wait ends within the caller's budget (#27): while
// another instance holds the goose lock, Open gives up before ctx's deadline
// with goose's lock error, a permanent error, instead of waiting 5 minutes.
func TestMigrationLockWaitFollowsTheBudget(t *testing.T) {
	t.Parallel()
	dbURL := newDatabase(t)

	// Another "replica" holds the migration lock for the whole test.
	holder, err := pgx.Connect(t.Context(), dbURL)
	if err != nil {
		t.Fatalf("connecting lock holder: %v", err)
	}
	t.Cleanup(func() { _ = holder.Close(context.Background()) })
	if _, err := holder.Exec(t.Context(), "SELECT pg_advisory_lock($1)", lock.DefaultLockID); err != nil {
		t.Fatalf("taking the migration lock: %v", err)
	}

	const budget = 3 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()
	start := time.Now()
	s, err := Open(ctx, dbURL, discardLogger())
	elapsed := time.Since(start)
	if err == nil {
		s.Close()
		t.Fatal("Open succeeded while another instance held the migration lock")
	}
	if errors.Is(err, ErrUnreachable) {
		t.Errorf("lock timeout wraps ErrUnreachable, so it would be retried: %v", err)
	}
	if !strings.Contains(err.Error(), "lock") {
		t.Errorf("err = %v, want the migration lock error", err)
	}
	if elapsed < time.Second || elapsed > budget+time.Second {
		t.Errorf("gave up after %v, want within the %v budget", elapsed, budget)
	}
}
