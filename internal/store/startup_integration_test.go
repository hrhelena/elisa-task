//go:build integration

package store

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3/lock"
)

// A server that answers but rejects the configuration is a permanent error
// (PR #31 review): Open must not wrap ErrUnreachable, so startup exits at
// once instead of retrying for the whole budget. The password stays out of
// the error.
func TestOpenRejectedConfigurationIsPermanent(t *testing.T) {
	t.Parallel()
	base, err := url.Parse(testEnv.baseURL)
	if err != nil {
		t.Fatalf("parsing base URL: %v", err)
	}
	with := func(edit func(u *url.URL)) string {
		u := *base
		edit(&u)
		return u.String()
	}
	tests := []struct {
		name, url, wantSub string
	}{
		{"wrong password", with(func(u *url.URL) { u.User = url.UserPassword("enron", "s3cret-typo") }), "28P01"},
		{"unknown role", with(func(u *url.URL) { u.User = url.UserPassword("nobody", "s3cret-typo") }), "28P01"},
		{"missing database", with(func(u *url.URL) { u.Path = "/does_not_exist" }), "3D000"},
		{"TLS required, server without TLS", with(func(u *url.URL) { u.RawQuery = "sslmode=require" }), "server refused TLS connection"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := Open(t.Context(), tt.url, discardLogger())
			if err == nil {
				s.Close()
				t.Fatal("Open succeeded, want a permanent error")
			}
			if errors.Is(err, ErrUnreachable) {
				t.Errorf("err wraps ErrUnreachable, so startup would retry it: %v", err)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("err = %v, want it to contain %q", err, tt.wantSub)
			}
			if strings.Contains(err.Error(), "s3cret-typo") {
				t.Errorf("err leaks the password: %v", err)
			}
		})
	}
}

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
