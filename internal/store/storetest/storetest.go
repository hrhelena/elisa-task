//go:build integration

package storetest

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// Image is the Postgres image used by integration tests.
	Image = "postgres:16-alpine"
	// startupDeadline replaces the module's 60s default: initdb is slow on
	// loaded Docker hosts and CI runners.
	startupDeadline = 3 * time.Minute
)

// Postgres is a running test container.
type Postgres struct {
	*tcpostgres.PostgresContainer
	// URL connects to the container's default database "enron".
	URL string

	dbSeq atomic.Int64 // names the databases made by CreateDatabase
}

// Start starts a Postgres container and waits until it accepts connections.
// The caller must call Terminate.
func Start(ctx context.Context) (*Postgres, error) {
	ctr, err := tcpostgres.Run(ctx, Image,
		tcpostgres.WithDatabase("enron"),
		tcpostgres.WithUsername("enron"),
		tcpostgres.WithPassword("enron"),
		// tcpostgres.BasicWaitStrategies with a longer deadline.
		testcontainers.WithWaitStrategyAndDeadline(startupDeadline,
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("starting postgres container: %w", err)
	}
	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("postgres connection string: %w", err)
	}
	return &Postgres{PostgresContainer: ctr, URL: url}, nil
}

// Terminate stops and removes the container.
func (p *Postgres) Terminate() error {
	return testcontainers.TerminateContainer(p.PostgresContainer)
}

// Stop stops the container without removing it, so clients see the
// database go away.
func (p *Postgres) Stop(ctx context.Context) error {
	timeout := 5 * time.Second
	return p.PostgresContainer.Stop(ctx, &timeout)
}

// CreateDatabase creates an empty database with a unique name in the
// container and returns its connection string, so that a test can have data
// of its own on a shared container. The database lives as long as the
// container.
func (p *Postgres) CreateDatabase(ctx context.Context) (string, error) {
	name := fmt.Sprintf("test_%d", p.dbSeq.Add(1))
	conn, err := pgx.Connect(ctx, p.URL)
	if err != nil {
		return "", fmt.Errorf("connecting to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		return "", fmt.Errorf("creating database %s: %w", name, err)
	}
	u, err := url.Parse(p.URL)
	if err != nil {
		return "", fmt.Errorf("parsing connection string: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}
