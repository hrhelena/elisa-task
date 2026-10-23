//go:build integration

package storetest

import (
	"context"
	"fmt"
	"time"

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
