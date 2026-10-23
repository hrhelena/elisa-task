//go:build integration

package storetest

import (
	"context"
	"fmt"
	"net/url"
	"strings"
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
//
// Like the store's own test databases (#4), it uses the ICU en-US
// collation, not the image's default: the alpine image (musl) sorts text in
// byte order, which would hide any query whose result depends on the
// database collation, while glibc and ICU servers sort punctuation by
// locale rules.
func (p *Postgres) CreateDatabase(ctx context.Context) (string, error) {
	return p.createDatabase(ctx, "test")
}

// createDatabase creates a database named prefix_N.
func (p *Postgres) createDatabase(ctx context.Context, prefix string) (string, error) {
	name := fmt.Sprintf("%s_%d", prefix, p.dbSeq.Add(1))
	conn, err := pgx.Connect(ctx, p.URL)
	if err != nil {
		return "", fmt.Errorf("connecting to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+
		" TEMPLATE template0 ENCODING 'UTF8' LOCALE_PROVIDER icu ICU_LOCALE 'en-US' LOCALE 'C'"); err != nil {
		return "", fmt.Errorf("creating database %s: %w", name, err)
	}
	u, err := url.Parse(p.URL)
	if err != nil {
		return "", fmt.Errorf("parsing connection string: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}

// CreateDatabaseWithoutConnect creates a database (as CreateDatabase does,
// named noconnect_N so it cannot clash with a package's own test_N
// databases) and a login role that lacks the CONNECT privilege on it, and
// returns a connection string for that role and database. Connecting with
// it fails with SQLSTATE 42501 (insufficient privilege).
func (p *Postgres) CreateDatabaseWithoutConnect(ctx context.Context) (string, error) {
	dbURL, err := p.createDatabase(ctx, "noconnect")
	if err != nil {
		return "", err
	}
	u, err := url.Parse(dbURL)
	if err != nil {
		return "", fmt.Errorf("parsing connection string: %w", err)
	}
	dbName := strings.TrimPrefix(u.Path, "/")
	role := dbName + "_role"
	const password = "s3cret-pw" // distinct from the names, for leak checks

	conn, err := pgx.Connect(ctx, p.URL)
	if err != nil {
		return "", fmt.Errorf("connecting to postgres: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	for _, stmt := range []string{
		"CREATE ROLE " + pgx.Identifier{role}.Sanitize() + " LOGIN PASSWORD '" + password + "'",
		"REVOKE CONNECT ON DATABASE " + pgx.Identifier{dbName}.Sanitize() + " FROM PUBLIC",
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return "", fmt.Errorf("preparing role %s without CONNECT: %w", role, err)
		}
	}
	u.User = url.UserPassword(role, password)
	return u.String(), nil
}
