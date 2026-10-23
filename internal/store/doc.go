// Package store implements persistence in Postgres, including the embedded
// schema migrations.
//
// Layout: store.go holds the backend-independent types and the Store
// contract, runs.go the ingestion run types and roles, postgres.go and
// postgres_runs.go the pgx implementation, migrate.go the embedded goose
// migrations (migrations/NNNNN_name.sql), and sanitize.go the last-line text
// sanitizing applied before anything reaches Postgres.
package store
