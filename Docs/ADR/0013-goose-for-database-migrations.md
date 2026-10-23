# 0013. Use goose v3 for database migrations

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#4](https://github.com/hrhelena/elisa-task/issues/4), [#5](https://github.com/hrhelena/elisa-task/issues/5), [0001](0001-use-postgres-as-the-datastore.md)

## Summary

Schema migrations use `github.com/pressly/goose/v3`, with SQL files embedded in the binary and applied at startup.

## Context

The Postgres schema ([0001](0001-use-postgres-as-the-datastore.md)) needs versioned migrations that run in docker compose,
kind and the cloud without a separate tool or job, and that do not race when more than one pod starts.

## Decision

- SQL migrations live in `internal/store/migrations/NNNNN_name.sql` and are embedded with `//go:embed`.
- They run at startup through the goose `Provider` API with the Postgres session locker, before the service reports ready.
  A failed migration exits the process with status 1.
- No separate migration CLI or Kubernetes Job.

## Rationale

| Option | Pros | Cons |
|---|---|---|
| goose v3 (chosen) | Embedded `fs.FS`, clean Go API, built-in Postgres session locker | Smaller ecosystem than golang-migrate |
| golang-migrate | Widely used, many sources | Locking and embedding need more glue; API is less ergonomic for in-process use |
| Hand-rolled | No dependency | Reinvents version tracking and locking |

## Consequences

- Positive: a single binary migrates itself; concurrent starts are safe.
- Negative: schema changes ship with the app version (no independent rollback of schema).
- Follow-ups: #4 adds goose and the first migration; #5 adds the second.
