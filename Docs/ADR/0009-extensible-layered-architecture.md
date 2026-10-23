# 0009. Extensible layered architecture

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#3](https://github.com/hrhelena/elisa-task/issues/3), [#7](https://github.com/hrhelena/elisa-task/issues/7), [#14](https://github.com/hrhelena/elisa-task/issues/14), [0001](0001-use-postgres-as-the-datastore.md), [0002](0002-no-full-text-indexing-store-headers-only.md), [0005](0005-no-authentication-or-authorization.md)

## Summary

The service is split into small layers joined by narrow interfaces (HTTP, ingestion, parsing, storage, fetch), so that
other data sources, full-text search, authentication and richer analytics can be added without rewrites. The README
lists these extensions.

## Context

The user asked for a design that stays flexible and for a list of possible extensions in the README.

## Decision

Package layout (see `.claude/skills/go-conventions/SKILL.md`):

- `cmd/enron-service`: configuration, wiring, lifecycle. The only place that knows concrete types.
- `internal/httpapi`: routing, handlers and the middleware chain. Depends on small interfaces (for example
  `Ingester`, `StatsReader`, `Fetcher`), not on Postgres.
- `internal/ingest`: walks a source and pipelines messages to the store. The source is an interface
  (for example `Source` yielding `(relPath, io.ReadCloser)`), with the maildir walker as the first implementation.
- `internal/mailparse` (or a parser inside `internal/ingest`): turns raw bytes into a `Message` struct; no I/O.
- `internal/store`: the `Store` interface implementation for Postgres plus embedded migrations.
- `internal/fetch`: download and extraction.

Rules: interfaces are defined by the consumer and kept small; no package-level mutable globals; `context.Context` flows
through every blocking call.

Extensions to list in the README: full-text search (Postgres `tsvector` or OpenSearch), content-hash deduplication,
other data sources (mbox, PST, IMAP, S3), richer analytics (top recipients, sender/recipient graph, volume over time,
per-mailbox stats), authn/authz middleware, ingestion as a Kubernetes Job or queue-driven workers, incremental ingestion.

## Rationale

Narrow interfaces keep each ticket independently testable (fakes in unit tests, real Postgres in integration tests)
and let coding agents work on packages in parallel.

## Consequences

- Positive: parallel development, easy unit testing, low cost to extend.
- Negative: a few extra interfaces for a small service.
- Follow-ups: README ticket lists the extensions.
