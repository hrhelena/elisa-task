# 0001. Use Postgres as the datastore

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#4](https://github.com/hrhelena/elisa-task/issues/4), [#5](https://github.com/hrhelena/elisa-task/issues/5), [0002](0002-no-full-text-indexing-store-headers-only.md), [0006](0006-cloud-native-deployment-with-kustomize-and-kind.md), [0010](0010-ingestion-state-in-postgres-with-advisory-lock.md)

## Summary

The service stores ingested message metadata and ingestion state in PostgreSQL, accessed through `github.com/jackc/pgx/v5`
(`pgxpool`) behind a Go `Store` interface. Bulk loading uses `CopyFrom` or batched inserts.

## Context

- The workload is about 517,400 small rows (one per maildir file, see [0003](0003-count-every-maildir-file-as-one-message.md)),
  written by a single bulk writer. Reads are a `COUNT(*)` for `/status` and a `GROUP BY` sender with `LIMIT 10` for `/top-senders`.
- The service must be cloud native: deployable to EKS or GKE, and runnable locally with `kind` and docker compose
  ([0006](0006-cloud-native-deployment-with-kustomize-and-kind.md)).
- `prompt.md` limits the choice to SQLite or Postgres.

## Decision

- Use PostgreSQL (version 16 or later) as the only datastore.
- Use `pgx/v5` with `pgxpool`. Load rows with `pgx.CopyFrom` into a staging table, or with batched multi-row inserts.
- Hide storage behind a small Go `Store` interface (in `internal/store`), defined where it is consumed, so another backend can be added later.
- Locally, Postgres runs in docker compose and in kind. In the cloud, use a managed offering (RDS, Cloud SQL).
- Integration tests run against real Postgres via `testcontainers-go`.

## Rationale

| | SQLite | Postgres |
|---|---|---|
| Ops | Zero ops, embedded, single file | Extra component to run locally (compose/kind) |
| Performance for this size | Fast | Fast enough; `COPY` loads 517k rows in seconds |
| Kubernetes | The file on a ReadWriteOnce PVC pins the service to one stateful pod; no horizontal scaling | App pods are stateless; replicas scale freely |
| Concurrency | Single writer; reads can block during ingestion depending on journal mode | MVCC: reads are consistent while ingestion runs |
| Single-flight ingestion | Only an in-process lock | `pg_advisory_lock` works cluster-wide |
| HA / managed | Weak story on EKS/GKE | RDS / Cloud SQL with backups and failover |
| Growth path | Limited | `tsvector` FTS, partitioning, richer analytics |
| Tests | Trivial | Heavier: needs testcontainers-go and Docker |

SQLite would be enough for the raw numbers, but it conflicts with the cloud-native requirement: a stateful single pod
with no cluster-wide coordination. Postgres costs one extra container locally, which docker compose and kind absorb.

## Consequences

- Positive: stateless app pods, consistent `/status` across replicas, cluster-wide single-flight ingestion, a clear growth path.
- Negative: local development and integration tests need Docker. CI must run testcontainers.
- Follow-ups: schema and migrations ticket (#4); migration tool decided in [0013](0013-goose-for-database-migrations.md).
