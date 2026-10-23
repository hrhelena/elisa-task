# 0010. Ingestion state in Postgres with advisory-lock single-flight

- **Status:** Proposed
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#5](https://github.com/hrhelena/elisa-task/issues/5), [#7](https://github.com/hrhelena/elisa-task/issues/7), [#8](https://github.com/hrhelena/elisa-task/issues/8), [0001](0001-use-postgres-as-the-datastore.md), [0006](0006-cloud-native-deployment-with-kustomize-and-kind.md)

## Summary

Ingestion run state is stored in Postgres (an `ingestion_runs` table), so `GET /status` is consistent on every replica.
At most one ingestion runs at a time across the cluster, enforced with a Postgres session-level advisory lock.
The run executes asynchronously inside the API process that accepted `POST /start`.

## Context

- With stateless app pods ([0001](0001-use-postgres-as-the-datastore.md), [0006](0006-cloud-native-deployment-with-kustomize-and-kind.md)),
  `POST /start` and `GET /status` can hit different replicas. In-memory state would give inconsistent answers.
- Two concurrent ingestions of the same files would race on the same rows and double the work.
- A pod can die mid-run (rollout, OOM, node loss).

## Decision

- Table `ingestion_runs(id, state, started_at, finished_at, files_seen, messages_processed, messages_skipped,
  invalid_sender, invalid_date, error, updated_at)` with `state IN ('running','completed','failed')`.
  "No run yet" is reported as `idle`.
- `POST /start` acquires `pg_try_advisory_lock(<fixed key>)` on a dedicated connection taken from the pool and held for
  the whole run. If the lock is taken, the run is already in progress somewhere: respond `409 Conflict`.
- On success it inserts a `running` row, returns `202 Accepted` and runs the ingestion in a goroutine owned by the
  ingestion service (cancelled on shutdown). Counters are flushed to the row at least every 5 seconds and at the end.
- Stale-run recovery: if a `running` row exists while the lock is free (its owner died), it is marked `failed` with
  error `interrupted` before a new run starts, and `GET /status` reports it as `failed`.
- Graceful shutdown cancels the run, flushes counters and marks it `failed` with error `interrupted: shutdown`.
- Rows are inserted with `ON CONFLICT (source_path) DO NOTHING`, so a re-run never double-counts.

## Rationale

| Option | Pros | Cons |
|---|---|---|
| State in DB + advisory lock, in-process goroutine (chosen) | Consistent across replicas; no extra infra; crash detection via lock ownership | The API pod does heavy work; it needs the dataset volume mounted |
| In-memory state | Simplest | Wrong with more than one replica; lost on restart |
| Separate Kubernetes Job for ingestion | Isolates heavy work; natural retries | `POST /start` must create a Job (RBAC to the k8s API); harder to run locally without k8s |

This ADR is **Proposed**: it follows from the Postgres and cloud-native decisions, but the in-process vs Kubernetes Job
choice and the re-run semantics after completion need the user's confirmation.

## Consequences

- Positive: `/status` is correct on any replica; double-start is impossible cluster-wide; crashed runs are detected.
- Negative: the replica running the ingestion needs the dataset volume; a lock connection is held for the run.
- Follow-ups: ingestion engine ticket, `/start` and `/status` ticket.
