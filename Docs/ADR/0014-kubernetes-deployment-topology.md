# 0014. Kubernetes deployment topology: one replica, RWO dataset volume, Postgres StatefulSet in kind

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#10](https://github.com/hrhelena/elisa-task/issues/10), [#12](https://github.com/hrhelena/elisa-task/issues/12), [0004](0004-add-fetch-endpoint-for-dataset-download.md), [0006](0006-cloud-native-deployment-with-kustomize-and-kind.md), [0010](0010-ingestion-state-in-postgres-with-advisory-lock.md)

## Summary

The service runs as a single-replica Deployment with a ReadWriteOnce PVC for the dataset and the `Recreate` strategy.
In kind, Postgres runs as a plain StatefulSet; in the cloud, it is a managed service. RWX storage or ingestion as a
Kubernetes Job are the documented path to more replicas.

## Context

- Ingestion runs inside the API process ([0010](0010-ingestion-state-in-postgres-with-advisory-lock.md)) and reads the
  dataset from `DATA_DIR`, which `/fetch` fills ([0004](0004-add-fetch-endpoint-for-dataset-download.md)).
- A ReadWriteOnce volume can be mounted by one node at a time; RWX storage (EFS, Filestore) is not available in kind
  by default and costs more in the cloud.
- `/fetch` progress is held in memory in the pod that runs it.

## Decision

- **App:** `Deployment` with `replicas: 1`, strategy `Recreate` (the RWO volume cannot be attached to two pods during a
  rolling update), PVC `enron-data` (`ReadWriteOnce`, 5Gi) mounted at `/data`.
- **Fetch state:** in memory per pod. After a restart, a completed dataset is detected from the filesystem; an
  interrupted fetch is restarted with a new `POST /fetch`.
- **Postgres in kind:** a plain `StatefulSet` (`postgres:16-alpine`, one replica, 2Gi PVC) with a headless Service.
  No operator.
- **Postgres in the cloud:** managed (RDS, Cloud SQL); the cloud overlay contains no database resources.
- **Scaling path (documented, not built):** (a) RWX storage for `DATA_DIR` so several API replicas share the dataset;
  or (b) fetch and ingestion as a Kubernetes Job with stateless API replicas serving `/status` and `/top-senders`.
  Both work because run state is already in Postgres.

## Rationale

| Option | Pros | Cons |
|---|---|---|
| 1 replica + RWO + Recreate (chosen) | Works in kind and every cloud with default storage; simple | Brief downtime on rollout; no read scaling |
| Several replicas + RWX | Read scaling, no downtime | RWX storage class needed; not in kind by default |
| Ingestion as a Job + stateless API | Clean separation | Needs RBAC and Job orchestration from `/start` |
| CloudNativePG in kind | Production-like Postgres | Operator install for a local demo |

## Consequences

- Positive: one manifest set that runs in kind and in the cloud with default storage classes.
- Negative: rollouts cause a short outage; a pod restart loses in-flight fetch progress.
- Follow-ups: #12 implements the manifests; #14 documents the scaling path.
