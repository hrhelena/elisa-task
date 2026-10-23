# 0006. Cloud-native deployment: container image, kustomize, kind and managed Postgres

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#11](https://github.com/hrhelena/elisa-task/issues/11), [#12](https://github.com/hrhelena/elisa-task/issues/12), [#13](https://github.com/hrhelena/elisa-task/issues/13), [0001](0001-use-postgres-as-the-datastore.md), [0004](0004-add-fetch-endpoint-for-dataset-download.md), [0008](0008-project-local-pinned-tooling-and-ci.md)

## Summary

The service ships as a container image deployed with kustomize manifests (a base plus overlays for local kind and for
the cloud). Locally it runs with docker compose (quick dev loop) and kind (Kubernetes parity). In the cloud it targets
EKS or GKE with managed Postgres.

## Context

- The task asks for a cloud-native service that runs locally. The user interprets this as "deployable to EKS or GKE".
- Docker and kind are available on the development machine.

## Decision

- **Image:** multi-stage `Dockerfile`, static Go binary (`CGO_ENABLED=0`) on a distroless non-root base image.
- **Config:** twelve-factor, environment variables only, each with a default that works locally. Secrets (the database
  URL) come from a Kubernetes `Secret`.
- **Operability:** `GET /healthz` (liveness, process only), `GET /readyz` (readiness, checks the DB), structured JSON logs
  with `log/slog` to stdout, graceful shutdown on `SIGTERM` (stop accepting requests, drain, cancel ingestion cleanly),
  Prometheus metrics at `GET /metrics`.
- **Manifests:** `deploy/k8s/base` (Deployment, Service `ClusterIP`, ConfigMap, PVC for `DATA_DIR`, probes, resource
  requests/limits, security context) and overlays `deploy/k8s/overlays/kind` (in-cluster Postgres, locally built image
  loaded with `kind load`) and `deploy/k8s/overlays/cloud` (external managed Postgres via Secret, no in-cluster DB).
- **Postgres:** managed in the cloud (RDS or Cloud SQL). In kind, an in-cluster Postgres; whether this is a plain
  StatefulSet or the CloudNativePG operator is an open question (recommendation: plain StatefulSet).
- **Local:** `docker compose up` runs Postgres plus the service with the local `Dataset/` mounted. `make kind-up deploy`
  creates a kind cluster and deploys the kind overlay.

## Rationale

kustomize is built into `kubectl`, needs no templating language and fits a base/overlay split well. Helm would add
packaging features the project does not need. docker compose gives a faster inner loop than kind; kind proves the
manifests work.

## Consequences

- Positive: the same image and base manifests run locally and in the cloud; only the overlay changes.
- Negative: two local run modes to keep working; CI should at least build the image and validate the manifests
  (`kubectl kustomize`).
- Follow-ups: Dockerfile/compose ticket, k8s/kind ticket, optional kind smoke test in CI.
