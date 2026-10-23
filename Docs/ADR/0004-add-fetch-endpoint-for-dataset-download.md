# 0004. Add a POST /fetch endpoint for dataset download

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#10](https://github.com/hrhelena/elisa-task/issues/10), [#12](https://github.com/hrhelena/elisa-task/issues/12), [0006](0006-cloud-native-deployment-with-kustomize-and-kind.md), [0007](0007-tolerate-malformed-dataset-entries.md)

## Summary

Downloading and extracting the dataset is a separate, idempotent `POST /fetch` endpoint. `POST /start` only ingests
from the configured data directory.

## Context

- The task describes `POST /start` as "start the ingestion" and `/status` as tracking ingestion. It does not say how
  the dataset reaches the service.
- The dataset is `https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz` (a few hundred MB compressed, about 2.3 GB extracted, 517,401 files).
- Locally the dataset is already extracted in `Dataset/maildir` (gitignored). In a cluster, the pod starts with an empty volume.

## Decision

- `POST /fetch` downloads the tarball (URL configurable with `DATASET_URL`) and extracts it into `DATA_DIR`, so the
  maildir ends up at `DATA_DIR/maildir`.
- It is idempotent: if `DATA_DIR/maildir` is already present and complete, it does nothing and reports `ready`.
  Completion is marked by a marker file written after a successful extraction (`DATA_DIR/.fetch-complete`); a directory
  present without the marker (for example, the local pre-extracted copy) is also treated as ready.
- It runs asynchronously: the request returns `202 Accepted` and the progress (state, bytes downloaded) is reported
  in the response and in `GET /status` under `dataset`.
- Extraction streams the archive (no temp copy of the 2.3 GB tree), rejects path traversal and symlinks, and extracts
  into a temporary directory that is renamed into place on success.
- `POST /start` never downloads. If the maildir is missing it returns an error that tells the caller to run `/fetch`.

## Rationale

| Option | Pros | Cons |
|---|---|---|
| Separate `/fetch` (chosen) | `/start` keeps the spec's meaning; download and ingest fail and retry independently | One more endpoint |
| `/start` downloads when missing | One call | Mixes two long operations; a slow download looks like a slow ingestion |
| Out of band only (init container, manual) | No code | Not self-contained; harder to demo |

## Consequences

- Positive: the service is self-contained in a fresh cluster; local runs skip the download.
- Negative: the data directory must be writable and big enough (about 3 GB) in the cluster.
- Follow-ups: fetch ticket; the k8s manifests provide a PVC for `DATA_DIR`. The dataset volume topology (one replica, RWO) is
  decided in [0014](0014-kubernetes-deployment-topology.md).
