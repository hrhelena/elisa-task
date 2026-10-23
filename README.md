# Enron email metrics service

Go service that ingests the Enron maildir dataset into Postgres and exposes metrics over HTTP.
Full documentation lands with #14; architecture decisions are in [`Docs/ADR/`](Docs/ADR/README.md).

## Quick start

Prerequisites: Go (version in `go.mod`), Python 3 and Docker. All other tools are project-local and pinned.

```bash
make tools       # .venv with pinned pre-commit, git pre-commit hook, Go modules
make lint test   # golangci-lint + go vet, unit tests with -race
make build       # bin/enron-service
make help        # all targets
```

Run `make tools` in every checkout and git worktree before committing. The pre-commit hook lives in the
hooks directory that all worktrees share, but it uses the `.venv` of the worktree being committed to,
and refuses the commit (with a hint) if that `.venv` is missing.

Python dev tools are locked with hashes in `requirements-dev.txt`. To change them, edit `requirements-dev.in`
and run `make py-lock`.

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs on every pull request and on pushes to `main`, using the same
Makefile targets: `lint` (all pre-commit hooks, including the `go mod tidy` and formatting checks, then `make lint`),
`test` (`make test` with `-race`; `coverage.out` is uploaded as an artifact) and `itest` (`make itest` with
testcontainers on the runner's Docker). A new push to a branch or PR cancels that ref's older run;
on `main` every commit gets its own full run, which is never cancelled.

## Configuration

Environment variables only; every one has a default that runs locally. Invalid values make the service log an
error naming the variable and exit with status 1.

| Variable | Default | Meaning |
|---|---|---|
| `HTTP_ADDR` | `:8080` | HTTP listen address, `[host]:port` with a numeric port |
| `DATABASE_URL` | `postgres://enron:enron@localhost:5432/enron?sslmode=disable` | Postgres connection string; pgxpool options such as `pool_max_conns` may be added as query parameters. At startup the service connects and applies the embedded migrations before it listens; if that fails within 1 minute, it exits with status 1 |
| `DATA_DIR` | `./Dataset` | Dataset root; the maildir is expected at `$DATA_DIR/maildir` |
| `DATASET_URL` | `https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz` | Archive downloaded by `POST /fetch` (http or https) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` (JSON logs on stdout) |
| `SHUTDOWN_TIMEOUT` | `20s` | Upper bound for draining in-flight requests (resources are then closed within a separate fixed 5s budget) |
| `SHUTDOWN_DRAIN_DELAY` | `0s` | After `SIGTERM`, keep serving while `/readyz` reports `503` for this long before the listener closes (set a few seconds behind a load balancer) |
| `INGEST_WORKERS` | number of CPUs (at most 256) | Parallel ingestion workers (1 to 256) |
| `INGEST_BATCH_SIZE` | `2000` | Rows per insert batch (1 to 50000) |
| `INGEST_PROGRESS_INTERVAL` | `5s` | How often ingestion progress is persisted |

## Operational endpoints

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness: `200 {"status":"ok"}` while the process serves HTTP |
| `GET /readyz` | Readiness: `200 {"status":"ready"}`; `503` with the failing `checks` when a dependency check fails (2s timeout each; currently `postgres`, a ping), or `{"status":"shutting_down"}` during shutdown |
| `GET /metrics` | Prometheus metrics: `enron_http_requests_total`, `enron_http_request_duration_seconds`, `enron_http_panics_total`, the `enron_fetch_*` metrics below, Go runtime and process metrics |

Every response carries an `X-Request-ID` header (a valid incoming one is reused). Errors from every endpoint use
`{"error":{"code":"<snake_case>","message":"<text>"}}`; unknown routes return `404 not_found`, and a wrong method
returns `405 method_not_allowed` with an `Allow` header. On `SIGINT`/`SIGTERM` the service drains in-flight
requests, then closes its resources, and exits with status 0, or with 1 if draining exceeds `SHUTDOWN_TIMEOUT`.
A second signal exits immediately. The worst-case shutdown takes `SHUTDOWN_DRAIN_DELAY` + `SHUTDOWN_TIMEOUT` + 5s,
which must fit in the pod's `terminationGracePeriodSeconds`.

## Getting the dataset: `POST /fetch`

`POST /fetch` (ADR 0004) downloads `DATASET_URL` and extracts it to `$DATA_DIR/maildir` in the background.
If `$DATA_DIR/maildir` already exists (for example the local `Dataset/maildir`), nothing is downloaded.

```bash
curl -si -X POST localhost:8080/fetch
# 202 Accepted, Location: /status
# {"state":"downloading","path":"Dataset/maildir","bytes_downloaded":0,"bytes_total":-1,"files_extracted":0,
#  "error":null,"started_at":"2026-10-23T09:00:00Z","finished_at":null}
```

| Situation | Response |
|---|---|
| Dataset present | `200` with `"state":"ready"` |
| Fetch started, already running, or retried after a failure | `202`, `Location: /status`, current progress |
| `DATA_DIR` not writable | `500`, code `data_dir_not_writable` |
| Service shutting down | `503`, code `unavailable` |

States: `absent`, `downloading` (download and streamed extraction), `extracting` (archive fully received; verifying
and moving into place), `ready`, `failed` (with `error`, for example `download failed: HTTP 404`). Calling
`POST /fetch` while a fetch runs never starts a second download; calling it after a failure retries from scratch.

How it works: the archive is streamed through gzip and tar into `$DATA_DIR/.fetch-tmp-*` (never buffered in memory),
and renamed to `$DATA_DIR/maildir` only after the whole archive, including the gzip checksum, has been read. Then
`$DATA_DIR/.fetch-complete` records the URL, SHA-256, size and file count. Entries that are absolute, escape the
target (`..`), are links or devices, or lie outside `maildir/` are skipped and logged. A fetch fails if no data
arrives for 60s, if less than 4 GiB is free in `DATA_DIR`, or if the archive extracts to more than 8 GiB. On
failure or shutdown the temporary directory is removed; leftovers from a crash are removed at startup.

Metrics: `enron_fetch_bytes_total`, `enron_fetch_state{state}`, `enron_fetch_duration_seconds{result}`,
`enron_fetch_entries_skipped_total{reason}`.
