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
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `DATABASE_URL` | `postgres://enron:enron@localhost:5432/enron?sslmode=disable` | Postgres connection string |
| `DATA_DIR` | `./Dataset` | Dataset root; the maildir is expected at `$DATA_DIR/maildir` |
| `DATASET_URL` | `https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz` | Archive downloaded by `POST /fetch` (http or https) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` (JSON logs on stdout) |
| `SHUTDOWN_TIMEOUT` | `20s` | Upper bound for draining in-flight requests and closing resources |
| `SHUTDOWN_DRAIN_DELAY` | `0s` | After `SIGTERM`, keep serving while `/readyz` reports `503` for this long before the listener closes (set a few seconds behind a load balancer) |
| `INGEST_WORKERS` | number of CPUs | Parallel ingestion workers (>= 1) |
| `INGEST_BATCH_SIZE` | `2000` | Rows per insert batch (1 to 50000) |
| `INGEST_PROGRESS_INTERVAL` | `5s` | How often ingestion progress is persisted |

## Operational endpoints

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness: `200 {"status":"ok"}` while the process serves HTTP |
| `GET /readyz` | Readiness: `200 {"status":"ready"}`; `503` with the failing `checks` when a dependency check fails (2s timeout each), or `{"status":"shutting_down"}` during shutdown |
| `GET /metrics` | Prometheus metrics: `enron_http_requests_total`, `enron_http_request_duration_seconds`, `enron_http_panics_total`, Go runtime and process metrics |

Every response carries an `X-Request-ID` header (a valid incoming one is reused). Errors from every endpoint use
`{"error":{"code":"<snake_case>","message":"<text>"}}`; unknown routes return `404 not_found`, and a wrong method
returns `405 method_not_allowed` with an `Allow` header. On `SIGINT`/`SIGTERM` the service drains in-flight
requests and exits with status 0, or with 1 if draining exceeds `SHUTDOWN_TIMEOUT`.
