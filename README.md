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

## Container image and docker compose

```bash
make docker          # image enron-service:dev (override: make docker IMAGE=... VERSION=...)
make compose-up      # build, then start Postgres 16 and the service in the background
curl localhost:8080/readyz
make compose-down    # stop (the database volume is kept); make compose-clean also deletes it
make docker-smoke    # build, then check the image and the compose stack on ports 18080/15432
```

The image is built in two stages: a static binary (`CGO_ENABLED=0`, `-trimpath -s -w`, version from `VERSION`) on
`gcr.io/distroless/static-debian12:nonroot`. It has no shell, runs as `65532:65532`, listens on 8080 and is about
24 MB. Base images are pinned by digest in `Dockerfile` and `compose.yaml`.

`compose.yaml` starts the app only after the Postgres healthcheck (`pg_isready` over TCP) passes, and restarts it
if it exits with an error (for example, if it cannot reach the database at startup). It sets `DATABASE_URL` to the
compose Postgres and mounts `./Dataset` read-write at `/data` (`DATA_DIR`), so `POST /fetch` can fill an empty
directory. `make compose-up` creates the directory as you before it starts compose. Use it rather than a bare
`docker compose up`: on a rootful Linux daemon, compose would create a missing directory owned by root, and the
app could not write to it. The app has no container healthcheck (no shell or curl in the image), so poll `/readyz`.
Compose variables:

| Variable | Default | Meaning |
|---|---|---|
| `DATASET_DIR` | `./Dataset` | Host directory mounted at `/data` (e.g. the main checkout's `Dataset/` from a git worktree) |
| `APP_PORT` | `8080` | Host port for the service |
| `POSTGRES_PORT` | `5432` | Host port for Postgres |
| `APP_USER` | `65532:65532` | Container user. On Linux, use `"$(id -u):$(id -g)"` so the app can write to the bind mount; this works when you own the dataset directory, which `make compose-up` ensures for a new one |
| `LOG_LEVEL` | `info` | Passed to the service |

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs on every pull request and on pushes to `main`, using the same
Makefile targets: `lint` (all pre-commit hooks, including the `go mod tidy` and formatting checks, then `make lint`),
`test` (`make test` with `-race`; `coverage.out` is uploaded as an artifact), `itest` (`make itest` with
testcontainers on the runner's Docker) and `docker` (builds the image with the GitHub Actions build cache, never
pushes it, then runs `scripts/docker-smoke.sh`). A new push to a branch or PR cancels that ref's older run;
on `main` every commit gets its own full run, which is never cancelled.

## Configuration

Environment variables only; every one has a default that runs locally. Invalid values make the service log an
error naming the variable and exit with status 1.

| Variable | Default | Meaning |
|---|---|---|
| `HTTP_ADDR` | `:8080` | HTTP listen address, `[host]:port` with a numeric port |
| `DATABASE_URL` | `postgres://enron:enron@localhost:5432/enron?sslmode=disable` | Postgres connection string; pgxpool options such as `pool_max_conns` may be added as query parameters; `pool_max_conns` must be at least 2. A running ingestion holds one extra connection outside the pool for its cluster-wide advisory lock, so size the server's `max_connections` for `pool_max_conns + 1` per replica. The URL must point to Postgres directly or to a session-mode pooler: transaction-mode pgbouncer (`pool_mode=transaction`) is not supported, because session advisory locks need a session. The lock is held only while its connection lives: idle-connection killers (`idle_session_timeout` on the server, idle timeouts on a proxy or load balancer) silently drop the lock (the run's next progress write on that connection then fails), so set them well above the progress flush interval. See [Startup](#startup) |
| `DB_STARTUP_TIMEOUT` | `60s` | Budget for opening the database at startup: connection retries, the migration lock wait and the migrations (positive duration) |
| `DATA_DIR` | `./Dataset` | Dataset root; the maildir is expected at `$DATA_DIR/maildir` |
| `DATASET_URL` | `https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz` | Archive downloaded by `POST /fetch` (http or https) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` (JSON logs on stdout) |
| `SHUTDOWN_TIMEOUT` | `20s` | Upper bound for draining in-flight requests (resources are then closed within a separate fixed 5s budget) |
| `SHUTDOWN_DRAIN_DELAY` | `0s` | After `SIGTERM`, keep serving while `/readyz` reports `503` for this long before the listener closes (set a few seconds behind a load balancer) |
| `INGEST_WORKERS` | number of CPUs (at most 256) | Parallel ingestion workers (1 to 256) |
| `INGEST_BATCH_SIZE` | `2000` | Rows per insert batch (1 to 50000) |
| `INGEST_PROGRESS_INTERVAL` | `5s` | How often ingestion progress is persisted |

## API

| Endpoint | Purpose |
|---|---|
| `GET /top-senders[?limit=N]` | Senders ranked by message count (every maildir file counts once, ADR 0003), ties by address in byte order; messages without a valid sender are excluded (ADR 0012). `limit` is 1 to 100, default 10. `200 {"senders":[{"email":"kay.mann@enron.com","count":16735}]}`, `{"senders":[]}` when nothing is stored; `400 invalid_limit` for any other `limit` (non-numeric, out of range, empty or repeated); `503 unavailable` when the database query fails or exceeds 5s |

```sh
curl -s 'localhost:8080/top-senders?limit=3'
```

## Startup

The service connects to Postgres and applies the embedded migrations before it listens. `DB_STARTUP_TIMEOUT`
(default `60s`) bounds all of it:

- While the database is not reachable yet (connection refused, DNS failure, no answer within 5s or within a longer
  `connect_timeout` from `DATABASE_URL`, or the server reports that it is starting up, shutting down or out of
  connections), the service retries with capped exponential backoff and jitter (250ms, 500ms, 1s, ... up to 5s),
  logging each retry at `WARN` as `database not ready, retrying` with the attempt number and the error. The password
  is never logged.
- A server that answers but rejects the configuration is not retried: a wrong password or other authorization
  failure (SQLSTATE class `28`), a missing database (`3D000`), or a TLS failure (for example `sslmode=require`
  against a server without TLS, or a certificate that does not verify). When one connect makes several attempts
  (several hosts, or TLS then plaintext with `sslmode` unset, `prefer` or `allow`), it counts as permanent only if
  every attempt was rejected this way; a refused TLS attempt followed by "starting up" is retried.
- Waiting for the migration lock held by another replica polls every second until the same budget ends.
- Migrations run inside the budget too. Each one runs in a transaction, so a migration cut off by the deadline or a
  signal rolls back completely. A future `-- +goose NO TRANSACTION` migration (such as `CREATE INDEX CONCURRENTLY`)
  could be cut off halfway, so it must be written to be safe to run again.
- When the budget runs out, or on a permanent error (a malformed `DATABASE_URL`, one of the rejections above, a
  failed migration), it logs `opening database failed` and exits with status 1; the orchestrator then restarts it.
- `SIGINT`/`SIGTERM` during startup stops the retries and exits with status 0.

## Operational endpoints

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness: `200 {"status":"ok"}` while the process serves HTTP |
| `GET /readyz` | Readiness: `200 {"status":"ready"}`; `503` with the failing `checks` when a dependency check fails (2s timeout each; currently `postgres`, a ping), or `{"status":"shutting_down"}` during shutdown |
| `GET /metrics` | Prometheus metrics: `enron_http_requests_total`, `enron_http_request_duration_seconds`, `enron_http_panics_total`, the `enron_fetch_*` and `enron_ingest_*` metrics below, Go runtime and process metrics |

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
| `$DATA_DIR/maildir` exists but is not a directory | `409`, code `maildir_not_directory` (nothing is downloaded) |
| Service shutting down | `503`, code `unavailable` |

States: `absent`, `downloading` (download and streamed extraction), `extracting` (archive fully received; verifying
and moving into place), `ready`, `failed` (with `error`, for example `download failed: HTTP 404`). Calling
`POST /fetch` while a fetch runs never starts a second download; calling it after a failure retries from scratch.

How it works: the archive is streamed through gzip and tar into `$DATA_DIR/.fetch-tmp-*` (never buffered in memory),
and renamed to `$DATA_DIR/maildir` only after the whole archive, including the gzip checksum, has been read. Then
`$DATA_DIR/.fetch-complete` records the URL, SHA-256, size and file count. Entries that are absolute, escape the
target (`..`), are links or devices, or lie outside `maildir/` are skipped and logged. Before downloading, a fetch
fails if `DATA_DIR` has less than 4 GiB free, or fewer than 530,000 free inodes on filesystems that report them
(the dataset has about 521k files and directories, more than a small ext4 volume may hold). It also fails if no
data arrives for 60s, or if the archive extracts to more than 8 GiB. On failure or shutdown the temporary
directory is removed; leftovers from a crash are removed at startup.

Metrics: `enron_fetch_bytes_total`, `enron_fetch_state{state}`, `enron_fetch_duration_seconds{result}`,
`enron_fetch_entries_skipped_total{reason}`.

## Ingestion engine

The engine (`internal/ingest`) walks `$DATA_DIR/maildir` in lexical order and parses files on `INGEST_WORKERS`
goroutines. One writer stores the messages in batches of `INGEST_BATCH_SIZE` rows, or after 1s. Every file is one
message (ADR 0003); the row key is its relative path, so a re-run inserts only files not stored yet (ADR 0011).
At most one run is active in the cluster (an advisory lock, ADR 0010). Counters are written to the run row every
`INGEST_PROGRESS_INTERVAL` and at the end. Starting runs over HTTP (`POST /start`, `GET /status`) comes with #8.

Bad entries never fail a run (ADR 0007). Skips are counted per reason: `empty_file`, `not_an_email`,
`header_too_large`, `read_error` (including an unreadable directory, counted once), `not_regular_file` (symlinks,
devices), `outside_mailbox` (a file at the maildir root), `parse_panic` and `parse_error`. Field errors such as
`invalid_sender` and `invalid_date` are counted once per message, and the message is still stored. A run fails
only when the process shuts down (`interrupted: shutdown`), when the ingest lock is lost (`lock_lost`, for example
if its database session is killed), when the dataset root disappears, or when a batch insert still fails after
3 retries with backoff. A run whose process died is reported as failed with `interrupted`.

Measured on the full local dataset (517,401 files, MacBook with 10 cores, Postgres 16 in Docker): about 23 s,
with no skipped files and a peak RSS of about 60 MiB.

Metrics: `enron_ingest_files_total`, `enron_ingest_messages_total`, `enron_ingest_skipped_total{reason}`,
`enron_ingest_field_errors_total{field}`, `enron_ingest_batch_duration_seconds`, `enron_ingest_running`.
