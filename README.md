# Enron email metrics service

A Go service that ingests the [Enron email dataset](https://www.cs.cmu.edu/~enron/) (maildir format, 150 mailboxes,
517,401 files) into Postgres and serves metrics over HTTP: `POST /start` starts an ingestion, `GET /status` reports
whether it has finished and how many messages it processed, and `GET /top-senders` returns the 10 top senders.
`POST /fetch` downloads the dataset when it is not already on disk. The service runs locally with docker compose or
kind, and in the cloud (EKS, GKE) with managed Postgres. The requirements are in
[`Docs/task-description.txt`](Docs/task-description.txt), and the decisions behind the design are in the
[architecture decision records](Docs/ADR/README.md) (ADRs).

**Contents:** [Architecture](#architecture) · [Quick start](#quick-start-docker-compose) ·
[API reference](#api-reference) · [Configuration](#configuration) ·
[Data semantics and robustness](#data-semantics-and-robustness) · [Operations](#operations) ·
[Kubernetes](#running-on-kubernetes) · [Development](#development) · [Design decisions](#design-decisions) ·
[Known limitations](#known-limitations) · [Possible extensions](#possible-extensions)

## Architecture

```mermaid
flowchart LR
    client([HTTP client]) --> api
    subgraph service [enron-service, one process]
        api["httpapi<br/>routing, middleware chain,<br/>handlers, probes, metrics"]
        fetch["fetch<br/>download and extract<br/>the dataset archive"]
        ingest["ingest<br/>Source walker, worker pool,<br/>batch writer, counters"]
        mailparse["mailparse<br/>headers to Message,<br/>sender normalization"]
        store["store<br/>Store interface, Postgres,<br/>embedded migrations"]
        api -- "POST /fetch" --> fetch
        api -- "POST /start, GET /status" --> ingest
        api -- "GET /top-senders" --> store
        ingest --> mailparse
        ingest --> store
    end
    fetch -- "writes" --> data[("DATA_DIR/maildir")]
    ingest -- "reads" --> data
    cmu[(CMU archive)] -. "DATASET_URL" .-> fetch
    store --> pg[(Postgres 16)]
```

| Package | Role |
|---|---|
| `cmd/enron-service` | Configuration, wiring, startup and graceful shutdown. The only place that knows the concrete types. |
| `internal/config` | Environment variables, each with a default that runs locally. |
| `internal/httpapi` | `net/http` routing, handlers and the middleware chain (request ID, route resolution, panic recovery, access log, metrics, plus empty slots for authentication and authorization). It depends on small interfaces (`Ingester`, `SenderStats`, `Fetcher`), not on Postgres. |
| `internal/fetch` | Streams the dataset archive through gzip and tar into `DATA_DIR`. |
| `internal/ingest` | Walks a `Source` (the maildir walker is the only implementation), parses files on a worker pool, writes batches, persists counters, holds the cluster-wide ingest lock. |
| `internal/mailparse` | Turns raw bytes into a `Message` (headers only, no I/O) and normalizes addresses. |
| `internal/store` | The Postgres store (`pgx/v5`, `pgxpool`), run state, the advisory lock, and the goose migrations embedded in the binary. |

Layering and extension points: [ADR 0009](Docs/ADR/0009-extensible-layered-architecture.md). Storage:
[0001](Docs/ADR/0001-use-postgres-as-the-datastore.md) and [0002](Docs/ADR/0002-no-full-text-indexing-store-headers-only.md).
Run state and single-flight ingestion: [0010](Docs/ADR/0010-ingestion-state-in-postgres-with-advisory-lock.md). API
contract: [0011](Docs/ADR/0011-http-api-contract-and-start-semantics.md).

Run state lives in Postgres, so every replica reports the same `/status`. An ingestion runs in a goroutine of the
process that accepted `POST /start`. A Postgres session advisory lock ensures that at most one run is active across
the cluster.

## Quick start (docker compose)

Prerequisites: Docker with the compose plugin, `curl`, and `make`. To build and test outside containers, you also
need Go (the version in `go.mod`) and Python 3. Every other tool is project-local and pinned (see
[Development](#development)).

### 1. Start Postgres and the service

`make compose-up` builds the image and starts Postgres 16 and the service in the background. It mounts `./Dataset`
at `/data` in the container. If `Dataset/maildir` already holds the extracted dataset, the service uses it as it is.
If it does not, `POST /fetch` downloads the dataset into it.

```bash
make compose-up
curl -s localhost:8080/readyz          # {"status":"ready"} once the database is migrated
```

The image has no shell, so the container has no healthcheck: poll `/readyz` until it answers `200`. To use other
host ports (for example when 8080 is taken), or a dataset directory elsewhere (for example the main checkout's
`Dataset/` from a git worktree), set the compose variables:

```bash
APP_PORT=18091 POSTGRES_PORT=15491 make compose-up DATASET_DIR=/path/to/Dataset
```

| Compose variable | Default | Meaning |
|---|---|---|
| `DATASET_DIR` | `./Dataset` | Host directory mounted read-write at `/data` (`DATA_DIR`). Pass it to `make` (`make compose-up DATASET_DIR=...`), so that `make` creates the directory before compose starts. |
| `APP_PORT` | `8080` | Host port of the service |
| `POSTGRES_PORT` | `5432` | Host port of Postgres |
| `APP_USER` | `65532:65532` | Container user. On Linux, use `"$(id -u):$(id -g)"` so the app can write to the bind mount. This works when you own the dataset directory, which `make compose-up` ensures for a new one. Docker Desktop maps ownership, so the default works there. |
| `LOG_LEVEL` | `info` | Passed to the service |

Use `make compose-up` rather than a bare `docker compose up`. On a rootful Linux Docker daemon, compose would create a
missing dataset directory owned by root, and the app could not write into it. `compose.yaml` starts the app only
after the Postgres healthcheck (`pg_isready` over TCP) passes, and restarts it if it exits with an error (for
example, if it cannot reach the database at startup).

### 2. Walkthrough: fetch, start, status, top senders

The examples assume port 8080. Responses are from a real run on the full dataset, with timestamps shortened and
`curl -si` headers trimmed to the relevant ones (every response also carries `X-Content-Type-Options`,
`X-Request-Id`, `Date` and `Content-Length`).

**Get the dataset.** With a local `Dataset/maildir`, `POST /fetch` returns `200` at once with `"state":"ready"`.
Without one, it starts the download (443,254,787 bytes from CMU, then about 2.5 GB on disk) and returns `202`:

```console
$ curl -si -X POST localhost:8080/fetch
HTTP/1.1 202 Accepted
Location: /status
Content-Type: application/json; charset=utf-8

{"state":"downloading","path":"/data/maildir","bytes_downloaded":0,"bytes_total":-1,"files_extracted":0,"error":null,"started_at":"2026-10-23T11:35:41Z","finished_at":null}
```

Its progress is in the `dataset` section of `GET /status`, and the download takes as long as the CMU server needs.
Wait until `dataset.state` is `ready` (until then, `POST /start` answers `409 dataset_missing`):

```console
$ curl -s localhost:8080/status
{"state":"idle",...,"dataset":{"state":"downloading","path":"/data/maildir","bytes_downloaded":42442752,
 "bytes_total":443254787,"files_extracted":45005,"error":null,"started_at":"2026-10-23T11:35:41Z","finished_at":null}}
```

**Start the ingestion.**

```console
$ curl -si -X POST localhost:8080/start
HTTP/1.1 202 Accepted
Location: /status

{"run_id":1,"state":"running","started_at":"2026-10-23T11:29:22.717134Z"}

$ curl -si -X POST localhost:8080/start          # while it runs
HTTP/1.1 409 Conflict

{"error":{"code":"ingestion_running","message":"an ingestion is already running"}}
```

**Poll the status** until `"finished": true`:

```console
$ curl -s localhost:8080/status
{"state":"completed","finished":true,"messages_processed":517401,"total_messages":517401,"messages_skipped":0,
 "files_seen":517401,
 "skip_reasons":{"empty_file":0,"header_too_large":0,"not_an_email":0,"not_regular_file":0,"outside_mailbox":0,
                 "parse_error":0,"parse_panic":0,"read_error":0},
 "field_errors":{"invalid_date":0,"invalid_encoding":59,"invalid_recipient":182,"invalid_sender":3,
                 "missing_date":0,"missing_sender":0},
 "run_id":1,"started_at":"2026-10-23T11:29:22.717134Z","finished_at":"2026-10-23T11:31:47.081079Z",
 "updated_at":"2026-10-23T11:31:47.081079Z","error":null,
 "dataset":{"state":"ready","path":"/data/maildir","bytes_downloaded":0,"bytes_total":-1,"files_extracted":0,
            "error":null,"started_at":null,"finished_at":null}}
```

A full ingestion takes about 23 s natively on a 10-core MacBook. Through docker compose on Docker Desktop for macOS
it took about 2.5 minutes, because reading 517k files through the bind mount is slower.

**Top senders:**

```console
$ curl -s localhost:8080/top-senders
{"senders":[{"email":"kay.mann@enron.com","count":16735},{"email":"vince.kaminski@enron.com","count":14368},
 {"email":"jeff.dasovich@enron.com","count":11411},{"email":"pete.davis@enron.com","count":9149},
 {"email":"chris.germany@enron.com","count":8801},{"email":"sara.shackleton@enron.com","count":8777},
 {"email":"enron.announcements@enron.com","count":8587},{"email":"tana.jones@enron.com","count":8490},
 {"email":"steven.kean@enron.com","count":6759},{"email":"kate.symes@enron.com","count":5438}]}

$ curl -s 'localhost:8080/top-senders?limit=3'   # quote the URL: zsh treats ? as a glob
{"senders":[{"email":"kay.mann@enron.com","count":16735},{"email":"vince.kaminski@enron.com","count":14368},{"email":"jeff.dasovich@enron.com","count":11411}]}
```

### 3. Stop

```bash
make compose-down    # stop; the database volume is kept, so /status and /top-senders survive a restart
make compose-clean   # stop and delete the database volume
```

`make docker-smoke` builds the image and checks it and the compose stack, as its own compose project on ports
18080 and 15432, so it does not touch a running `make compose-up` stack. If those ports are taken, override them
the same way: `APP_PORT=18094 POSTGRES_PORT=15494 make docker-smoke`.

### Running without containers

For a debugger or a quick edit-run loop, run only Postgres in compose and the binary on the host:

```bash
docker compose up -d postgres
make build
./bin/enron-service          # the defaults match compose's Postgres and ./Dataset; see Configuration
```

## API reference

The contract is fixed by [ADR 0011](Docs/ADR/0011-http-api-contract-and-start-semantics.md) (`/start`, `/status`,
`/top-senders`) and [ADR 0004](Docs/ADR/0004-add-fetch-endpoint-for-dataset-download.md) (`/fetch`). There is no
authentication ([ADR 0005](Docs/ADR/0005-no-authentication-or-authorization.md)).

Conventions for every endpoint:

- Responses are JSON (`Content-Type: application/json; charset=utf-8`, `X-Content-Type-Options: nosniff`).
- Errors use one envelope: `{"error":{"code":"<snake_case>","message":"<text>"}}`.
- Unknown paths return `404 not_found`. A known path with the wrong method returns `405 method_not_allowed` with an
  `Allow` header.
- Every response carries `X-Request-ID`. A well-formed incoming one (1 to 64 characters from `A-Z a-z 0-9 -`) is
  reused, and anything else is replaced. The access log line of the request carries the same ID.
- `unavailable` (`503`) means the database failed or timed out, or the service is shutting down. Retry later.

| Endpoint | Success | Errors |
|---|---|---|
| [`POST /fetch`](#post-fetch) | `200` dataset ready, `202` fetch running | `409 maildir_not_directory`, `500 data_dir_not_writable`, `500 internal`, `503 unavailable` |
| [`POST /start`](#post-start) | `202` run started | `409 ingestion_running`, `409 dataset_missing`, `503 unavailable` |
| [`GET /status`](#get-status) | `200` | `503 unavailable` |
| [`GET /top-senders`](#get-top-senders) | `200` | `400 invalid_limit`, `503 unavailable` |
| [`GET /healthz`](#operational-endpoints) | `200` | none |
| [`GET /readyz`](#operational-endpoints) | `200` | `503` (not ready or shutting down) |
| [`GET /metrics`](#operational-endpoints) | `200` (Prometheus text format) | none |

### `POST /fetch`

Downloads `DATASET_URL` and extracts it to `$DATA_DIR/maildir` in the background
([ADR 0004](Docs/ADR/0004-add-fetch-endpoint-for-dataset-download.md)). Any request body is ignored.

| Situation | Response |
|---|---|
| `$DATA_DIR/maildir` is a readable directory, not a symlink (a completed fetch, or a pre-extracted copy such as the local `Dataset/maildir`) | `200` with `"state":"ready"`; nothing is downloaded |
| Fetch started, already running, or retried after a failure | `202`, `Location: /status`, the current progress |
| `$DATA_DIR/maildir` exists but is not a directory: a file, or a symlink to anything (symlinks are never followed) | `409 maildir_not_directory`; nothing is downloaded |
| `$DATA_DIR/maildir` is a directory the service cannot read (for example permissions, or a volume without a matching `fsGroup`) | `409 maildir_unreadable`; nothing is downloaded |
| `DATA_DIR` is not writable | `500 data_dir_not_writable` |
| Any other error while starting the fetch | `500 internal` |
| The service is shutting down | `503 unavailable` |

The body (the same object as `dataset` in `GET /status`):

| Field | Meaning |
|---|---|
| `state` | `absent`, `downloading` (download and streamed extraction), `extracting` (archive fully received, being verified and moved into place), `ready`, or `failed` |
| `path` | Where the maildir is expected (`$DATA_DIR/maildir`) |
| `bytes_downloaded`, `bytes_total` | Download progress; `bytes_total` is `-1` until the server sends a length |
| `files_extracted` | Files written so far |
| `error` | Why the fetch failed, for example `download failed: HTTP 404`, `insufficient free inodes: need 530000, have N`, or `insufficient disk space`. With `state: absent`, why an existing `$DATA_DIR/maildir` cannot be used, for example `maildir path exists and is not a directory: /data/maildir is a symlink, which is not followed`. Otherwise `null` |
| `started_at`, `finished_at` | Times of the current or last fetch in this process, `null` if none |

Calling `POST /fetch` while a fetch runs never starts a second download. Calling it after a failure retries from
scratch. How it works and its safety checks are described under [Getting the dataset](#getting-the-dataset).

### `POST /start`

Starts an ingestion of `$DATA_DIR/maildir` in the background
([ADR 0010](Docs/ADR/0010-ingestion-state-in-postgres-with-advisory-lock.md),
[ADR 0011](Docs/ADR/0011-http-api-contract-and-start-semantics.md)). Any request body is ignored. The request has a
10 s budget.

| Situation | Response |
|---|---|
| No run in progress (never ran, or the last run `completed` or `failed`) | `202`, `Location: /status`, `{"run_id":4,"state":"running","started_at":"2026-10-23T10:00:00Z"}` |
| A run is in progress on any replica | `409 ingestion_running` |
| `$DATA_DIR/maildir` is missing | `409 dataset_missing`, with the message `maildir not found at <path>; call POST /fetch first` |
| `$DATA_DIR/maildir` exists but cannot be used (a symlink, a file, unreadable) | `409 dataset_missing`, with the message `the maildir at <path> cannot be used: <problem>`, for example `it is a symlink, which is not followed; replace it with the directory itself` (`POST /fetch` refuses it too) |
| The database fails or the 10 s budget runs out | `503 unavailable` |
| The service is shutting down | `503 unavailable` |

Starting again after a finished run is an idempotent re-ingest: the dataset is walked again, but files already
stored (same relative path) are not inserted again, so nothing is counted twice. It also resumes a run that crashed.

### `GET /status`

The state of the current or last run, read from Postgres, so every replica gives the same answer. The response has
`Cache-Control: no-store`. It returns `503 unavailable` when the database fails or takes more than 5 s.

| Field | Meaning |
|---|---|
| `state` | `idle` (no run ever started), `running`, `completed` or `failed` |
| `finished` | `true` only when `state` is `completed`. A failed run has not finished its job (`state` and `error` say why). |
| `messages_processed` | Messages parsed and stored by this run: the task's "total number of messages processed" |
| `total_messages` | Rows in the `messages` table. During a run it can be ahead of `messages_processed`: rows from earlier runs count, and the run's counters are written only every `INGEST_PROGRESS_INTERVAL`. The gap is not double counting. |
| `files_seen` | Files the run has handled. `files_seen = messages_processed + messages_skipped` holds in every progress update, also for failed runs. |
| `messages_skipped` | Files that are not messages (see [Malformed input](#malformed-input)) |
| `skip_reasons` | Skips per reason. For a run, every known reason is listed, with 0 when it did not occur. |
| `field_errors` | Field-level problems per kind, counted once per message. The message is still stored and counted. |
| `run_id`, `started_at`, `finished_at`, `updated_at` | The run row; `null` when `idle`. `updated_at` is the last progress write. |
| `error` | Why a run failed, for example `interrupted: shutdown`, `interrupted`, or `lock_lost`; otherwise `null` |
| `dataset` | The `POST /fetch` object for this process |

`skip_reasons` and `field_errors` are never `null`. When `idle` they are `{}`. A run that is still going:

```json
{
  "state": "running", "finished": false,
  "messages_processed": 120345, "total_messages": 120345,
  "messages_skipped": 2, "files_seen": 120347,
  "skip_reasons": {"empty_file": 1, "header_too_large": 0, "not_an_email": 1, "not_regular_file": 0,
                   "outside_mailbox": 0, "parse_error": 0, "parse_panic": 0, "read_error": 0},
  "field_errors": {"invalid_date": 0, "invalid_encoding": 0, "invalid_recipient": 0, "invalid_sender": 1,
                   "missing_date": 0, "missing_sender": 0},
  "run_id": 3, "started_at": "2026-10-23T10:00:00Z", "finished_at": null,
  "updated_at": "2026-10-23T10:01:05Z", "error": null,
  "dataset": {"state": "ready", "path": "/data/maildir", "bytes_downloaded": 0, "bytes_total": -1,
              "files_extracted": 0, "error": null, "started_at": null, "finished_at": null}
}
```

Here 120,347 files were seen. Two were skipped (one empty file and one that is not an email) and are not counted as
messages. 120,345 were stored, one of them without a usable sender: it counts in `messages_processed` but not in
`/top-senders`.

### `GET /top-senders`

`GET /top-senders[?limit=N]` ranks senders by message count, highest first, with ties broken by address in byte
order. Every maildir file counts once ([ADR 0003](Docs/ADR/0003-count-every-maildir-file-as-one-message.md)).
Messages without a valid sender are left out ([ADR 0012](Docs/ADR/0012-lenient-sender-normalization.md)).

| Request | Response |
|---|---|
| no `limit` | `200`, the top 10 |
| `limit` from 1 to 100 | `200`, the top `limit` |
| any other `limit` (non-numeric, signed, out of range, empty, repeated, malformed escape) | `400 invalid_limit` |
| nothing stored yet | `200 {"senders":[]}` |
| the database fails or takes more than 5 s | `503 unavailable` |

```console
$ curl -s 'localhost:8080/top-senders?limit=2'
{"senders":[{"email":"kay.mann@enron.com","count":16735},{"email":"vince.kaminski@enron.com","count":14368}]}
$ curl -s 'localhost:8080/top-senders?limit=0'
{"error":{"code":"invalid_limit","message":"limit must be an integer between 1 and 100"}}
```

### Operational endpoints

| Endpoint | Response |
|---|---|
| `GET /healthz` | Liveness: `200 {"status":"ok"}` whenever the process serves HTTP. It never checks the database. |
| `GET /readyz` | Readiness: `200 {"status":"ready"}`. `503 {"status":"not_ready","checks":{"postgres":"<error>"}}` when a dependency check fails (each check has a 2 s timeout; currently one Postgres ping). `503 {"status":"shutting_down"}` once shutdown has begun. |
| `GET /metrics` | Prometheus metrics (see [Metrics](#metrics)) |

## Configuration

The service reads environment variables only, and every one has a default that runs locally
(`internal/config`). An empty value means the default. Invalid values make the service log an error that names each
bad variable and exit with status 1.

| Variable | Default | Meaning |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Listen address, `[host]:port` with a numeric port |
| `DATABASE_URL` | `postgres://enron:enron@localhost:5432/enron?sslmode=disable` | Postgres connection string (see [Database connection](#database-connection)) |
| `DB_STARTUP_TIMEOUT` | `60s` | Budget for opening the database at startup: connection retries, the migration lock wait and the migrations (positive duration) |
| `DATA_DIR` | `./Dataset` | Dataset root. The maildir is expected at `$DATA_DIR/maildir`. |
| `DATASET_URL` | `https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz` | Archive that `POST /fetch` downloads (an absolute `http` or `https` URL) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` (case-insensitive) |
| `SHUTDOWN_TIMEOUT` | `20s` | Upper bound for draining in-flight requests (positive duration). Resources are then closed within a separate fixed 5 s budget. |
| `SHUTDOWN_DRAIN_DELAY` | `0s` | After `SIGTERM`, keep serving for this long while `/readyz` reports `503`, before the listener closes. Set a few seconds behind a load balancer. |
| `INGEST_WORKERS` | number of CPUs, at most 256 | Parallel parsing workers (1 to 256) |
| `INGEST_BATCH_SIZE` | `2000` | Rows per insert batch (1 to 50,000) |
| `INGEST_PROGRESS_INTERVAL` | `5s` | How often a run's counters are written to Postgres (positive duration) |

Durations use Go syntax (`500ms`, `5s`, `1m`). Docker compose sets `DATABASE_URL` to its Postgres, `DATA_DIR=/data`
and `LOG_LEVEL`. In Kubernetes, the ConfigMap `enron-service-config` sets everything except `DATABASE_URL`, which
comes from the Secret `enron-db`, and `INGEST_PROGRESS_INTERVAL`, which keeps its default. The ConfigMap differs from the defaults in `DATA_DIR=/data`, `INGEST_WORKERS=4`,
`SHUTDOWN_DRAIN_DELAY=5s`.

### Database connection

- pgxpool options such as `pool_max_conns` can be added to `DATABASE_URL` as query parameters. `pool_max_conns`
  must be at least 2.
- A running ingestion holds one extra connection outside the pool for its advisory lock, so size the server's
  `max_connections` for `pool_max_conns + 1` per replica.
- `DATABASE_URL` must point to Postgres directly or to a session-mode pooler. Transaction-mode pgbouncer
  (`pool_mode=transaction`) is not supported, because session advisory locks need a session.
- The lock is held only while its connection lives. Idle-connection killers (`idle_session_timeout` on the server,
  idle timeouts on a proxy or load balancer) silently drop the lock, and the run's next progress write on that
  connection then fails with `lock_lost`. Set them well above `INGEST_PROGRESS_INTERVAL`.
- In the cloud, use `sslmode=require` or `verify-full`.

## Data semantics and robustness

### What counts as a message

Every regular file under `maildir/<mailbox>/` that parses as an email is one message: one row, one count in
`/status`, and one count for its sender in `/top-senders`
([ADR 0003](Docs/ADR/0003-count-every-maildir-file-as-one-message.md)). There is no deduplication. This matters,
because about half of the files are copies of the same email in several folders (`sent`, `_sent_mail`,
`all_documents`, ...) or in several people's mailboxes. On the full dataset, the 517,401 stored files have only
255,512 distinct content hashes. For example, `kay.mann@enron.com` sent 16,735 files but only 4,887 distinct emails.

The relative path (for example `allen-p/_sent_mail/1.`) is the row key, which makes re-ingestion idempotent. Each
row also stores `content_sha256`, a SHA-256 over the normalized From, To and Cc, the decoded Subject, the raw `Date`
header and the raw body bytes
(not `Message-ID` or the export's `X-Folder`/`X-FileName`), so deduplication can be added without a re-ingest (see
[Possible extensions](#possible-extensions)). Bodies are hashed, not stored
([ADR 0002](Docs/ADR/0002-no-full-text-indexing-store-headers-only.md)).

### Sender normalization

The sender is the `From` address, normalized by these rules
([ADR 0012](Docs/ADR/0012-lenient-sender-normalization.md)):

1. Parse strictly with `net/mail.ParseAddress`. If that fails, fall back to the text inside the last `<...>`, or else
   the first whitespace-separated token that contains `@`.
2. Trim spaces, quotes, stray `<`/`>` and trailing `,`, `;` and dots, strip a `mailto:` prefix, and lowercase.
3. The result is valid when it has exactly one `@`, a non-empty local part and a domain containing a `.`, and no
   whitespace, control characters or the specials `()<>[]:;,\`.
4. Broken but usable forms are kept as written, not repaired. `.ward@enron.com` and `w..cantrell@enron.com` are
   their own senders. `X-From` (a display name) is stored but never used for ranking or repair.
5. A message with no usable address is stored with a `NULL` sender. It counts as a message, is left out of
   `/top-senders`, and adds 1 to `field_errors.invalid_sender` (or `missing_sender` when there is no `From` header).

Two rules refine ADR 0012 as written: stray `<` and `>` are trimmed, and an address that contains whitespace or
control characters is invalid. Both keep normalization idempotent.

### Malformed input

A bad file never fails an ingestion ([ADR 0007](Docs/ADR/0007-tolerate-malformed-dataset-entries.md)). There are
two levels.

**File-level problems:** the file is not a message. It is skipped, not stored, and counted in `messages_skipped` and
`skip_reasons`:

| Reason | Meaning |
|---|---|
| `empty_file` | The file is empty |
| `not_an_email` | No parseable header block |
| `header_too_large` | The header block is larger than 1 MiB |
| `read_error` | The file could not be read. An unreadable directory counts once, and its files are not visited. |
| `not_regular_file` | A symlink, device, socket, ... |
| `outside_mailbox` | A regular file at the maildir root, outside any mailbox |
| `parse_panic` | The parser panicked on this file (recovered) |
| `parse_error` | The parser returned an unexpected error |

**Field-level problems:** the file is still a message. It is stored and counted, the bad field is stored as `NULL`
(or sanitized), and `field_errors` counts each kind once per message: `invalid_sender`, `missing_sender`,
`invalid_date`, `missing_date`, `invalid_encoding` (a header that could not be decoded) and `invalid_recipient`.
All text is sanitized before insert: invalid UTF-8 becomes U+FFFD and NUL bytes are removed.

Each skip and field error is logged at `WARN` with the relative path and the reason. Logs are sampled per reason and
run: the first 100, then one in 1,000. They are also counted in the metrics `enron_ingest_skipped_total{reason}`
and `enron_ingest_field_errors_total{field}`.

A run fails only for infrastructure reasons: the process shuts down (`interrupted: shutdown`), the ingest lock is
lost (`lock_lost`, for example when its database session is killed), the dataset root disappears or is not a
directory, or a batch insert still fails after 3 retries with backoff (each attempt times out after 30 s). A run
whose process died (still `running` in the table, but nobody holds the lock) is reported by `/status` as `failed`
with `interrupted`. The next `POST /start` records that in the table before it starts a new run.

### Results on the real dataset

Measured on 2026-10-23 on the full dataset (150 mailboxes, 517,401 files):

| | |
|---|---|
| Messages stored (`messages_processed`, `total_messages`) | 517,401 |
| Files skipped | 0 |
| `field_errors` | `invalid_sender` 3, `invalid_encoding` 59, `invalid_recipient` 182, all others 0 |
| Invalid senders | `pep <performance.>`, `<"linda".@enron@enron.com>`, `<"d@piassick".@enron@enron.com>` |
| Senders accepted only by the lenient fallback | 15,366 (about 13.6k are double-dot addresses such as `w..cantrell@enron.com`) |
| Distinct senders | 20,323 |
| Top 3 senders | `kay.mann@enron.com` 16,735; `vince.kaminski@enron.com` 14,368; `jeff.dasovich@enron.com` 11,411 |
| Distinct content hashes | 255,512 |
| Ingestion time | about 23 s natively (10-core MacBook, Postgres 16 in Docker, peak RSS about 60 MiB); about 2.5 min through docker compose on Docker Desktop for macOS |
| Database size | about 342 MB |

The parser-level numbers come from a test that parses the whole local dataset, which is skipped unless
`ENRON_MAILDIR` points at it:

```bash
ENRON_MAILDIR=$PWD/Dataset/maildir go test -count=1 -run TestCorpus -v ./internal/mailparse
ENRON_MAILDIR=$PWD/Dataset/maildir go test -tags integration -run FullCorpus -v -timeout 30m ./internal/ingest
```

The first reports totals per outcome with samples. It counts `invalid_recipient` per occurrence (219), not per
message (182). The second ingests everything into a Postgres testcontainer and logs the time taken.

## Operations

### Getting the dataset

`POST /fetch` streams the archive through gzip and tar into `$DATA_DIR/.fetch-tmp-*`, without buffering it in memory.
It renames the result to `$DATA_DIR/maildir` only after it has read the whole archive, including the gzip checksum.
`$DATA_DIR/.fetch-complete` then records the URL, SHA-256, size and file count.

Safety checks:

- Entries that are absolute, escape the target (`..`), are links or devices, or lie outside `maildir/` are skipped
  and logged.
- Before downloading, a fetch fails if `DATA_DIR` has less than 4 GiB free, or fewer than 530,000 free inodes on
  filesystems that report them. The dataset has about 521k files and directories, more than a small ext4 volume
  holds (see [Dataset volume and inodes](#dataset-volume-and-inodes)).
- It fails if no data arrives for 60 s, or if the archive extracts to more than 8 GiB.
- On failure or shutdown, the temporary directory is removed. A partial tree with many files can take longer than
  the 5 s close budget (for example about 140k files through a Docker Desktop bind mount). The process then exits
  with status 1 first (see [Shutdown](#shutdown)). Such leftovers, like those from a crash, are removed at the next
  startup.

The fetch state is held in memory in the pod that runs it. After a restart, a completed dataset is found on disk
again, and an interrupted fetch must be started again with `POST /fetch`.

### Ingestion engine

The engine walks `$DATA_DIR/maildir` in lexical order and parses files on `INGEST_WORKERS` goroutines. One writer
stores rows in batches of `INGEST_BATCH_SIZE`, or every second, with `ON CONFLICT (source_path) DO NOTHING`. Counters
are written to the run row every `INGEST_PROGRESS_INTERVAL` and at the end. At most one run is active in the cluster
(a Postgres session advisory lock on a dedicated connection). The dataset root must be a directory, not a symlink.
`POST /fetch` applies the same rule. To use a dataset stored elsewhere, mount it, or point `DATA_DIR` at the
directory that contains the real `maildir` directory.

### Startup

The service connects to Postgres and applies the embedded migrations
([ADR 0013](Docs/ADR/0013-goose-for-database-migrations.md)) before it listens. `DB_STARTUP_TIMEOUT` (default
`60s`) bounds all of it:

- While the database is not reachable yet (connection refused, DNS failure, no answer within 5 s or within a longer
  `connect_timeout` from `DATABASE_URL`, or the server says it is starting up, shutting down or out of
  connections), the service retries with capped exponential backoff and jitter (250 ms, 500 ms, 1 s, ... up to 5 s).
  It logs each retry at `WARN` as `database not ready, retrying` with the attempt number and the error. The
  password is never logged.
- A server that answers but rejects the configuration is not retried. This covers a wrong password or another
  authorization failure (SQLSTATE class `28`), a missing database (`3D000`), a missing privilege such as `CONNECT`
  on the database (`42501`), and a TLS failure (for example `sslmode=require` against a server without TLS, or a
  certificate that does not verify). When one connect makes several attempts (several hosts, or TLS then plaintext
  with `sslmode` unset, `prefer` or `allow`), it counts as permanent only if every attempt was rejected this way. A
  refused TLS attempt followed by "starting up" is retried.
- Waiting for the migration lock held by another replica polls every second until the same budget runs out.
- Migrations run inside the budget too. Each one runs in a transaction, so a migration cut off by the deadline or a
  signal rolls back completely.
- When the budget runs out, or on a permanent error (a malformed `DATABASE_URL`, one of the rejections above, a
  failed migration), the service logs `opening database failed` and exits with status 1. Compose and Kubernetes
  then restart it. Once the cause is fixed (for example the database is created, or the `CONNECT` grant exists),
  the next restart recovers without any other action.
- `SIGINT` or `SIGTERM` during startup stops the retries, and the service exits with status 0.

### Shutdown

On `SIGINT` or `SIGTERM` the service:

1. Reports not ready (`/readyz` returns `503 shutting_down`) and keeps serving for `SHUTDOWN_DRAIN_DELAY`, so that a
   load balancer or the Kubernetes Service drops it first.
2. Closes the listener and drains in-flight requests for up to `SHUTDOWN_TIMEOUT`.
3. Closes its resources within a fixed 5 s. A running ingestion normally needs at most 3 s of that: its workers stop
   at once, the run is recorded as `failed` with `interrupted: shutdown`, and it releases its lock. A slow database
   can extend this by up to 5 s. In that case, the final write is cut off, and the run is reported as `interrupted`
   by the crashed-owner path ([ADR 0010](Docs/ADR/0010-ingestion-state-in-postgres-with-advisory-lock.md)). A
   running fetch is cancelled and its temporary directory removed (or, if that takes too long, at the next startup).
4. Exits with status 0 after a clean, timely shutdown. It exits with status 1 if draining exceeded
   `SHUTDOWN_TIMEOUT`, or if the 5 s close budget ran out (handlers still running, or a closer such as the fetch
   cleanup did not finish), or if a closer failed. Either way nothing stored is lost: an interrupted run is reported
   as `interrupted`, and leftover fetch directories are removed at the next startup. A second signal exits at once.

The worst case is `SHUTDOWN_DRAIN_DELAY + SHUTDOWN_TIMEOUT + 5s`, which must fit in the orchestrator's grace period:
25 s with the defaults, within compose's 30 s `stop_grace_period`, and 30 s with the Kubernetes ConfigMap, within
the pod's `terminationGracePeriodSeconds: 40`.

### Logs

JSON on stdout (`log/slog`), one object per line, with `time`, `level`, `msg` and `service`, plus fields per event.
Each request is logged once as `http request` with `method`, `route` (the matched pattern, such as `GET /status`),
`path`, `status`, `duration_ms`, `bytes` and `request_id`. Ingestion logs carry `component: ingest` and `run_id`.

```json
{"time":"2026-10-23T11:29:22.7Z","level":"INFO","msg":"ingestion started","service":"enron-service","component":"ingest","run_id":1,"workers":10,"batch_size":2000}
```

### Metrics

`GET /metrics` serves Prometheus metrics. Vectors with labels appear after their first observation.

| Metric | Type | Meaning |
|---|---|---|
| `enron_http_requests_total{method,route,code}` | counter | Requests served. `route` is the matched pattern (`unmatched` otherwise), so the cardinality is bounded. |
| `enron_http_request_duration_seconds{method,route}` | histogram | Request latency |
| `enron_http_panics_total` | counter | Handler panics recovered by the middleware |
| `enron_fetch_bytes_total` | counter | Bytes of the archive downloaded |
| `enron_fetch_state{state}` | gauge | 1 for the current dataset state, 0 for the others |
| `enron_fetch_duration_seconds{result}` | histogram | Duration of finished fetches (`ready` or `failed`) |
| `enron_fetch_entries_skipped_total{reason}` | counter | Archive entries skipped during extraction |
| `enron_ingest_files_total` | counter | Dataset entries handled |
| `enron_ingest_messages_total` | counter | Messages parsed and written (inserted, or already stored) |
| `enron_ingest_skipped_total{reason}` | counter | Files skipped, by reason |
| `enron_ingest_field_errors_total{field}` | counter | Field errors, by kind |
| `enron_ingest_batch_duration_seconds` | histogram | Batch insert latency |
| `enron_ingest_running` | gauge | 1 while this process runs an ingestion |

The Go runtime (`go_*`) and process (`process_*`) collectors are registered too.

## Running on Kubernetes

The manifests live in `deploy/k8s` ([ADR 0006](Docs/ADR/0006-cloud-native-deployment-with-kustomize-and-kind.md),
[ADR 0014](Docs/ADR/0014-kubernetes-deployment-topology.md), [ADR 0015](Docs/ADR/0015-dataset-volume-sized-for-inodes.md)):

| Path | Contents |
|---|---|
| `base/` | Namespace `enron` (Pod Security `restricted` enforced), ConfigMap `enron-service-config`, PVC `enron-data` (10Gi, RWO), Deployment `enron-service` (1 replica, `Recreate`), Service `enron-service` (`ClusterIP` 80 to 8080) |
| `overlays/kind/` | The base, plus Postgres as a plain StatefulSet `postgres` (`postgres:16-alpine`, 2Gi, headless Service, no operator), the generated Secret `enron-db`, and the locally built image |
| `overlays/cloud/` | The base only: managed Postgres through an externally created Secret `enron-db`, image `REGISTRY/enron-service:VERSION` |
<!-- #40 adds its overlay as a row here. -->

There is one replica with the `Recreate` strategy, because the dataset volume is ReadWriteOnce, so a rollout has a
short outage. There is no PodDisruptionBudget: with one replica it would either block node drains forever
(`minAvailable: 1`) or allow the eviction anyway. The scaling path is under
[Possible extensions](#possible-extensions).

### Local cluster with kind

Prerequisites: Docker, [kind](https://kind.sigs.k8s.io/) and `kubectl` on the host (the Makefile never installs
them). kustomize, kubeconform and yq are pinned in `go.mod` and run through `go tool`.

```bash
make kind-up        # kind cluster "elisa-enron" (node image pinned in deploy/kind/cluster.yaml); no-op if it exists
make deploy         # make docker, kind load, apply overlays/kind, wait for postgres and enron-service
kubectl --context kind-elisa-enron -n enron port-forward svc/enron-service 8080:80
curl -s localhost:8080/readyz
curl -si -X POST localhost:8080/fetch    # then /start, /status, /top-senders as in the quick start
make undeploy       # delete the namespace and everything in it, including both volumes
make kind-down      # delete the cluster
```

- Every `kubectl` call in these targets passes `--context kind-$(KIND_CLUSTER)` (`KIND_CLUSTER` defaults to
  `elisa-enron`), so no other cluster is ever targeted. `kind create cluster` adds the `kind-elisa-enron` context to
  your kubeconfig and makes it current. `make kind-up` then switches back to the context that was current before
  (or leaves none current if there was none), and `make kind-down` removes the context again.
- `make deploy` loads the image under a tag derived from its image ID (`enron-service:dev-<id>`), so a rebuilt image
  rolls out even though the pod uses `imagePullPolicy: IfNotPresent`. `make docker` takes the image's timestamps
  from the commit (`SOURCE_DATE_EPOCH` and the `created` label) and disables the BuildKit provenance attestation, so
  running `make deploy` again for the same commit (with a warm build cache) yields the same image ID and leaves the
  running pod, and any in-flight `/fetch`, alone. A dirty tree changes `VERSION` and therefore the image, and so
  does a build from an empty cache.
- The credentials in `overlays/kind` (`enron`/`enron`) are for the throwaway local cluster only.
- The dataset gets into the cluster through `POST /fetch`, which downloads into the `enron-data` volume. kind's
  local-path provisioner backs it with a directory inside the node container, so neither the 10Gi request nor the
  inode count limits it there.
<!-- #40 (opt-in local Dataset mount for kind, skipping /fetch) adds its instructions here. -->

### Cloud (EKS, GKE)

The cloud overlay contains no database resources. Postgres is managed (RDS or Cloud SQL) and reached with
`sslmode=require` (or `verify-full`); on GKE, a Cloud SQL Auth Proxy sidecar is an alternative to a direct
connection. The StorageClass is the cluster default (gp3 on EKS, standard-rwo on GKE).

Set the real image in a local overlay on top of `overlays/cloud`, rather than editing the tracked overlay: running
`kustomize edit` there would dirty the repository and fail `make k8s-validate`, which expects the placeholder.
Directories named `deploy/k8s/overlays/*-local/` are gitignored.

```bash
kubectl create namespace enron   # the overlay also defines it; the Secret must exist before the pod starts
kubectl -n enron create secret generic enron-db \
  --from-literal=DATABASE_URL='postgres://USER:PASSWORD@HOST:5432/enron?sslmode=require'
mkdir -p deploy/k8s/overlays/prod-local
cat > deploy/k8s/overlays/prod-local/kustomization.yaml <<'EOF'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../cloud
images:
  - name: REGISTRY/enron-service     # the placeholder that overlays/cloud sets
    newName: ghcr.io/you/enron-service
    newTag: v1.2.3
EOF
kubectl apply -k deploy/k8s/overlays/prod-local
```

The same local overlay is where a size patch for `enron-data` goes (see the next section).

**Do not run `kubectl delete -k` on the cloud overlay** unless you mean to delete everything. The overlay contains
the `enron` Namespace, so deleting it deletes the namespace and everything in it: the out-of-band Secret `enron-db`,
and the PVC `enron-data` with the downloaded dataset (with the default `Delete` reclaim policy, the cloud disk is
deleted too). To stop the service, scale it down instead:
`kubectl -n enron scale deployment/enron-service --replicas=0`. To remove only the app, delete the Deployment and the
Service by name.

### Dataset volume and inodes

The PVC `enron-data` requests **10Gi** even though the extracted dataset is about 2.5 GB, because the dataset has
about 521k files and directories ([ADR 0015](Docs/ADR/0015-dataset-volume-sized-for-inodes.md)). ext4, the
filesystem of the EKS gp3 and GKE standard-rwo defaults, allocates about one inode per 16 KiB: 5Gi gives about 327k
inodes (too few), and 10Gi about 655k. `POST /fetch` checks before downloading, and fails fast with
`insufficient free inodes: need 530000, have N` below 530,000 free inodes, or with `insufficient disk space` below
4 GiB free.

To change the size, add a patch to your local overlay on top of `overlays/cloud` that keeps at least 530,000 free
inodes and 4 GiB free: at least 10Gi on ext4, and smaller only with a StorageClass whose filesystem allocates inodes
dynamically (for example XFS via `csi.storage.k8s.io/fstype: xfs`).

### Probes, shutdown and resources

| Setting | Value | Why |
|---|---|---|
| `startupProbe` | `GET /healthz`, every 5 s, `failureThreshold: 30` (150 s) | The service only listens after it removes leftover `.fetch-tmp-*` trees and opens and migrates the database, which can take up to `DB_STARTUP_TIMEOUT` (60 s, see [Startup](#startup)). Liveness and readiness start only after it succeeds, so a slow start or a database that is still down does not get the pod killed. |
| `livenessProbe` | `GET /healthz`, every 10 s, `failureThreshold: 3` | Process only: a database outage never restarts the pod |
| `readinessProbe` | `GET /readyz`, every 5 s, `timeoutSeconds: 3` | `/readyz` pings Postgres with a 2 s timeout, so the probe timeout must be longer than the Kubernetes default of 1 s |
| `SHUTDOWN_DRAIN_DELAY` / `SHUTDOWN_TIMEOUT` | `5s` / `20s` (ConfigMap) | On `SIGTERM` the pod reports not ready and keeps serving for 5 s, so the Service drops its endpoint before connections close |
| `terminationGracePeriodSeconds` | `40` | Above the worst-case shutdown: 5 s drain + 20 s `SHUTDOWN_TIMEOUT` + 5 s for closers = 30 s |
| Resources | requests `cpu: 250m, memory: 256Mi`; limit `memory: 1Gi` | No CPU limit, to avoid throttling during ingestion |
| Security | uid/gid `65532`, `fsGroup: 65532`, `readOnlyRootFilesystem`, no privilege escalation, all capabilities dropped, `RuntimeDefault` seccomp, no service account token | `/tmp` is an `emptyDir`. Cloud CSI block volumes (EBS gp3, GCE PD) support `fsGroup`, so kubelet makes the volume root group `65532` and group-writable. kind's local-path volume is a hostPath directory created with mode `0777`, to which kubelet does not apply `fsGroup`. `fsGroupChangePolicy: OnRootMismatch` (also on Postgres) avoids a recursive chown of about 517k dataset files on every pod start. |

The pod template carries `prometheus.io/scrape`, `prometheus.io/port: "8080"` and `prometheus.io/path: /metrics`.

### Validating the manifests

<!-- #40 updates "both overlays" and the list of checks below for its overlay. -->
`make k8s-validate` renders the base and both overlays with the pinned kustomize, validates every resource with
kubeconform (`-strict`, the Kubernetes schema of the kind node version, cached in `.tools/`), and checks the values
above with yq. It checks probes, grace period, replicas, `Recreate`, the 10Gi RWO PVC, hardening, Postgres as the
only StatefulSet in kind, and that the cloud overlay has no StatefulSet and no Secret. It also checks that the grace
period exceeds drain + shutdown + 5 s, and that the startup budget exceeds `DB_STARTUP_TIMEOUT`.
`kubectl kustomize deploy/k8s/overlays/kind` renders the same output as the pinned kustomize.
<!-- #13 (kind end-to-end smoke test) adds its section here. -->

## Development

### Tooling

Prerequisites: Go (the version in `go.mod`), Python 3 and Docker. Everything else is project-local and pinned
([ADR 0008](Docs/ADR/0008-project-local-pinned-tooling-and-ci.md)); nothing is installed globally:

- Go tools (golangci-lint v2, goimports, kustomize, kubeconform, yq) are `tool` directives in `go.mod`, run with
  `go tool <name>`.
- pre-commit and its hook packages are locked with hashes in `requirements-dev.txt` and installed into `.venv/` by
  `make tools`. To change them, edit `requirements-dev.in` and run `make py-lock`.

```bash
make tools       # .venv with pinned pre-commit, the git pre-commit hook, Go modules
make lint test   # golangci-lint and go vet, then unit tests with -race
make help        # every target
```

| Target | What it does |
|---|---|
| `tools` | Create `.venv`, install the git hook, download Go modules |
| `fmt` | goimports (with the module as local prefix) and `gofmt -s` |
| `lint` | golangci-lint, and `go vet` including integration-tagged files |
| `test` | Unit tests with `-race` (`TEST_FLAGS` adds flags) |
| `itest` | Unit and integration tests with `-race`; needs Docker |
| `build` | Static binary `bin/enron-service` |
| `pre-commit` | Every pre-commit hook on every file |
| `docker`, `docker-smoke` | Build the image `enron-service:dev` (`IMAGE`, `VERSION` overridable); build and smoke-test it with compose on ports 18080/15432 |
| `compose-up`, `compose-down`, `compose-clean` | The [docker compose](#quick-start-docker-compose) stack |
| `k8s-validate` | [Validate the manifests](#validating-the-manifests) |
| `kind-up`, `deploy`, `undeploy`, `kind-down` | The [kind](#local-cluster-with-kind) workflow |
| `py-lock` | Re-resolve `requirements-dev.in` into the hashed lock |
| `clean` | Remove build outputs (`bin/`) |
<!-- #13 adds its `e2e-kind` target as a row here. -->

The image is built in two stages: a static binary (`CGO_ENABLED=0`, `-trimpath -s -w`, version from `VERSION`) on
`gcr.io/distroless/static-debian12:nonroot`. It has no shell, runs as `65532:65532`, listens on 8080 and is about
24 MB. Base images are pinned by digest in `Dockerfile` and `compose.yaml`.

### Pre-commit hooks

The git hook runs gofmt `-s`, goimports, `go vet`, golangci-lint, a `go mod tidy` check, and trailing-whitespace,
end-of-file, YAML, merge-conflict and large-file checks. Run `make tools` in every checkout and git worktree before
committing. Git worktrees share one hooks directory, but the hook uses the `.venv` of the worktree being committed
to, and refuses the commit (with a hint) if that `.venv` is missing.

For the AI agents that work on this repository, a Claude Code `PreToolUse` hook
(`.claude/hooks/block-hook-bypass.py`) refuses shell commands that would skip the git hooks: `--no-verify`,
`commit -n`, `SKIP=` and overriding `core.hooksPath`.

### Tests

| Kind | Where | Run with |
|---|---|---|
| Unit | `*_test.go` next to the code; fixtures in `testdata/` (including malformed mail) | `make test` |
| Integration | `*_integration_test.go` behind the `integration` build tag, against real Postgres 16 in testcontainers (`cmd/enron-service`, `internal/ingest`, `internal/store`) | `make itest` (needs Docker) |
| Full dataset | `TestCorpus` (parser) and the `FullCorpus` ingestion test, skipped unless `ENRON_MAILDIR` is set | see [Results on the real dataset](#results-on-the-real-dataset) |
| Image and compose | `scripts/docker-smoke.sh` | `make docker-smoke` |
| Manifests | `scripts/k8s-validate.sh` | `make k8s-validate` |
<!-- #13 adds the kind end-to-end test as a row here. -->

`-race` needs cgo. If your C toolchain cannot link (for example Xcode command line tools that do not match the
macOS SDK), `CGO_ENABLED=0 go test -count=1 ./...` runs the same tests without the race detector; CI always uses
`-race`.

### CI

GitHub Actions (`.github/workflows/ci.yml`) runs on every pull request and on pushes to `main`, through the same
Makefile targets:

| Job | Steps |
|---|---|
| `lint` | Every pre-commit hook (formatting, `go mod tidy`, file checks), then `make lint` |
| `test` | `make test` with `-race`; `coverage.out` is uploaded as an artifact |
| `itest` | `make itest` with testcontainers on the runner's Docker |
| `docker` | Build the image with the GitHub Actions cache (never pushed), then `scripts/docker-smoke.sh` |
| `manifests` | `make k8s-validate`, and a check that `kubectl kustomize` renders the same output as the pinned kustomize |
<!-- #13 adds the kind e2e job as a row here. -->

A new push to a branch or PR cancels that ref's older run. On `main` every commit gets its own full run, which is
never cancelled. Third-party actions are pinned to commit SHAs.

### Workflow

The project is built by AI agents under human review ([`CLAUDE.md`](CLAUDE.md)). Work is tracked on a
[GitHub Project](https://github.com/users/hrhelena/projects/1) kanban board. A product-owner agent writes the ADRs
and the tickets (spec plus Gherkin scenarios). A coder agent implements one ticket per branch and opens a PR. A
reviewer agent reviews it until it approves. Architectural decisions are confirmed by the user and recorded as ADRs.
Branches are `<issue>-<slug>`, and commits follow Conventional Commits.

## Design decisions

The full records are in [`Docs/ADR`](Docs/ADR/README.md). In short:

| ADR | Decision |
|---|---|
| [0001](Docs/ADR/0001-use-postgres-as-the-datastore.md) | Postgres (pgx, pgxpool) over SQLite: stateless pods, cluster-wide locking, managed offerings in the cloud |
| [0002](Docs/ADR/0002-no-full-text-indexing-store-headers-only.md) | No full-text index and no bodies: only parsed headers and a content hash are stored |
| [0003](Docs/ADR/0003-count-every-maildir-file-as-one-message.md) | One maildir file is one message, with no deduplication |
| [0004](Docs/ADR/0004-add-fetch-endpoint-for-dataset-download.md) | A separate, idempotent `POST /fetch` downloads the dataset; `POST /start` only ingests |
| [0005](Docs/ADR/0005-no-authentication-or-authorization.md) | No authentication, authorization or accounting; the middleware chain is the extension point |
| [0006](Docs/ADR/0006-cloud-native-deployment-with-kustomize-and-kind.md) | Distroless image, kustomize base and overlays, kind locally, EKS or GKE with managed Postgres |
| [0007](Docs/ADR/0007-tolerate-malformed-dataset-entries.md) | Bad entries are skipped or tolerated, counted and logged; they never fail a run |
| [0008](Docs/ADR/0008-project-local-pinned-tooling-and-ci.md) | Project-local pinned tools, pre-commit hooks, GitHub Actions |
| [0009](Docs/ADR/0009-extensible-layered-architecture.md) | Small layers behind narrow, consumer-defined interfaces |
| [0010](Docs/ADR/0010-ingestion-state-in-postgres-with-advisory-lock.md) | Run state in Postgres; single-flight ingestion with a session advisory lock; runs execute in-process |
| [0011](Docs/ADR/0011-http-api-contract-and-start-semantics.md) | The JSON contract; `/start` after a finished run re-ingests idempotently; `finished` means completed |
| [0012](Docs/ADR/0012-lenient-sender-normalization.md) | Strict-then-lenient sender parsing, kept as written, invalid senders excluded from the ranking |
| [0013](Docs/ADR/0013-goose-for-database-migrations.md) | goose v3 with SQL migrations embedded and applied at startup |
| [0014](Docs/ADR/0014-kubernetes-deployment-topology.md) | One replica, RWO dataset volume, `Recreate`; Postgres StatefulSet in kind |
| [0015](Docs/ADR/0015-dataset-volume-sized-for-inodes.md) | The dataset volume is 10Gi for its inode count, not its size |

## Known limitations

- **No authentication.** Anyone who can reach the service can start a fetch or an ingestion. The manifests expose it
  only as a `ClusterIP` Service; do not expose it publicly
  ([ADR 0005](Docs/ADR/0005-no-authentication-or-authorization.md)).
- **One replica.** The dataset volume is ReadWriteOnce, so the Deployment has one replica, a rollout has a short
  outage, and reads do not scale out ([ADR 0014](Docs/ADR/0014-kubernetes-deployment-topology.md)).
- **Counts include copies.** Per-file counting means a message in both `sent` and `_sent_mail` counts twice, and the
  ranking reflects how many folders people kept, not only how much they wrote (16,735 files but 4,887 distinct
  emails for the top sender).
- **Odd sender strings rank on their own.** Addresses such as `.ward@enron.com` or `w..cantrell@enron.com` are not
  merged with the person's usual address.
- **Fetch progress is per pod and in memory.** A restart during a download loses it, and a new `POST /fetch` starts
  over. On another replica, the `dataset` section of `/status` shows that replica's view.
- **A re-run re-reads everything.** `POST /start` after a completed run walks and parses all 517k files again, even
  though it inserts nothing new ([ADR 0011](Docs/ADR/0011-http-api-contract-and-start-semantics.md)).
- **No bodies stored.** Any body-based feature (search, content analytics) has to read the maildir again
  ([ADR 0002](Docs/ADR/0002-no-full-text-indexing-store-headers-only.md)).
- **Schema changes ship with the binary.** Migrations run at startup; there is no independent schema rollback
  ([ADR 0013](Docs/ADR/0013-goose-for-database-migrations.md)). A future `-- +goose NO TRANSACTION` migration (such
  as `CREATE INDEX CONCURRENTLY`) could be cut off halfway by the startup deadline, so it must be safe to run again.
- **Session-bound lock.** Transaction-mode poolers are not supported, and idle-connection killers can drop the
  ingest lock and fail the run with `lock_lost` (see [Database connection](#database-connection)).
- **`messages_inserted` can undercount.** If a batch commit succeeds but its answer is lost, the retry finds the rows
  already stored, so the run's internal `messages_inserted` counter undercounts. `messages_processed` and the
  stored rows stay exact.
- **Close can time out during a failed start.** If a start failure has already begun releasing its lock with the
  10 s write timeout, a concurrent shutdown does not shorten it. Closing can then exceed its 5 s budget and the
  process exits with status 1. No data is lost, and the run is reported as `interrupted`.
- **Slow compose on macOS.** Through the Docker Desktop bind mount, an ingestion takes minutes instead of about
  23 s.

## Possible extensions

The layering ([ADR 0009](Docs/ADR/0009-extensible-layered-architecture.md)) leaves a clear place for each of these.

- **Full-text search.** Store bodies (today only hashed) and index them, either in Postgres (a `tsvector` column with
  a GIN index, no new component) or in OpenSearch/Elasticsearch (better relevance and aggregations, one more
  stateful service). Expose it as `GET /search?q=...`. It needs a migration and a re-ingest, because bodies are not
  in the database ([ADR 0002](Docs/ADR/0002-no-full-text-indexing-store-headers-only.md)).
- **Deduplication.** `content_sha256` is already stored for every row, so "distinct emails" is a query away
  (`COUNT(DISTINCT content_sha256)`, or a ranking over distinct hashes per sender), for example as
  `GET /top-senders?distinct=true`. No re-ingest is needed.
- **Other data sources.** `internal/ingest` reads from the `Source` interface (`Walk` and `Available`). The maildir
  walker is the first implementation; mbox, PST, IMAP or an S3 prefix would be others, chosen by configuration.
- **Dataset in object storage.** Read the archive or the extracted tree straight from S3 or GCS (a `Source` and a
  `fetch` target), which removes the dataset volume and its inode sizing.
- **Richer analytics.** The stored headers already support top recipients (`to_addrs`, `cc_addrs`, `bcc_addrs`),
  a who-emails-whom graph, volume over time (`sent_at`), per-mailbox and per-folder statistics, and thread
  reconstruction (from `In-Reply-To`/`References` and subjects, which would need two more columns). Each is a query
  and a handler behind a new small interface.
- **Authentication and authorization.** The router has two empty middleware slots
  ([ADR 0005](Docs/ADR/0005-no-authentication-or-authorization.md)): a global one for authentication (OIDC/JWT, which
  puts the caller identity in the request context), and one for the mutating routes `POST /fetch` and `POST /start`
  for authorization (for example admin-only). Alternatively, enforce auth outside the service, at the ingress
  (OAuth2 proxy, cloud IAP) or in a service mesh (mTLS and authorization policies).
- **Horizontal scaling** ([ADR 0014](Docs/ADR/0014-kubernetes-deployment-topology.md)). Either use RWX storage (EFS,
  Filestore) for `DATA_DIR` so several API replicas share the dataset, or run fetch and ingestion as a Kubernetes
  Job, or as queue-driven workers, behind stateless API replicas that serve `/status` and `/top-senders`. Both work
  as they are, because run state and the lock are already in Postgres.
- **Incremental ingestion.** Skip files already stored (by path and modification time) instead of re-parsing them,
  or watch the source for new files.
- **Tracing.** OpenTelemetry spans for requests, batches and database calls, next to the existing metrics.
