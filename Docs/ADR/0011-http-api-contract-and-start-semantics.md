# 0011. HTTP API contract and /start semantics

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#8](https://github.com/hrhelena/elisa-task/issues/8), [#9](https://github.com/hrhelena/elisa-task/issues/9), [#10](https://github.com/hrhelena/elisa-task/issues/10), [0003](0003-count-every-maildir-file-as-one-message.md), [0004](0004-add-fetch-endpoint-for-dataset-download.md), [0007](0007-tolerate-malformed-dataset-entries.md), [0010](0010-ingestion-state-in-postgres-with-advisory-lock.md)

## Summary

The JSON contract for `POST /start`, `GET /status` and `GET /top-senders` is fixed. `POST /start` after a finished run
starts a new, idempotent re-ingest; `finished` is `true` only for a completed run; `/top-senders` returns a `senders`
array with an optional `limit` of 1 to 100.

## Context

`Docs/task-description.txt` names the endpoints but not their payloads, status codes or behavior on repeated calls.
Several replicas may serve requests ([0010](0010-ingestion-state-in-postgres-with-advisory-lock.md)), and the dataset may be
absent until `POST /fetch` has run ([0004](0004-add-fetch-endpoint-for-dataset-download.md)).

## Decision

**Errors (all endpoints):** `{"error":{"code":"<snake_case>","message":"<text>"}}` with `Content-Type: application/json`.

**`POST /start`:**

| Situation | Response |
|---|---|
| No run in progress (never ran, or last run `completed`/`failed`) | `202 Accepted`, `Location: /status`, `{"run_id":4,"state":"running","started_at":"..."}` |
| A run is in progress on any replica | `409 Conflict`, code `ingestion_running` |
| `$DATA_DIR/maildir` missing | `409 Conflict`, code `dataset_missing` |
| Database unavailable | `503 Service Unavailable`, code `unavailable` |

A new run after a finished one re-walks the dataset; files already stored (same relative path) are not inserted again,
so nothing is double-counted.

**`GET /status`:** `200` with `state` (`idle`, `running`, `completed`, `failed`), `finished` (`true` only when
`state == "completed"`; a failed run reports `false`), `messages_processed` (messages parsed and stored by the
current or last run), `total_messages` (rows stored), `files_seen`, `messages_skipped`, `skip_reasons`, `field_errors`,
`run_id`, `started_at`, `finished_at`, `updated_at`, `error`, and a `dataset` object (from `/fetch`). Maps are never `null`.

**`GET /top-senders[?limit=N]`:** `200` with `{"senders":[{"email":"kay.mann@enron.com","count":16735}, ...]}`.
`limit` defaults to 10 and accepts integers 1 to 100; anything else is `400`, code `invalid_limit`. Order is `count`
descending, then `email` ascending. Messages without a valid sender are excluded. An empty database gives `{"senders":[]}`.

## Rationale

| Question | Options | Chosen and why |
|---|---|---|
| `/start` after completion | 202 re-ingest; 409 `ingestion_completed`; 409 unless `?force=true` | 202 re-ingest: simple, retry-safe, resumes crashed runs, and idempotent by design |
| Meaning of `finished` | true when completed; true when completed or failed | Only completed: a failed ingestion has not finished its job; `state` carries the detail |
| `/top-senders` shape | `{"senders":[...]}` with `limit`; bare array of 10 | Wrapping object leaves room for metadata; `limit` costs almost nothing |

## Consequences

- Positive: clients can poll `/status` and retry `/start` safely; responses can grow without breaking clients.
- Negative: a re-run re-reads the whole dataset even when nothing changed.
- Follow-ups: #8, #9, #10 implement this contract; #14 documents it.
