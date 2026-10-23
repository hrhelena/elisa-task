# 0007. Tolerate malformed dataset entries

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#6](https://github.com/hrhelena/elisa-task/issues/6), [#7](https://github.com/hrhelena/elisa-task/issues/7), [#8](https://github.com/hrhelena/elisa-task/issues/8), [0003](0003-count-every-maildir-file-as-one-message.md), [0004](0004-add-fetch-endpoint-for-dataset-download.md)

## Summary

Ingestion never fails because of a bad entry. Malformed entries are tolerated where possible, skipped otherwise,
counted, reported in `GET /status` and logged.

## Context

The dataset comes from an external source. A scan on 2026-10-23 of the 517,401 files found, for example:

- 90 files that `grep` classifies as binary (non-UTF-8 bytes, odd encodings).
- About 1,557 `From` headers in a broken `name <.local@enron.com>` form (for example `houston <.ward@enron.com>`), and one
  with no domain at all (`pep <performance.>`).
- Not yet quantified, but expected: RFC 2047 encoded words in headers and non-standard date formats.

Other risks: empty files, non-email files, very long header lines, NUL bytes (Postgres `TEXT` rejects them).

## Decision

Two levels of problems:

1. **Field-level problems** (the file is still a message): missing or invalid `From`, unparseable `Date`, undecodable
   `Subject`, invalid UTF-8. The message is stored and counted. The bad field is stored as `NULL` (or raw/sanitized text),
   and a per-run counter is incremented (for example `invalid_sender`, `invalid_date`). Text is sanitized before insert:
   invalid UTF-8 becomes U+FFFD and NUL bytes are removed.
2. **File-level problems** (not a message): empty files, files with no parseable header block, unreadable files,
   files over a size limit. The file is skipped, not counted as a message, and counted in `messages_skipped`, with a
   reason code.

Reporting:
- `GET /status` reports `messages_skipped` and the field-level counters for the current or last run.
- Each skip and field problem is logged at `WARN` with the relative path and reason, sampled after the first 100 per
  reason per run to protect the log volume. Per-reason totals are logged at the end of the run.
- Prometheus counters: `enron_ingest_skipped_total{reason}` and `enron_ingest_field_errors_total{field}`.
- Only infrastructure errors (database unreachable, data directory missing or unreadable at the root) fail a run.

## Rationale

Failing on the first bad record would make ingestion of a real-world dataset impossible. Silently dropping data would
make counts unexplainable. Counting and reporting gives both robustness and auditability.

## Consequences

- Positive: ingestion completes on the real dataset; the `/status` numbers add up
  (`files_seen = messages_processed + messages_skipped`).
- Negative: the parser must be tested with a fixture set of malformed files (`testdata/`).
- Follow-ups: exact sender normalization rules are an open question (see the parser ticket).
