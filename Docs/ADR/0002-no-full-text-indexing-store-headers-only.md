# 0002. No full-text indexing; store parsed headers only

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#4](https://github.com/hrhelena/elisa-task/issues/4), [#6](https://github.com/hrhelena/elisa-task/issues/6), [#14](https://github.com/hrhelena/elisa-task/issues/14), [0001](0001-use-postgres-as-the-datastore.md), [0003](0003-count-every-maildir-file-as-one-message.md), [0009](0009-extensible-layered-architecture.md)

## Summary

The service does not build a full-text index and does not store message bodies. It stores parsed headers and file
location metadata only. Full-text search is a documented future extension.

## Context

- The required endpoints are `POST /start`, `GET /status` (finished flag and message count) and `GET /top-senders`
  (top 10 senders). None of them searches text.
- Top senders is a `GROUP BY` on the normalized From address, which a B-tree index on that column serves well.
- Bodies make up most of the 2.3 GB dataset. Storing them would multiply the database size, ingestion time and memory use
  for no current consumer.

## Decision

- Per maildir file, store: relative file path (unique), mailbox (user directory, e.g. `allen-p`), folder (e.g. `_sent_mail`),
  `Message-ID`, raw and parsed `Date`, raw and normalized `From`, `X-From`, `To`/`Cc`/`Bcc` address lists, `Subject`,
  and a content hash (see [0003](0003-count-every-maildir-file-as-one-message.md)).
- Do not store bodies or attachments. Do not create `tsvector` columns, GIN indexes or an external search engine.
- Index the normalized sender column for `/top-senders`.

## Rationale

| Option | Pros | Cons |
|---|---|---|
| No FTS, headers only (chosen) | Small DB, fast ingestion, simple schema | Text search needs a later migration and re-ingest |
| Postgres `tsvector` + GIN | No extra component | Larger DB, slower ingestion, no consumer today |
| OpenSearch / Elasticsearch | Best search features | Another stateful service to run in kind and the cloud |

YAGNI: no requirement needs search, and adding it later is cheap because ingestion is re-runnable.

## Consequences

- Positive: fast ingestion and a small database; the schema stays easy to explain.
- Negative: adding search later requires re-reading the maildir files (bodies are not in the DB).
- Follow-ups: the README lists FTS (Postgres `tsvector` or OpenSearch) as a possible extension.
