-- One row per maildir file (ADR 0003). Parsed headers only, no bodies (ADR 0002).
--
-- source_path and from_addr use COLLATE "C" (byte order). Their ordering is part
-- of the contract: /top-senders breaks ties by address (ADR 0011), and the
-- default collation differs by server (musl/alpine sorts bytes, glibc and ICU
-- sort punctuation by locale rules), so ".ward@enron.com" (ADR 0012) would rank
-- differently in kind and in a managed cloud database. "C" also makes the
-- unique and sender index comparisons plain memcmp.

-- +goose Up
CREATE TABLE messages (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_path    TEXT        COLLATE "C" NOT NULL UNIQUE, -- relative to $DATA_DIR/maildir, e.g. "allen-p/_sent_mail/1."
    mailbox        TEXT        NOT NULL,          -- "allen-p"
    folder         TEXT        NOT NULL,          -- "_sent_mail", nested folders joined by "/", "" if none
    message_id     TEXT,
    sent_at        TIMESTAMPTZ,                   -- NULL when Date is missing or unparseable
    date_raw       TEXT,
    from_addr      TEXT        COLLATE "C",       -- normalized sender (ADR 0012); NULL when missing/invalid
    from_raw       TEXT,
    x_from         TEXT,
    to_addrs       TEXT[]      NOT NULL DEFAULT '{}',
    cc_addrs       TEXT[]      NOT NULL DEFAULT '{}',
    bcc_addrs      TEXT[]      NOT NULL DEFAULT '{}',
    subject        TEXT,
    content_sha256 BYTEA       CHECK (content_sha256 IS NULL OR length(content_sha256) = 32),
    run_id         BIGINT,                        -- FK to ingestion_runs added in 00002 (#5)
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Serves GET /top-senders (GROUP BY from_addr over non-NULL senders). The partial
-- predicate matches the query's WHERE clause, so NULL senders are not indexed.
CREATE INDEX messages_from_addr_idx ON messages (from_addr) WHERE from_addr IS NOT NULL;

-- +goose Down
DROP TABLE messages;
