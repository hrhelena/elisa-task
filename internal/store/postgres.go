package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// checkerName is the readiness check name reported by /readyz (#3).
const checkerName = "postgres"

// pingTimeout bounds one connection attempt in Open, so a server that drops
// packets (rather than refusing the connection) cannot use up the caller's
// whole startup budget in a single attempt that the caller would retry. A
// longer connect_timeout in the database URL takes precedence.
const pingTimeout = 5 * time.Second

// messageColumnList is the single source of truth for the columns that
// InsertMessages writes, in the order of the values built by messageRow.
// id and ingested_at are filled in by the database.
const messageColumnList = "source_path, mailbox, folder, message_id, sent_at, date_raw, from_addr, from_raw, " +
	"x_from, to_addrs, cc_addrs, bcc_addrs, subject, content_sha256, run_id"

const (
	// The staging table copies only the column types: no NOT NULL, UNIQUE or
	// identity, so COPY never fails on a conflict and duplicates are resolved
	// by the final INSERT. It lives for one transaction, in this session only.
	createStagingSQL = "CREATE TEMP TABLE messages_staging ON COMMIT DROP AS SELECT " +
		messageColumnList + " FROM messages WITH NO DATA"
	// ORDER BY source_path makes every writer take unique-index locks in the
	// same order, so concurrent batches with overlapping paths cannot
	// deadlock (ON CONFLICT DO NOTHING waits on in-progress conflicting rows).
	mergeStagingSQL = "INSERT INTO messages (" + messageColumnList + ") SELECT " + messageColumnList +
		" FROM messages_staging ORDER BY source_path ON CONFLICT (source_path) DO NOTHING"
	countMessagesSQL = "SELECT count(*) FROM messages"
	// Served by messages_from_addr_idx. Ties break on the address so the
	// ranking is deterministic (ADR 0011); from_addr is COLLATE "C", so that
	// order is byte order on every server, independent of its locale.
	topSendersSQL = `SELECT from_addr, count(*) FROM messages WHERE from_addr IS NOT NULL
GROUP BY from_addr ORDER BY count(*) DESC, from_addr ASC LIMIT $1`
)

// Postgres is the Store backed by PostgreSQL via a pgx connection pool.
type Postgres struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

var _ Store = (*Postgres)(nil)

// Open connects to databaseURL and applies pending migrations before
// returning, so a returned store always has the current schema. Pool
// settings can be given in the URL (pool_max_conns and so on).
//
// A transient connection failure (see isTransientConnectError) wraps
// ErrUnreachable and may be retried; any other error, including a rejected
// password, a missing database or a TLS failure, is permanent. ctx bounds
// the whole call, including the wait for the migration lock (see migrate).
func Open(ctx context.Context, databaseURL string, logger *slog.Logger) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing database URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, max(pingTimeout, cfg.ConnConfig.ConnectTimeout))
	err = pool.Ping(pingCtx)
	cancel()
	if err != nil {
		pool.Close()
		if isTransientConnectError(err) {
			return nil, fmt.Errorf("connecting to postgres: %w: %w", ErrUnreachable, err)
		}
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	if err := migrate(ctx, pool, logger); err != nil {
		pool.Close()
		return nil, err
	}
	return &Postgres{pool: pool, logger: logger}, nil
}

// Close closes the connection pool. It waits for acquired connections to be
// released.
func (p *Postgres) Close() { p.pool.Close() }

// Name returns the readiness check name, "postgres".
func (*Postgres) Name() string { return checkerName }

// Check reports whether the database is reachable.
func (p *Postgres) Check(ctx context.Context) error {
	if err := p.pool.Ping(ctx); err != nil {
		return fmt.Errorf("pinging postgres: %w", err)
	}
	return nil
}

// InsertMessages stores msgs in one transaction: COPY into a temporary
// staging table, then INSERT ... ON CONFLICT (source_path) DO NOTHING.
// It returns the number of newly inserted rows. Paths that are already
// stored, or repeated within msgs, are skipped without error, which makes
// re-ingestion idempotent (ADR 0003, 0010). All text is sanitized first, so
// two paths that differ only in invalid UTF-8 bytes map to the same key and
// the second counts as already stored (maildir paths are ASCII in practice).
func (p *Postgres) InsertMessages(ctx context.Context, msgs []Message) (inserted int64, err error) {
	if len(msgs) == 0 {
		return 0, nil
	}
	rows := make([][]any, len(msgs))
	for i := range msgs {
		if rows[i], err = messageRow(&msgs[i]); err != nil {
			return 0, err
		}
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginning insert transaction: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(context.WithoutCancel(ctx)); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rolling back insert transaction: %w", rbErr))
		}
	}()

	if _, err := tx.Exec(ctx, createStagingSQL); err != nil {
		return 0, fmt.Errorf("creating staging table: %w", err)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"messages_staging"}, messageColumns(), pgx.CopyFromRows(rows)); err != nil {
		return 0, fmt.Errorf("copying %d messages to staging: %w", len(rows), err)
	}
	tag, err := tx.Exec(ctx, mergeStagingSQL)
	if err != nil {
		return 0, fmt.Errorf("merging staged messages: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing inserted messages: %w", err)
	}
	return tag.RowsAffected(), nil
}

// CountMessages returns the number of stored messages.
func (p *Postgres) CountMessages(ctx context.Context) (int64, error) {
	var n int64
	if err := p.pool.QueryRow(ctx, countMessagesSQL).Scan(&n); err != nil {
		return 0, fmt.Errorf("counting messages: %w", err)
	}
	return n, nil
}

// TopSenders returns up to limit senders ordered by message count
// descending, then address ascending. Messages without a valid sender
// (from_addr NULL) are excluded. limit < 1 returns ErrInvalidLimit. The
// result is never nil.
func (p *Postgres) TopSenders(ctx context.Context, limit int) ([]SenderCount, error) {
	if limit < 1 {
		return nil, fmt.Errorf("top senders limit %d: %w", limit, ErrInvalidLimit)
	}
	rows, err := p.pool.Query(ctx, topSendersSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("querying top senders: %w", err)
	}
	senders, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (SenderCount, error) {
		var s SenderCount
		err := row.Scan(&s.Email, &s.Count)
		return s, err
	})
	if err != nil {
		return nil, fmt.Errorf("reading top senders: %w", err)
	}
	if senders == nil {
		senders = []SenderCount{}
	}
	return senders, nil
}

// messageColumns returns the insert column names for COPY.
func messageColumns() []string {
	return strings.Split(messageColumnList, ", ")
}

// messageRow validates m and returns its sanitized column values in
// messageColumnList order. Validation runs on the sanitized values, which
// are what gets stored.
func messageRow(m *Message) ([]any, error) {
	sourcePath := sanitizeString(m.SourcePath)
	if sourcePath == "" {
		return nil, fmt.Errorf("%w: empty source path (%q before sanitizing)", ErrInvalidMessage, m.SourcePath)
	}
	if n := len(m.ContentSHA256); n != 0 && n != 32 {
		return nil, fmt.Errorf("%w: %q: content hash is %d bytes, want 32", ErrInvalidMessage, m.SourcePath, n)
	}
	var hash []byte // nil -> NULL; an empty non-nil slice would be stored as ''
	if len(m.ContentSHA256) == 32 {
		hash = m.ContentSHA256
	}
	var runID *int64 // 0 -> NULL, so rows outside a run satisfy the FK added in #5
	if m.RunID != 0 {
		runID = &m.RunID
	}
	// An empty sender is no sender: NULL, so it is excluded from top senders
	// rather than ranked as "" (ADR 0011, 0012).
	fromAddr := sanitizeOptional(m.FromAddr)
	if fromAddr != nil && *fromAddr == "" {
		fromAddr = nil
	}
	return []any{
		sourcePath,
		sanitizeString(m.Mailbox),
		sanitizeString(m.Folder),
		sanitizeOptional(m.MessageID),
		m.SentAt,
		sanitizeOptional(m.DateRaw),
		fromAddr,
		sanitizeOptional(m.FromRaw),
		sanitizeOptional(m.XFrom),
		sanitizeList(m.To),
		sanitizeList(m.Cc),
		sanitizeList(m.Bcc),
		sanitizeOptional(m.Subject),
		hash,
		runID,
	}, nil
}
