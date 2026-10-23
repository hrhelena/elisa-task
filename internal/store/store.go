package store

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrInvalidLimit is returned by TopSenders when limit < 1.
	ErrInvalidLimit = errors.New("limit must be at least 1")
	// ErrInvalidMessage is returned by InsertMessages when a message breaks
	// an invariant the caller owns (empty SourcePath, hash not 32 bytes).
	// It signals a programming error, not malformed dataset content: that is
	// sanitized or stored as NULL instead (ADR 0007).
	ErrInvalidMessage = errors.New("invalid message")
	// ErrUnreachable is wrapped by Open when connecting failed for a reason
	// that may pass: the server is down, starting up, out of connections,
	// unresolvable or slow. The caller may retry. Every other Open error is
	// permanent: a malformed URL, a rejected password (SQLSTATE class 28), a
	// missing database (3D000), a TLS failure, or a failed migration.
	ErrUnreachable = errors.New("database unreachable")
)

// Message is one parsed maildir file (ADR 0003). Optional header fields are
// pointers: nil is stored as NULL.
type Message struct {
	SourcePath string // relative to $DATA_DIR/maildir, e.g. "allen-p/_sent_mail/1."; unique key
	Mailbox    string // "allen-p"
	Folder     string // "_sent_mail"; nested folders joined by "/", "" if none

	MessageID *string
	DateRaw   *string
	FromAddr  *string // normalized sender (ADR 0012); nil (or "") when missing or invalid, stored as NULL
	FromRaw   *string
	XFrom     *string
	Subject   *string

	SentAt *time.Time // nil when Date is missing or unparseable

	To, Cc, Bcc []string // nil is stored as '{}'

	ContentSHA256 []byte // 32 bytes or nil
	RunID         int64  // ingestion run; 0 is stored as NULL
}

// SenderCount is one row of the top-senders ranking.
type SenderCount struct {
	Email string
	Count int64
}

// MessageWriter stores parsed messages.
type MessageWriter interface {
	// InsertMessages stores msgs idempotently, keyed by SourcePath, and
	// returns how many rows were newly inserted.
	InsertMessages(ctx context.Context, msgs []Message) (inserted int64, err error)
}

// MessageStats answers the read-side metrics queries.
type MessageStats interface {
	CountMessages(ctx context.Context) (int64, error)
	// TopSenders ranks senders by message count descending, then email
	// ascending, excluding messages without a valid sender.
	TopSenders(ctx context.Context, limit int) ([]SenderCount, error)
}

// Store is the full storage surface of the service. It exists to document
// the backend contract and to let other backends be added (ADR 0001, 0009).
// Consumers should still declare the narrow interface they need (for example
// MessageStats) rather than depend on Store. Later tickets extend it by
// embedding further role interfaces (run state, #5).
type Store interface {
	MessageWriter
	MessageStats
	// Name and Check make a Store usable as a readiness checker.
	Name() string
	Check(ctx context.Context) error
	Close()
}
