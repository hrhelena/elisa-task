package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
)

// serverCancelHandler is the ctxwatch.Handler of an ingest lock connection.
// A canceled query is canceled on the server with a cancel request, so the
// session, and with it the lock, survives (ADR 0010). If the server has not
// stopped the query within deadlineDelay, the socket deadline ends it and pgx
// closes the connection.
//
// Unlike pgconn.CancelRequestContextWatcherHandler, the canceled call does
// not return until the server has acknowledged the cancel request by closing
// the cancel connection, which Postgres does after signaling the backend
// (libpq's PQcancel waits for the same thing). The pgx handler stops waiting
// as soon as the canceled query ends, and then only sleeps 100ms: if the
// query had completed on its own and the server processes the cancel request
// later than that, the request cancels the next query on the connection,
// such as the final "interrupted: shutdown" write (#51). Since every lock
// query goes through one connection, one at a time, waiting here protects
// all of them. A cancel request that arrives while the backend is idle is
// ignored by Postgres.
//
// The wait is bounded by the same deadline, and is cut short when the store
// closes the lock's socket (closing), so Close and Release never wait for it.
type serverCancelHandler struct {
	conn          *pgconn.PgConn
	deadlineDelay time.Duration
	closing       <-chan struct{}
	logger        *slog.Logger

	done chan struct{} // closed when the cancel request is answered or abandoned
}

// HandleCancel implements ctxwatch.Handler. It runs when the watched context
// is canceled, while the query may still run.
func (h *serverCancelHandler) HandleCancel(canceled context.Context) {
	deadline := time.Now().Add(h.deadlineDelay)
	_ = h.conn.Conn().SetDeadline(deadline)
	h.done = make(chan struct{})
	go func() {
		defer close(h.done)
		// The request outlives the canceled context: it ends at the deadline
		// or when the socket is closed.
		ctx, cancel := context.WithDeadline(context.WithoutCancel(canceled), deadline)
		defer cancel()
		go func() {
			select {
			case <-h.closing:
				cancel()
			case <-ctx.Done():
			}
		}()
		if err := h.conn.CancelRequest(ctx); err != nil && !isClosed(h.closing) {
			h.logger.Warn("ingest lock connection: cancel request not acknowledged", slog.Any("error", err))
		}
	}()
}

// HandleUnwatchAfterCancel implements ctxwatch.Handler. It runs when the
// canceled query has ended, before the next query can start, and waits until
// the cancel request can no longer hit a later query.
func (h *serverCancelHandler) HandleUnwatchAfterCancel() {
	<-h.done
	_ = h.conn.Conn().SetDeadline(time.Time{})
}

func isClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// withServerCancel returns cfg set up for an ingest lock connection: a
// canceled query is canceled on the server (serverCancelHandler) instead of
// pgx's default, which closes the connection and so would drop the
// cluster-wide lock whenever a run write times out. closing is closed when
// the store closes the connection's socket. Pool connections keep the
// default, so their queries end at their own deadline even when the server
// does not answer.
func withServerCancel(cfg *pgx.ConnConfig, closing <-chan struct{}, logger *slog.Logger) *pgx.ConnConfig {
	cfg.BuildContextWatcherHandler = func(c *pgconn.PgConn) ctxwatch.Handler {
		return &serverCancelHandler{conn: c, deadlineDelay: cancelDeadlineDelay, closing: closing, logger: logger}
	}
	return cfg
}
