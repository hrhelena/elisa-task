package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/hrhelena/elisa-task/internal/store"
)

// Error codes of GET /top-senders (ADR 0011).
const (
	CodeInvalidLimit = "invalid_limit"
)

// Limits of GET /top-senders?limit=N (ADR 0011).
const (
	DefaultTopSendersLimit = 10
	MaxTopSendersLimit     = 100
)

// topSendersTimeout bounds the ranking query (ticket #9).
const topSendersTimeout = 5 * time.Second

const invalidLimitMessage = "limit must be an integer between 1 and 100"

// SenderStats ranks senders by message count. It is implemented by the store
// (#4): count descending, then email ascending, excluding messages without a
// valid sender (ADR 0003, 0012).
type SenderStats interface {
	TopSenders(ctx context.Context, limit int) ([]store.SenderCount, error)
}

type topSendersResponse struct {
	Senders []senderJSON `json:"senders"`
}

type senderJSON struct {
	Email string `json:"email"`
	Count int64  `json:"count"`
}

// topSendersHandler serves GET /top-senders[?limit=N].
func topSendersHandler(stats SenderStats, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit, ok := parseLimit(r)
		if !ok {
			writeError(w, http.StatusBadRequest, CodeInvalidLimit, invalidLimitMessage)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), topSendersTimeout)
		defer cancel()
		senders, err := stats.TopSenders(ctx, limit)
		if err != nil {
			logger.LogAttrs(ctx, slog.LevelError, "top senders query failed",
				slog.Int("limit", limit),
				slog.Any("err", err),
				slog.String("request_id", RequestIDFromContext(ctx)),
			)
			writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the database is unavailable, retry later")
			return
		}
		resp := topSendersResponse{Senders: make([]senderJSON, len(senders))} // never null
		for i, s := range senders {
			resp.Senders[i] = senderJSON{Email: s.Email, Count: s.Count}
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

// parseLimit returns the limit query parameter, DefaultTopSendersLimit when
// it is absent, and ok == false when it is repeated, empty, not a decimal
// integer, or outside 1..MaxTopSendersLimit.
func parseLimit(r *http.Request) (limit int, ok bool) {
	values, present := r.URL.Query()["limit"]
	if !present {
		return DefaultTopSendersLimit, true
	}
	if len(values) != 1 || !isDigits(values[0]) {
		return 0, false
	}
	n, err := strconv.Atoi(values[0])
	if err != nil || n < 1 || n > MaxTopSendersLimit {
		return 0, false
	}
	return n, true
}

// isDigits reports whether s is a non-empty run of ASCII digits, rejecting the
// signs and spaces strconv.Atoi would otherwise accept or report vaguely.
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
