package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// CodeInvalidLimit is the error code of GET /top-senders for a bad limit
// (ADR 0011).
const CodeInvalidLimit = "invalid_limit"

// Limits of GET /top-senders?limit=N (ADR 0011).
const (
	DefaultTopSendersLimit = 10
	MaxTopSendersLimit     = 100
)

// topSendersTimeout bounds the ranking query (ticket #9).
const topSendersTimeout = 5 * time.Second

const invalidLimitMessage = "limit must be an integer between 1 and 100"

// SenderCount is one entry of the top-senders ranking.
type SenderCount struct {
	Email string `json:"email"`
	Count int64  `json:"count"`
}

// SenderStats ranks senders by message count: count descending, then email
// ascending, excluding messages without a valid sender (ADR 0003, 0012). The
// store (#4) backs it through an adapter in main, so this package does not
// depend on the storage packages (ADR 0009).
type SenderStats interface {
	TopSenders(ctx context.Context, limit int) ([]SenderCount, error)
}

type topSendersResponse struct {
	Senders []SenderCount `json:"senders"`
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
			level, msg := slog.LevelError, "top senders query failed"
			if r.Context().Err() != nil {
				// The client went away (or the server is closing the
				// connection): not a database failure, and nobody reads
				// the response. The 5s query timeout is not covered here,
				// since it cancels ctx, not r.Context().
				level, msg = slog.LevelDebug, "top senders query canceled by the client"
			}
			logger.LogAttrs(ctx, level, msg,
				slog.Int("limit", limit),
				slog.Any("err", err),
				slog.String("request_id", RequestIDFromContext(ctx)),
			)
			writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the database is unavailable, retry later")
			return
		}
		if senders == nil {
			senders = []SenderCount{} // never null
		}
		writeJSON(w, http.StatusOK, topSendersResponse{Senders: senders})
	})
}

// parseLimit returns the limit query parameter, DefaultTopSendersLimit when
// it is absent, and ok == false when it is malformed, repeated, empty, not a
// decimal integer, or outside 1..MaxTopSendersLimit.
func parseLimit(r *http.Request) (limit int, ok bool) {
	values, malformed := limitValues(r.URL.RawQuery)
	if malformed {
		return 0, false
	}
	if len(values) == 0 {
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

// limitValues returns the decoded values of every "limit" pair in rawQuery.
// It does not use r.URL.Query(), which silently drops the pairs it cannot
// parse (a bad percent escape, or a ";" since Go 1.17), so "?limit=abc;"
// would look like no limit at all. A limit pair that cannot be decoded, or
// a "&"-separated segment with a ";" in which any part has the key "limit",
// makes the whole parameter malformed. Broken pairs with other keys are
// ignored, as Query does.
func limitValues(rawQuery string) (values []string, malformed bool) {
	for segment := range strings.SplitSeq(rawQuery, "&") {
		if !hasLimitKey(segment) {
			continue
		}
		if strings.Contains(segment, ";") {
			malformed = true
			continue
		}
		_, raw, _ := strings.Cut(segment, "=")
		v, err := url.QueryUnescape(raw)
		if err != nil {
			malformed = true
			continue
		}
		values = append(values, v)
	}
	return values, malformed
}

// hasLimitKey reports whether any ";"-separated part of a query segment has
// the (decoded) key "limit".
func hasLimitKey(segment string) bool {
	for part := range strings.SplitSeq(segment, ";") {
		rawKey, _, _ := strings.Cut(part, "=")
		if key, err := url.QueryUnescape(rawKey); err == nil && key == "limit" {
			return true
		}
	}
	return false
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
