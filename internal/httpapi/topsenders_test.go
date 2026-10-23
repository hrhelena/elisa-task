package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hrhelena/elisa-task/internal/store"
)

// fakeSenderStats is a SenderStats that returns a fixed result and records
// its calls.
type fakeSenderStats struct {
	senders []store.SenderCount
	err     error

	mu       sync.Mutex
	limits   []int
	deadline time.Duration // time left on the context of the last call; 0 if none
}

func (f *fakeSenderStats) TopSenders(ctx context.Context, limit int) ([]store.SenderCount, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limits = append(f.limits, limit)
	f.deadline = 0
	if d, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(d)
	}
	if f.err != nil {
		return nil, f.err
	}
	// Like the store: never more than limit.
	return f.senders[:min(limit, len(f.senders))], nil
}

func (f *fakeSenderStats) calls() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.limits)
}

// newSendersAPI returns the API with stats behind GET /top-senders.
func newSendersAPI(t *testing.T, stats SenderStats) *testAPI {
	t.Helper()
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := prometheus.NewRegistry()
	return &testAPI{handler: New(Deps{Registry: reg, Senders: stats}, logger), logs: logs, reg: reg}
}

type topSendersBody struct {
	Senders []struct {
		Email string `json:"email"`
		Count int64  `json:"count"`
	} `json:"senders"`
}

func decodeSenders(t *testing.T, body string) []store.SenderCount {
	t.Helper()
	var resp topSendersBody
	dec := json.NewDecoder(strings.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decoding %q: %v", body, err)
	}
	if resp.Senders == nil {
		t.Fatalf("senders is null or missing in %q", body)
	}
	out := make([]store.SenderCount, len(resp.Senders))
	for i, s := range resp.Senders {
		out[i] = store.SenderCount{Email: s.Email, Count: s.Count}
	}
	return out
}

// rankedSenders returns n senders in store order, the first with count
// first and each following one with one message fewer (at least 1).
func rankedSenders(n int, first int64) []store.SenderCount {
	out := make([]store.SenderCount, n)
	for i := range out {
		out[i] = store.SenderCount{Email: fmt.Sprintf("sender%02d@enron.com", i), Count: max(first-int64(i), 1)}
	}
	return out
}

// Scenario: Top 10 by default (handler half: the default limit, the shape and
// the order are passed through; the ranking itself is the store's, covered by
// TestBinaryTopSenders in cmd/enron-service).
func TestTopSendersDefaultsToTen(t *testing.T) {
	t.Parallel()
	all := rankedSenders(12, 5)
	all[0].Email = "kay.mann@enron.com"
	stats := &fakeSenderStats{senders: all}
	api := newSendersAPI(t, stats)

	rec := api.do(t, http.MethodGet, "/top-senders", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d %q, want 200", rec.Code, rec.Body.String())
	}
	decodeBody(t, rec) // Content-Type check
	got := decodeSenders(t, rec.Body.String())
	if !slices.Equal(got, all[:10]) {
		t.Errorf("senders = %v, want %v", got, all[:10])
	}
	if got[0] != (store.SenderCount{Email: "kay.mann@enron.com", Count: 5}) {
		t.Errorf("first entry = %v, want kay.mann@enron.com with 5", got[0])
	}
	if calls := stats.calls(); !slices.Equal(calls, []int{10}) {
		t.Errorf("store called with limits %v, want [10]", calls)
	}
}

// The exact wire format of ADR 0011.
func TestTopSendersResponseShape(t *testing.T) {
	t.Parallel()
	stats := &fakeSenderStats{senders: []store.SenderCount{
		{Email: "kay.mann@enron.com", Count: 16735},
		{Email: "vince.kaminski@enron.com", Count: 14368},
	}}
	rec := newSendersAPI(t, stats).do(t, http.MethodGet, "/top-senders", nil)
	want := `{"senders":[{"email":"kay.mann@enron.com","count":16735},{"email":"vince.kaminski@enron.com","count":14368}]}` + "\n"
	if rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Errorf("GET /top-senders = %d %q, want 200 %q", rec.Code, rec.Body.String(), want)
	}
}

func TestTopSendersValidLimitsArePassedToTheStore(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		query string
		want  int
	}{
		{"", 10},
		{"?limit=1", 1},
		{"?limit=10", 10},
		{"?limit=100", 100},
		{"?limit=007", 7},
		{"?other=x&limit=3", 3},
	} {
		t.Run(tc.query, func(t *testing.T) {
			t.Parallel()
			stats := &fakeSenderStats{senders: rankedSenders(150, 200)}
			rec := newSendersAPI(t, stats).do(t, http.MethodGet, "/top-senders"+tc.query, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d %q, want 200", rec.Code, rec.Body.String())
			}
			if calls := stats.calls(); !slices.Equal(calls, []int{tc.want}) {
				t.Errorf("store called with limits %v, want [%d]", calls, tc.want)
			}
			if got := decodeSenders(t, rec.Body.String()); len(got) != tc.want {
				t.Errorf("%d entries, want %d", len(got), tc.want)
			}
		})
	}
}

// Scenario Outline: Invalid limit (0, 101, abc, -5), plus other malformed
// values. The store is never called.
func TestTopSendersInvalidLimit(t *testing.T) {
	t.Parallel()
	for _, query := range []string{
		"limit=0",
		"limit=101",
		"limit=abc",
		"limit=-5",
		"limit=",
		"limit",
		"limit=%2B5", // "+5"
		"limit=%205", // " 5"
		"limit=5%20",
		"limit=1.5",
		"limit=1e1",
		"limit=0x10",
		"limit=99999999999999999999",
		"limit=5&limit=6",
		"limit=5&limit=5",
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			stats := &fakeSenderStats{}
			rec := newSendersAPI(t, stats).do(t, http.MethodGet, "/top-senders?"+query, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %q, want 400", rec.Code, rec.Body.String())
			}
			if code := errorCode(t, rec); code != CodeInvalidLimit {
				t.Errorf("error code = %q, want %q", code, CodeInvalidLimit)
			}
			want := `{"error":{"code":"invalid_limit","message":"limit must be an integer between 1 and 100"}}` + "\n"
			if rec.Body.String() != want {
				t.Errorf("body = %q, want %q", rec.Body.String(), want)
			}
			if calls := stats.calls(); len(calls) != 0 {
				t.Errorf("store called with %v for an invalid limit", calls)
			}
		})
	}
}

// Scenario: Empty database. Both an empty and a nil store result are
// rendered as an empty array, never null.
func TestTopSendersEmptyDatabase(t *testing.T) {
	t.Parallel()
	for name, senders := range map[string][]store.SenderCount{"empty": {}, "nil": nil} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := newSendersAPI(t, &fakeSenderStats{senders: senders}).do(t, http.MethodGet, "/top-senders", nil)
			if want := `{"senders":[]}` + "\n"; rec.Code != http.StatusOK || rec.Body.String() != want {
				t.Errorf("GET /top-senders = %d %q, want 200 %q", rec.Code, rec.Body.String(), want)
			}
		})
	}
}

// Scenario: Database unavailable. The cause is logged with the request ID but
// not exposed to the client.
func TestTopSendersDatabaseUnavailable(t *testing.T) {
	t.Parallel()
	for name, err := range map[string]error{
		"query error": errors.New("querying top senders: dial tcp 10.0.0.5:5432: connect: connection refused"),
		"timeout":     fmt.Errorf("querying top senders: %w", context.DeadlineExceeded),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			api := newSendersAPI(t, &fakeSenderStats{err: err})
			rec := api.do(t, http.MethodGet, "/top-senders", http.Header{HeaderRequestID: {"req-42"}})
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d %q, want 503", rec.Code, rec.Body.String())
			}
			if code := errorCode(t, rec); code != CodeUnavailable {
				t.Errorf("error code = %q, want %q", code, CodeUnavailable)
			}
			if strings.Contains(rec.Body.String(), "10.0.0.5") || strings.Contains(rec.Body.String(), "deadline") {
				t.Errorf("response leaks the internal error: %q", rec.Body.String())
			}
			logged := api.logs.find(t, "top senders query failed")
			if len(logged) != 1 || logged[0]["level"] != "ERROR" || logged[0]["request_id"] != "req-42" ||
				logged[0]["err"] != err.Error() {
				t.Errorf("want one ERROR log with request_id and err, got %v", logged)
			}
		})
	}
}

// The query runs under a 5s timeout derived from the request context.
func TestTopSendersQueryTimeout(t *testing.T) {
	t.Parallel()
	stats := &fakeSenderStats{senders: []store.SenderCount{}}
	newSendersAPI(t, stats).do(t, http.MethodGet, "/top-senders", nil)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	if stats.deadline <= 0 || stats.deadline > topSendersTimeout {
		t.Errorf("time left on the query context = %v, want within (0, %v]", stats.deadline, topSendersTimeout)
	}
}

func TestTopSendersRouting(t *testing.T) {
	t.Parallel()

	t.Run("other methods get 405", func(t *testing.T) {
		t.Parallel()
		rec := newSendersAPI(t, &fakeSenderStats{}).do(t, http.MethodPost, "/top-senders", nil)
		if rec.Code != http.StatusMethodNotAllowed || errorCode(t, rec) != CodeMethodNotAllowed {
			t.Errorf("POST /top-senders = %d %q, want 405 method_not_allowed", rec.Code, rec.Body.String())
		}
	})

	t.Run("no route without a SenderStats", func(t *testing.T) {
		t.Parallel()
		rec := newSendersAPI(t, nil).do(t, http.MethodGet, "/top-senders", nil)
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET /top-senders without Senders = %d, want 404", rec.Code)
		}
	})

	t.Run("metrics use the route pattern", func(t *testing.T) {
		t.Parallel()
		api := newSendersAPI(t, &fakeSenderStats{senders: []store.SenderCount{}})
		api.do(t, http.MethodGet, "/top-senders?limit=5", nil)
		api.do(t, http.MethodGet, "/top-senders?limit=0", nil)
		body := api.do(t, http.MethodGet, "/metrics", nil).Body.String()
		for _, want := range []string{
			`enron_http_requests_total{code="200",method="GET",route="GET /top-senders"} 1`,
			`enron_http_requests_total{code="400",method="GET",route="GET /top-senders"} 1`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("metrics missing %q", want)
			}
		}
	})
}
