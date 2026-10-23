package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines returns the decoded JSON log records.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for line := range bytes.Lines(b.buf.Bytes()) {
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// find returns the log records with the given msg.
func (b *syncBuffer) find(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, rec := range b.lines(t) {
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// testAPI is an API instance with captured logs and its own registry.
type testAPI struct {
	handler http.Handler
	logs    *syncBuffer
	reg     *prometheus.Registry
}

// newTestAPI builds the handler like New does, with a DEBUG logger. extra may
// register additional routes on the router before the chain is built.
func newTestAPI(t *testing.T, ready *Readiness, extra func(*router)) *testAPI {
	t.Helper()
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := prometheus.NewRegistry()
	deps := Deps{Registry: reg, Readiness: ready}
	rt := newRouter(deps, logger)
	rt.routes(deps)
	if extra != nil {
		extra(rt)
	}
	return &testAPI{handler: rt.handler(), logs: logs, reg: reg}
}

func (a *testAPI) do(t *testing.T, method, target string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, nil)
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != contentTypeJSON {
		t.Errorf("Content-Type = %q, want %q", ct, contentTypeJSON)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %q: %v", rec.Body.String(), err)
	}
	return body
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	errObj, ok := decodeBody(t, rec)["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error object in %q", rec.Body.String())
	}
	if msg, _ := errObj["message"].(string); msg == "" {
		t.Errorf("error message is empty in %q", rec.Body.String())
	}
	code, _ := errObj["code"].(string)
	return code
}

// Scenario: Liveness.
func TestLiveness(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, nil, nil)
	rec := api.do(t, http.MethodGet, "/healthz", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got, want := rec.Body.String(), `{"status":"ok"}`+"\n"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
	decodeBody(t, rec)
}

type fakeChecker struct {
	name string
	err  error
	// block makes Check wait for its context to end.
	block bool
}

func (c fakeChecker) Name() string { return c.name }

func (c fakeChecker) Check(ctx context.Context) error {
	if c.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return c.err
}

// Scenario: Readiness with a failing dependency (plus the other readiness states).
func TestReadiness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		checkers     []Checker
		shuttingDown bool
		wantCode     int
		wantBody     string
	}{
		{
			name:     "no checkers",
			wantCode: http.StatusOK,
			wantBody: `{"status":"ready"}`,
		},
		{
			name:     "all checks pass",
			checkers: []Checker{fakeChecker{name: "postgres"}, fakeChecker{name: "disk"}},
			wantCode: http.StatusOK,
			wantBody: `{"status":"ready"}`,
		},
		{
			name:     "failing dependency",
			checkers: []Checker{fakeChecker{name: "postgres", err: errors.New("connection refused")}},
			wantCode: http.StatusServiceUnavailable,
			wantBody: `{"status":"not_ready","checks":{"postgres":"connection refused"}}`,
		},
		{
			name: "only failing checks are listed",
			checkers: []Checker{
				fakeChecker{name: "disk"},
				fakeChecker{name: "postgres", err: errors.New("connection refused")},
			},
			wantCode: http.StatusServiceUnavailable,
			wantBody: `{"status":"not_ready","checks":{"postgres":"connection refused"}}`,
		},
		{
			name:     "check exceeding the timeout",
			checkers: []Checker{fakeChecker{name: "postgres", block: true}},
			wantCode: http.StatusServiceUnavailable,
			wantBody: `{"status":"not_ready","checks":{"postgres":"context deadline exceeded"}}`,
		},
		{
			name:         "shutting down skips checks",
			checkers:     []Checker{fakeChecker{name: "postgres", block: true}},
			shuttingDown: true,
			wantCode:     http.StatusServiceUnavailable,
			wantBody:     `{"status":"shutting_down"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ready := NewReadiness(tt.checkers...)
			ready.timeout = 50 * time.Millisecond
			if tt.shuttingDown {
				ready.SetShuttingDown()
			}
			api := newTestAPI(t, ready, nil)

			start := time.Now()
			rec := api.do(t, http.MethodGet, "/readyz", nil)
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Errorf("readyz took %v", elapsed)
			}
			if rec.Code != tt.wantCode {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tt.wantBody {
				t.Errorf("body = %s, want %s", got, tt.wantBody)
			}
		})
	}
}

func TestReadinessDefaultTimeout(t *testing.T) {
	t.Parallel()

	if got := NewReadiness().timeout; got != 2*time.Second {
		t.Errorf("default check timeout = %v, want 2s", got)
	}
}

// Scenario: Request ID propagation.
func TestRequestIDPropagation(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, nil, nil)
	rec := api.do(t, http.MethodGet, "/healthz", http.Header{"X-Request-Id": {"abc-123"}})
	if got := rec.Header().Get("X-Request-ID"); got != "abc-123" {
		t.Errorf("X-Request-ID = %q, want abc-123", got)
	}
	logs := api.logs.find(t, "http request")
	if len(logs) != 1 {
		t.Fatalf("got %d access log lines, want 1", len(logs))
	}
	if logs[0]["request_id"] != "abc-123" {
		t.Errorf("access log request_id = %v, want abc-123", logs[0]["request_id"])
	}
}

func TestRequestIDInHandlerContext(t *testing.T) {
	t.Parallel()

	var seen string
	api := newTestAPI(t, nil, func(rt *router) {
		rt.read("GET /id", http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			seen = RequestIDFromContext(r.Context())
		}))
	})
	rec := api.do(t, http.MethodGet, "/id", http.Header{"X-Request-Id": {"Req-1"}})
	if seen != "Req-1" || rec.Header().Get(HeaderRequestID) != "Req-1" {
		t.Errorf("handler saw %q, response header %q, want Req-1", seen, rec.Header().Get(HeaderRequestID))
	}
	if got := RequestIDFromContext(context.Background()); got != "" {
		t.Errorf("RequestIDFromContext(empty) = %q, want empty", got)
	}
}

// Scenario: Malicious request ID is replaced.
func TestMaliciousRequestIDReplaced(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"<script>", "", "a b", "abc_123", strings.Repeat("a", 65), "id\x00"} {
		t.Run(bad, func(t *testing.T) {
			t.Parallel()

			api := newTestAPI(t, nil, nil)
			var hdr http.Header
			if bad != "" {
				hdr = http.Header{"X-Request-Id": {bad}}
			}
			rec := api.do(t, http.MethodGet, "/healthz", hdr)
			got := rec.Header().Get(HeaderRequestID)
			if got == bad || !validRequestID.MatchString(got) {
				t.Errorf("X-Request-ID = %q, want a newly generated valid ID", got)
			}
			if logs := api.logs.find(t, "http request"); len(logs) != 1 || logs[0]["request_id"] != got {
				t.Errorf("access log does not carry the generated ID %q: %v", got, logs)
			}
		})
	}

	t.Run("generated IDs are unique", func(t *testing.T) {
		t.Parallel()

		api := newTestAPI(t, nil, nil)
		a := api.do(t, http.MethodGet, "/healthz", nil).Header().Get(HeaderRequestID)
		b := api.do(t, http.MethodGet, "/healthz", nil).Header().Get(HeaderRequestID)
		if a == b {
			t.Errorf("two requests got the same generated ID %q", a)
		}
	})
}

// Scenario: Panic in a handler.
func TestPanicInHandler(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, nil, func(rt *router) {
		rt.read("GET /boom", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("kaboom")
		}))
	})

	rec := api.do(t, http.MethodGet, "/boom", http.Header{"X-Request-Id": {"panic-1"}})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if code := errorCode(t, rec); code != CodeInternal {
		t.Errorf("error code = %q, want %q", code, CodeInternal)
	}
	if strings.Contains(rec.Body.String(), "kaboom") {
		t.Errorf("panic value leaked to the client: %s", rec.Body.String())
	}

	// The server keeps serving subsequent requests.
	if next := api.do(t, http.MethodGet, "/healthz", nil); next.Code != http.StatusOK {
		t.Errorf("request after panic: status = %d, want 200", next.Code)
	}

	panics := api.logs.find(t, "panic in handler")
	if len(panics) != 1 {
		t.Fatalf("got %d panic log lines, want 1", len(panics))
	}
	p := panics[0]
	stack, _ := p["stack"].(string)
	if p["level"] != "ERROR" || p["panic"] != "kaboom" || p["request_id"] != "panic-1" || !strings.Contains(stack, "goroutine") {
		t.Errorf("unexpected panic log record: %v", p)
	}

	access := api.logs.find(t, "http request")
	if len(access) != 2 || access[0]["status"] != float64(500) || access[0]["route"] != "GET /boom" {
		t.Errorf("panicking request not access-logged as a 500: %v", access)
	}

	metrics := scrape(t, api)
	for _, want := range []string{
		"enron_http_panics_total 1",
		`enron_http_requests_total{code="500",method="GET",route="GET /boom"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics do not contain %q", want)
		}
	}
}

// TestPanicAfterResponseCommittedAbortsConnection: once the status line is
// committed (by Write, WriteHeader or a flush), a panic aborts the connection
// instead of appending an error body, and logs and metrics record the status
// the client actually received, marked as aborted.
func TestPanicAfterResponseCommittedAbortsConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		commit func(w http.ResponseWriter)
		status int
	}{
		{"write", func(w http.ResponseWriter) { _, _ = w.Write([]byte("partial")) }, http.StatusOK},
		{"write header", func(w http.ResponseWriter) { w.WriteHeader(http.StatusAccepted) }, http.StatusAccepted},
		{"flush", func(w http.ResponseWriter) { _ = http.NewResponseController(w).Flush() }, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := newTestAPI(t, nil, func(rt *router) {
				rt.read("GET /half", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					tt.commit(w)
					panic("late")
				}))
			})
			rec := httptest.NewRecorder()
			func() {
				defer func() {
					if p := recover(); p != http.ErrAbortHandler { //nolint:errorlint // identity check, as net/http does.
						t.Errorf("recovered %v, want http.ErrAbortHandler", p)
					}
				}()
				api.handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/half", nil))
				t.Error("expected the handler chain to abort")
			}()

			if rec.Code != tt.status || strings.Contains(rec.Body.String(), CodeInternal) {
				t.Errorf("client got %d %q, want %d without an error body", rec.Code, rec.Body.String(), tt.status)
			}
			if n := len(api.logs.find(t, "panic in handler")); n != 1 {
				t.Errorf("got %d panic log lines, want 1", n)
			}
			access := api.logs.find(t, "http request")
			if len(access) != 1 || access[0]["status"] != float64(tt.status) || access[0]["aborted"] != true {
				t.Errorf("access log = %v, want status %d and aborted=true", access, tt.status)
			}
			metrics := scrape(t, api)
			want := `enron_http_requests_total{code="` + strconv.Itoa(tt.status) + `",method="GET",route="GET /half"} 1`
			for _, w := range []string{want, "enron_http_panics_total 1"} {
				if !strings.Contains(metrics, w) {
					t.Errorf("metrics do not contain %q", w)
				}
			}
		})
	}
}

// TestPanicDropsHandlerHeaders: headers the handler set before panicking
// describe a response that never happened and must not reach the 500.
func TestPanicDropsHandlerHeaders(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, nil, func(rt *router) {
		rt.read("GET /boom", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			h := w.Header()
			h.Set("Content-Encoding", "gzip")
			h.Set("Location", "/elsewhere")
			h.Set("Cache-Control", "max-age=3600")
			h.Set("Content-Length", "12345")
			panic("kaboom")
		}))
	})
	rec := api.do(t, http.MethodGet, "/boom", http.Header{"X-Request-Id": {"hdr-1"}})
	if rec.Code != http.StatusInternalServerError || errorCode(t, rec) != CodeInternal {
		t.Fatalf("got %d %s, want 500 internal", rec.Code, rec.Body.String())
	}
	for _, k := range []string{"Content-Encoding", "Location", "Cache-Control", "Content-Length"} {
		if v := rec.Header().Get(k); v != "" {
			t.Errorf("handler header %s=%q leaked into the 500", k, v)
		}
	}
	if got := rec.Header().Get(HeaderRequestID); got != "hdr-1" {
		t.Errorf("X-Request-ID = %q, want it kept", got)
	}
}

func TestAbortHandlerPanicPassesThrough(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, nil, func(rt *router) {
		rt.read("GET /abort", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic(http.ErrAbortHandler)
		}))
	})
	defer func() {
		if p := recover(); p != http.ErrAbortHandler { //nolint:errorlint // identity check, as net/http does.
			t.Errorf("recovered %v, want http.ErrAbortHandler", p)
		}
		if n := len(api.logs.find(t, "panic in handler")); n != 0 {
			t.Errorf("deliberate abort was logged as a panic")
		}
	}()
	api.do(t, http.MethodGet, "/abort", nil)
	t.Error("expected the handler chain to abort")
}

// Scenario: Unknown route and wrong method.
func TestUnknownRouteAndWrongMethod(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, nil, nil)

	rec := api.do(t, http.MethodGet, "/nope", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /nope: status = %d, want 404", rec.Code)
	}
	if code := errorCode(t, rec); code != CodeNotFound {
		t.Errorf("GET /nope: error code = %q, want %q", code, CodeNotFound)
	}

	rec = api.do(t, http.MethodDelete, "/healthz", nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("DELETE /healthz: status = %d, want 405", rec.Code)
	}
	if code := errorCode(t, rec); code != CodeMethodNotAllowed {
		t.Errorf("DELETE /healthz: error code = %q, want %q", code, CodeMethodNotAllowed)
	}
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodGet) {
		t.Errorf("Allow = %q, want it to contain GET", allow)
	}

	// Unmatched requests share one bounded label set, logged at INFO.
	metrics := scrape(t, api)
	for _, want := range []string{
		`enron_http_requests_total{code="404",method="GET",route="unmatched"} 1`,
		`enron_http_requests_total{code="405",method="DELETE",route="unmatched"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics do not contain %q", want)
		}
	}
	for _, rec := range api.logs.find(t, "http request") {
		if rec["path"] == "/nope" && rec["level"] != "INFO" {
			t.Errorf("GET /nope logged at %v, want INFO", rec["level"])
		}
	}
}

// scrape returns the /metrics text exposition.
func scrape(t *testing.T, api *testAPI) string {
	t.Helper()
	rec := api.do(t, http.MethodGet, "/metrics", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics: status = %d", rec.Code)
	}
	return rec.Body.String()
}

// Scenario: Metrics exposed with bounded labels.
func TestMetricsExposedWithBoundedLabels(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, nil, func(rt *router) {
		rt.read("GET /items/{id}", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	})
	api.do(t, http.MethodGet, "/healthz", nil)
	api.do(t, http.MethodGet, "/healthz", nil)
	for _, p := range []string{"/items/1", "/items/2", "/items/3", "/random/a", "/random/b"} {
		api.do(t, http.MethodGet, p, nil)
	}
	api.do(t, "BREW", "/healthz", nil)

	metrics := scrape(t, api)
	for _, want := range []string{
		`enron_http_requests_total{code="200",method="GET",route="GET /healthz"} 2`,
		`enron_http_requests_total{code="200",method="GET",route="GET /items/{id}"} 3`,
		`enron_http_requests_total{code="404",method="GET",route="unmatched"} 2`,
		`enron_http_requests_total{code="405",method="OTHER",route="unmatched"} 1`,
		`enron_http_request_duration_seconds_count{method="GET",route="GET /healthz"} 2`,
		"# TYPE enron_http_panics_total counter",
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics do not contain %q", want)
		}
	}
	for _, raw := range []string{"/items/1", "/random/a", "BREW"} {
		if strings.Contains(metrics, raw) {
			t.Errorf("raw value %q leaked into a metric label", raw)
		}
	}
}

func TestNewTwiceOnOneRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	a := New(Deps{Registry: reg}, logger)
	b := New(Deps{Registry: reg}, logger)

	for _, h := range []http.Handler{a, b} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
	}
	rec := httptest.NewRecorder()
	a.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if want := `enron_http_requests_total{code="200",method="GET",route="GET /healthz"} 2`; !strings.Contains(rec.Body.String(), want) {
		t.Errorf("metrics do not contain %q: both handlers should share the collectors", want)
	}
	if warns := logs.find(t, "registering metric failed"); len(warns) != 0 {
		t.Errorf("unexpected registration warnings: %v", warns)
	}
}

func TestNewWithNilDeps(t *testing.T) {
	t.Parallel()

	h := New(Deps{}, slog.New(slog.DiscardHandler))
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s: status = %d, want 200", path, rec.Code)
		}
	}
}

func TestAccessLogFields(t *testing.T) {
	t.Parallel()

	api := newTestAPI(t, nil, func(rt *router) {
		rt.read("GET /hello/{name}", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTeapot)
			_, _ = w.Write([]byte("hello"))
		}))
	})
	api.do(t, http.MethodGet, "/hello/kay", http.Header{"X-Request-Id": {"log-1"}})
	api.do(t, http.MethodGet, "/healthz", nil)
	api.do(t, http.MethodGet, "/readyz", nil)
	api.do(t, http.MethodGet, "/metrics", nil)

	lines := api.logs.find(t, "http request")
	if len(lines) != 4 {
		t.Fatalf("got %d access log lines, want 4", len(lines))
	}
	got := lines[0]
	want := map[string]any{
		"level": "INFO", "method": "GET", "route": "GET /hello/{name}", "path": "/hello/kay",
		"status": float64(http.StatusTeapot), "bytes": float64(5), "request_id": "log-1",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("access log %s = %v, want %v", k, got[k], v)
		}
	}
	if d, ok := got["duration_ms"].(float64); !ok || d < 0 {
		t.Errorf("duration_ms = %v, want a non-negative number", got["duration_ms"])
	}
	for _, probe := range lines[1:] {
		if probe["level"] != "DEBUG" {
			t.Errorf("probe %v logged at %v, want DEBUG", probe["path"], probe["level"])
		}
	}
}

func TestChainOrder(t *testing.T) {
	t.Parallel()

	var order []string
	mw := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name+">")
				next.ServeHTTP(w, r)
				order = append(order, "<"+name)
			})
		}
	}
	h := chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { order = append(order, "h") }),
		mw("a"), mw("b"), mw("c"))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))

	if got, want := strings.Join(order, " "), "a> b> c> h <c <b <a"; got != want {
		t.Errorf("order = %q, want %q", got, want)
	}
}

// TestMutatingRoutesGetExtraMiddleware covers the ADR 0005 extension point:
// mutating routes get extra middleware (where authorization would go), read
// routes do not, and a middleware that replaces the request does not lose the
// route label.
func TestMutatingRoutesGetExtraMiddleware(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	guard := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Admin") != "yes" {
				writeError(w, http.StatusForbidden, "forbidden", "admin only")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, "admin")))
		})
	}
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusAccepted, map[string]any{"who": r.Context().Value(ctxKey{})})
	})
	api := newTestAPI(t, nil, func(rt *router) {
		rt.mutating = append(rt.mutating, guard)
		rt.mutate("POST /start", ok)
		rt.read("GET /status", ok)
	})

	if rec := api.do(t, http.MethodPost, "/start", nil); rec.Code != http.StatusForbidden {
		t.Errorf("POST /start without admin: status = %d, want 403", rec.Code)
	}
	if rec := api.do(t, http.MethodPost, "/start", http.Header{"X-Admin": {"yes"}}); rec.Code != http.StatusAccepted ||
		!strings.Contains(rec.Body.String(), `"admin"`) {
		t.Errorf("POST /start as admin: status = %d body %s, want 202 with identity", rec.Code, rec.Body.String())
	}
	if rec := api.do(t, http.MethodGet, "/status", nil); rec.Code != http.StatusAccepted {
		t.Errorf("GET /status: status = %d, want it to bypass the mutating middleware", rec.Code)
	}

	metrics := scrape(t, api)
	for _, want := range []string{
		`enron_http_requests_total{code="403",method="POST",route="POST /start"} 1`,
		`enron_http_requests_total{code="202",method="POST",route="POST /start"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics do not contain %q", want)
		}
	}
}

// TestGlobalMiddlewareSeesResolvedRoute covers the ADR 0005 slot for a
// global Authenticate middleware: requests it rejects are logged and counted
// under their route, and it may replace the request without losing it.
func TestGlobalMiddlewareSeesResolvedRoute(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	authenticate := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") == "" && !isQuietPath(r.URL.Path) { // probes stay open
				writeError(w, http.StatusUnauthorized, "unauthenticated", "missing credentials")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, "alice")))
		})
	}
	api := newTestAPI(t, nil, func(rt *router) {
		rt.global = append(rt.global, authenticate)
		rt.mutate("POST /start", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusAccepted, map[string]any{"who": r.Context().Value(ctxKey{})})
		}))
	})

	if rec := api.do(t, http.MethodPost, "/start", nil); rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated POST /start: status = %d, want 401", rec.Code)
	}
	if rec := api.do(t, http.MethodPost, "/start", http.Header{"Authorization": {"x"}}); rec.Code != http.StatusAccepted ||
		!strings.Contains(rec.Body.String(), "alice") {
		t.Errorf("authenticated POST /start: %d %s, want 202 with identity", rec.Code, rec.Body.String())
	}

	access := api.logs.find(t, "http request")
	if len(access) != 2 || access[0]["route"] != "POST /start" || access[0]["status"] != float64(401) {
		t.Errorf("rejected request not logged under its route: %v", access)
	}
	metrics := scrape(t, api)
	for _, want := range []string{
		`enron_http_requests_total{code="401",method="POST",route="POST /start"} 1`,
		`enron_http_requests_total{code="202",method="POST",route="POST /start"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Errorf("metrics do not contain %q", want)
		}
	}
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	t.Run("value", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		writeJSON(rec, http.StatusCreated, map[string]int{"n": 1})
		if rec.Code != http.StatusCreated || rec.Body.String() != "{\"n\":1}\n" {
			t.Errorf("got %d %q", rec.Code, rec.Body.String())
		}
		decodeBody(t, rec)
	})

	t.Run("error envelope", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		writeError(rec, http.StatusConflict, "ingestion_running", "an ingestion run is in progress")
		want := `{"error":{"code":"ingestion_running","message":"an ingestion run is in progress"}}` + "\n"
		if rec.Code != http.StatusConflict || rec.Body.String() != want {
			t.Errorf("got %d %q, want 409 %q", rec.Code, rec.Body.String(), want)
		}
		decodeBody(t, rec)
	})

	t.Run("unencodable value", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		writeJSON(rec, http.StatusOK, map[string]any{"f": func() {}})
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want 500", rec.Code)
		}
		if code := errorCode(t, rec); code != CodeInternal {
			t.Errorf("error code = %q, want %q", code, CodeInternal)
		}
	})
}

func TestResponseRecorderDefaultsAndUnwrap(t *testing.T) {
	t.Parallel()

	inner := httptest.NewRecorder()
	rec := newResponseRecorder(inner)
	if rec.Status() != http.StatusOK {
		t.Errorf("Status() before writing = %d, want 200", rec.Status())
	}
	rec.WriteHeader(http.StatusEarlyHints) // 1xx does not fix the final status.
	rec.WriteHeader(http.StatusNoContent)
	if rec.Status() != http.StatusNoContent {
		t.Errorf("Status() = %d, want 204", rec.Status())
	}
	if err := http.NewResponseController(rec).Flush(); err != nil {
		t.Errorf("Flush through Unwrap: %v", err)
	}
	if !inner.Flushed {
		t.Error("underlying writer was not flushed")
	}
}
