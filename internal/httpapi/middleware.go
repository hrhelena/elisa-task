package httpapi

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"strings"
	"time"
)

// Middleware wraps a handler with cross-cutting behavior.
type Middleware func(http.Handler) http.Handler

// chain wraps h with mws. The first middleware is the outermost, so it sees
// the request first and the response last.
func chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// HeaderRequestID carries the request ID on requests and responses.
const HeaderRequestID = "X-Request-ID"

// validRequestID bounds what an incoming X-Request-ID may contain, so that a
// client cannot inject markup, control characters or huge values into logs
// and responses.
var validRequestID = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)

type requestInfoKey struct{}

// requestInfo is per-request state shared between the middleware layers. It
// is created by requestID, the outermost layer, and travels in the context, so
// inner layers may freely replace the *http.Request (r.WithContext) without
// hiding values from outer ones.
type requestInfo struct {
	id string
	// route is the registered pattern that matches, or "" if none does. Set by
	// resolveRoute, right after requestID.
	route string
	// quiet: the matched route logs its access line at DEBUG.
	quiet bool
}

func requestInfoFrom(ctx context.Context) *requestInfo {
	if info, ok := ctx.Value(requestInfoKey{}).(*requestInfo); ok {
		return info
	}
	return &requestInfo{}
}

// RequestIDFromContext returns the request ID set by the RequestID
// middleware, or "" if there is none.
func RequestIDFromContext(ctx context.Context) string {
	return requestInfoFrom(ctx).id
}

// requestID reuses a well-formed incoming X-Request-ID or generates a new one,
// and sets it on the response header and in the request context.
func requestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(HeaderRequestID)
			if !validRequestID.MatchString(id) {
				id = rand.Text() // 26 base32 characters, matches validRequestID.
			}
			w.Header().Set(HeaderRequestID, id)
			ctx := context.WithValue(r.Context(), requestInfoKey{}, &requestInfo{id: id})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// recoverer turns a handler panic into a 500 JSON error, logs it with its
// stack and counts it, so one bad request never takes the server down.
func recoverer(logger *slog.Logger, panics interface{ Inc() }) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := newResponseRecorder(w)
			defer func(ctx context.Context) {
				p := recover()
				if p == nil {
					return
				}
				if p == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does.
					panic(p) // Deliberate abort: let net/http close the connection quietly.
				}
				panics.Inc()
				logger.LogAttrs(ctx, slog.LevelError, "panic in handler",
					slog.String("panic", fmt.Sprint(p)),
					slog.String("stack", string(debug.Stack())),
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.String("request_id", RequestIDFromContext(ctx)),
				)
				if rec.wroteHeader {
					// Part of a response (or a flushed header) is already on the
					// wire; the only honest option left is to abort the connection.
					panic(http.ErrAbortHandler)
				}
				// Drop whatever the handler set (Content-Encoding, Location,
				// caching headers...): they describe a response that never
				// happened. Only the request ID survives.
				h := w.Header()
				id := h.Get(HeaderRequestID)
				clear(h)
				if id != "" {
					h.Set(HeaderRequestID, id)
				}
				writeError(w, http.StatusInternalServerError, CodeInternal, "internal server error")
			}(r.Context())
			next.ServeHTTP(rec, r)
		})
	}
}

// isQuietPath reports whether path is a probe, scrape or docs endpoint, whose access
// log lines are logged at DEBUG so that they do not drown the INFO log, even
// when the mux rejects the request (wrong method). Matched routes are quiet
// when their route table entry says so (route.quiet), which also covers the
// docs routes.
func isQuietPath(path string) bool {
	switch path {
	case "/healthz", "/readyz", "/metrics", "/openapi.json", "/docs":
		return true
	default:
		return strings.HasPrefix(path, "/docs/")
	}
}

// accessLog writes one line per request. For a request whose handler panics
// it logs what the client receives (see responseRecorder.outcome) while the
// panic propagates.
func accessLog(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := newResponseRecorder(w)
			info := requestInfoFrom(r.Context())
			completed := false
			defer func(ctx context.Context) {
				status, aborted := rec.outcome(completed)
				level := slog.LevelInfo
				if info.quiet || isQuietPath(r.URL.Path) {
					level = slog.LevelDebug
				}
				attrs := []slog.Attr{
					slog.String("method", r.Method),
					slog.String("route", info.route),
					slog.String("path", r.URL.Path),
					slog.Int("status", status),
					slog.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000),
					slog.Int64("bytes", rec.bytes),
					slog.String("request_id", info.id),
				}
				if aborted {
					attrs = append(attrs, slog.Bool("aborted", true))
				}
				logger.LogAttrs(ctx, level, "http request", attrs...)
			}(r.Context())
			next.ServeHTTP(rec, r)
			completed = true
		})
	}
}

// instrument records request count and latency per method, route pattern and
// status code. Like accessLog, it records what the client of a panicking
// request receives (see responseRecorder.outcome).
func instrument(m *httpMetrics) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := newResponseRecorder(w)
			info := requestInfoFrom(r.Context())
			completed := false
			defer func() {
				status, _ := rec.outcome(completed)
				m.observe(r.Method, info.route, status, time.Since(start))
			}()
			next.ServeHTTP(rec, r)
			completed = true
		})
	}
}

// responseRecorder captures the status code and body size of a response.
// Unwrap lets http.ResponseController reach the underlying writer (flush,
// deadlines, hijack).
type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{ResponseWriter: w}
}

func (r *responseRecorder) WriteHeader(code int) {
	if !r.wroteHeader && code >= http.StatusOK {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	if err != nil {
		return n, fmt.Errorf("writing response: %w", err)
	}
	return n, nil
}

// Status returns the status sent, or 200 if the handler wrote nothing (which
// net/http turns into an empty 200 response).
func (r *responseRecorder) Status() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// FlushError marks the response as committed (a flush sends the header) and
// flushes the underlying writer. http.ResponseController finds this method
// before Unwrap, so recorders further out see the flush too.
func (r *responseRecorder) FlushError() error {
	if !r.wroteHeader {
		r.status = http.StatusOK
		r.wroteHeader = true
	}
	if err := http.NewResponseController(r.ResponseWriter).Flush(); err != nil {
		return fmt.Errorf("flushing response: %w", err)
	}
	return nil
}

// outcome returns the status the client receives and whether the response
// was aborted. completed is false when the handler panicked: recoverer then
// sends a 500 if nothing was committed yet, or aborts the connection after
// the committed status otherwise.
func (r *responseRecorder) outcome(completed bool) (status int, aborted bool) {
	switch {
	case completed:
		return r.Status(), false
	case r.wroteHeader:
		return r.status, true
	default:
		return http.StatusInternalServerError, false
	}
}

func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// fallbackWriter rewrites the plain-text 404 and 405 responses of
// http.ServeMux into the JSON error envelope. The mux sets the Allow header
// before writing a 405, so it is preserved.
type fallbackWriter struct {
	http.ResponseWriter
	rewritten bool
}

func (f *fallbackWriter) WriteHeader(code int) {
	switch code {
	case http.StatusNotFound:
		f.rewritten = true
		writeError(f.ResponseWriter, code, CodeNotFound, "no such endpoint")
	case http.StatusMethodNotAllowed:
		f.rewritten = true
		writeError(f.ResponseWriter, code, CodeMethodNotAllowed, "method not allowed for this endpoint")
	default:
		f.ResponseWriter.WriteHeader(code)
	}
}

func (f *fallbackWriter) Write(b []byte) (int, error) {
	if f.rewritten {
		return len(b), nil // Drop the mux's plain-text body.
	}
	n, err := f.ResponseWriter.Write(b)
	if err != nil {
		return n, fmt.Errorf("writing response: %w", err)
	}
	return n, nil
}

func (f *fallbackWriter) Unwrap() http.ResponseWriter { return f.ResponseWriter }
