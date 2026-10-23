package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// routeUnmatched is the route label of requests no pattern matched (404/405).
const routeUnmatched = "unmatched"

// httpMetrics are the HTTP server metrics. Labels use the route pattern
// (r.Pattern), never the raw path, and a normalized method, so that their
// cardinality is bounded no matter what clients send.
type httpMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	panics   prometheus.Counter
}

func newHTTPMetrics(reg prometheus.Registerer, logger *slog.Logger) *httpMetrics {
	return &httpMetrics{
		requests: register(reg, logger, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "enron_http_requests_total",
			Help: "HTTP requests served, by method, route pattern and status code.",
		}, []string{"method", "route", "code"})),
		duration: register(reg, logger, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "enron_http_request_duration_seconds",
			Help:    "HTTP request latency, by method and route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"})),
		panics: register(reg, logger, prometheus.NewCounter(prometheus.CounterOpts{
			Name: "enron_http_panics_total",
			Help: "Handler panics recovered by the HTTP server.",
		})),
	}
}

// register registers c, reusing an identical collector that is already
// registered (for example when New is called twice with one registry). Any
// other failure is logged and leaves c working but unexported.
func register[C prometheus.Collector](reg prometheus.Registerer, logger *slog.Logger, c C) C {
	err := reg.Register(c)
	if err == nil {
		return c
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(C); ok {
			return existing
		}
	}
	logger.Warn("registering metric failed", slog.Any("err", err))
	return c
}

func (m *httpMetrics) observe(method, pattern string, status int, d time.Duration) {
	method = normalizeMethod(method)
	route := pattern
	if route == "" {
		route = routeUnmatched
	}
	m.requests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(method, route).Observe(d.Seconds())
}

// normalizeMethod maps non-standard methods to "OTHER" to bound cardinality.
func normalizeMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}

func metricsHandler(reg *prometheus.Registry, logger *slog.Logger) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorLog:      slog.NewLogLogger(logger.Handler(), slog.LevelError),
		ErrorHandling: promhttp.ContinueOnError,
	})
}
