package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

// Deps are the dependencies of the HTTP API. Feature dependencies are small
// interfaces defined in this package (ADR 0009) and are optional: a nil
// feature dependency means its routes are not registered.
type Deps struct {
	// Registry receives the HTTP metrics and is served at GET /metrics.
	// Nil means a fresh, private registry.
	Registry *prometheus.Registry
	// Readiness backs GET /readyz. Nil means no dependency checks.
	Readiness *Readiness

	// Feature dependencies (#8 Ingester and StatsReader, #9 top senders,
	// #10 Fetcher) are added here and registered in routes.
}

// New returns the service's HTTP handler: every route, wrapped in the
// middleware chain.
func New(deps Deps, logger *slog.Logger) http.Handler {
	rt := newRouter(deps, logger)
	rt.routes(deps)
	return rt.handler()
}

// router groups the mux with the middleware every route gets.
type router struct {
	mux       *http.ServeMux
	logger    *slog.Logger
	metrics   *httpMetrics
	registry  *prometheus.Registry
	readiness *Readiness
	// patterns are the registered route patterns, the only values allowed in
	// the route log field and metric label.
	patterns map[string]bool
	// global is applied to every route, inside request ID, route
	// resolution, recovery, access log and metrics. ADR 0005: a global
	// Authenticate middleware (which puts the caller identity in the request
	// context) is appended here when auth is introduced.
	global []Middleware
	// mutating is applied to routes that trigger work or change state
	// (POST /fetch, POST /start). ADR 0005: an Authorize middleware (for
	// example admin-only) is appended here when auth is introduced.
	mutating []Middleware
}

func newRouter(deps Deps, logger *slog.Logger) *router {
	reg := deps.Registry
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	ready := deps.Readiness
	if ready == nil {
		ready = NewReadiness()
	}
	return &router{
		mux:       http.NewServeMux(),
		logger:    logger,
		metrics:   newHTTPMetrics(reg, logger),
		registry:  reg,
		readiness: ready,
		patterns:  map[string]bool{},
	}
}

// routes registers every endpoint. Patterns always include the method, so the
// mux answers other methods with 405 and an Allow header.
func (rt *router) routes(_ Deps) {
	rt.read("GET /healthz", http.HandlerFunc(handleHealthz))
	rt.read("GET /readyz", http.HandlerFunc(rt.readiness.handleReadyz))
	rt.read("GET /metrics", metricsHandler(rt.registry, rt.logger))

	// Feature routes follow this shape (nil dependency, no route):
	//
	//	if deps.Ingester != nil {
	//		rt.mutate("POST /start", startHandler(deps.Ingester))
	//	}
}

// read registers a route that only reads state.
func (rt *router) read(pattern string, h http.Handler) {
	rt.handle(pattern, h)
}

// mutate registers a route that triggers work or changes state; it gets the
// extra mutating middleware (the place for authorization, ADR 0005).
func (rt *router) mutate(pattern string, h http.Handler) {
	rt.handle(pattern, chain(h, rt.mutating...))
}

func (rt *router) handle(pattern string, h http.Handler) {
	rt.patterns[pattern] = true
	rt.mux.Handle(pattern, h)
}

// handler wraps the mux in the global middleware chain, outermost first.
// The route is resolved right after the request ID, before anything that may
// reject a request, so every log line and metric carries it.
func (rt *router) handler() http.Handler {
	mws := []Middleware{
		requestID(),
		rt.resolveRoute(),
		recoverer(rt.logger, rt.metrics.panics),
		accessLog(rt.logger),
		instrument(rt.metrics),
	}
	// rt.global runs after logging and metrics, so requests it rejects are
	// still logged and counted under their route.
	mws = append(mws, rt.global...)
	return chain(rt.serveMux(), mws...)
}

// resolveRoute records the registered pattern that matches the request for
// accessLog, instrument and serveMux. Unmatched requests keep route "".
// mux.Handler only looks the pattern up; it has no side effects.
func (rt *router) resolveRoute() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, pattern := rt.mux.Handler(r); rt.patterns[pattern] {
				requestInfoFrom(r.Context()).route = pattern
			}
			next.ServeHTTP(w, r)
		})
	}
}

// serveMux serves the mux, turning its plain-text 404 and 405 responses into
// the JSON envelope.
func (rt *router) serveMux() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestInfoFrom(r.Context()).route == "" {
			w = &fallbackWriter{ResponseWriter: w}
		}
		rt.mux.ServeHTTP(w, r)
	})
}
