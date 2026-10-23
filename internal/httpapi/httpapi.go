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

	// Fetcher backs POST /fetch (#10). Nil means the route is not registered.
	Fetcher Fetcher
	// Senders backs GET /top-senders (#9). Nil means the route is not registered.
	Senders SenderStats

	// Ingester backs POST /start and GET /status (#8). Nil means neither
	// route is registered.
	Ingester Ingester
	// Datasets fills the dataset section of GET /status (#10). Nil means
	// the section is omitted.
	Datasets DatasetReporter

	// Version is the build version, shown as info.version in the OpenAPI
	// document (ADR 0016). Empty means "dev".
	Version string
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
	// quiet are the registered patterns whose access log lines are written at
	// DEBUG (probes, scrape, docs).
	quiet map[string]bool
	// buildSpec generates the OpenAPI document from the documented routes.
	// Tests replace it to force a failure.
	buildSpec func(version string, routes []route) ([]byte, error)
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
		quiet:     map[string]bool{},
		buildSpec: buildSpec,
	}
}

// route is one entry of the route table, which drives both the mux and the
// OpenAPI document (ADR 0016).
type route struct {
	method, path string // the mux pattern is method + " " + path
	handler      http.Handler
	// mutating routes trigger work or change state; they get rt.mutating
	// (the place for authorization, ADR 0005).
	mutating bool
	// quiet routes log their access line at DEBUG (probes, scrape, docs).
	quiet bool
	// documented routes appear in the OpenAPI document, described by op.
	// Only the docs routes themselves are undocumented.
	documented bool
	op         *operation
}

func (r route) pattern() string { return r.method + " " + r.path }

// routes registers every endpoint. Patterns always include the method, so the
// mux answers other methods with 405 and an Allow header.
func (rt *router) routes(deps Deps) {
	api := rt.apiRoutes(deps)
	version := deps.Version
	if version == "" {
		version = "dev"
	}
	spec, err := rt.buildSpec(version, api)
	if err != nil {
		rt.logger.Error(specFailedMessage, slog.Any("err", err))
		spec = nil
	}
	for _, r := range append(api, docsRoutes(spec)...) {
		rt.register(r)
	}
}

// apiRoutes is the route table of the API itself. A route whose dependency
// is nil is left out, so it is neither served nor documented.
func (rt *router) apiRoutes(deps Deps) []route {
	routes := []route{
		{method: http.MethodGet, path: "/healthz", handler: http.HandlerFunc(handleHealthz),
			quiet: true, documented: true, op: healthzOperation()},
		{method: http.MethodGet, path: "/readyz", handler: http.HandlerFunc(rt.readiness.handleReadyz),
			quiet: true, documented: true, op: readyzOperation()},
		{method: http.MethodGet, path: "/metrics", handler: metricsHandler(rt.registry, rt.logger),
			quiet: true, documented: true, op: metricsOperation()},
	}
	if deps.Fetcher != nil {
		routes = append(routes, route{method: http.MethodPost, path: "/fetch",
			handler: fetchHandler(deps.Fetcher, rt.logger), mutating: true, documented: true, op: fetchOperation()})
	}
	if deps.Senders != nil {
		routes = append(routes, route{method: http.MethodGet, path: "/top-senders",
			handler: topSendersHandler(deps.Senders, rt.logger), documented: true, op: topSendersOperation()})
	}
	if deps.Ingester != nil {
		routes = append(routes,
			route{method: http.MethodPost, path: "/start",
				handler: startHandler(deps.Ingester, rt.logger), mutating: true, documented: true, op: startOperation()},
			route{method: http.MethodGet, path: "/status",
				handler: statusHandler(deps.Ingester, deps.Datasets, rt.logger), documented: true, op: statusOperation()},
		)
	}
	return routes
}

// register adds a route table entry to the mux.
func (rt *router) register(r route) {
	if r.mutating {
		rt.mutate(r.pattern(), r.handler)
	} else {
		rt.read(r.pattern(), r.handler)
	}
	if r.quiet {
		rt.quiet[r.pattern()] = true
	}
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
				info := requestInfoFrom(r.Context())
				info.route = pattern
				info.quiet = rt.quiet[pattern]
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
