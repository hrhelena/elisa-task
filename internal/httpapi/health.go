package httpapi

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultCheckTimeout bounds every readiness check.
const DefaultCheckTimeout = 2 * time.Second

// Checker is a readiness dependency check, such as a database ping.
// Check must honor ctx cancellation.
type Checker interface {
	// Name identifies the check in the /readyz response, e.g. "postgres".
	Name() string
	// Check returns nil when the dependency is usable.
	Check(ctx context.Context) error
}

// Readiness decides whether the service should receive traffic. It runs the
// registered checkers and reports not ready once shutdown has begun.
type Readiness struct {
	checkers     []Checker
	timeout      time.Duration
	shuttingDown atomic.Bool
}

// NewReadiness returns a Readiness that runs checkers on every /readyz
// request, each bounded by DefaultCheckTimeout.
func NewReadiness(checkers ...Checker) *Readiness {
	return &Readiness{checkers: checkers, timeout: DefaultCheckTimeout}
}

// SetShuttingDown makes /readyz fail from now on. It is safe to call
// concurrently with requests.
func (r *Readiness) SetShuttingDown() { r.shuttingDown.Store(true) }

// ShuttingDown reports whether SetShuttingDown has been called.
func (r *Readiness) ShuttingDown() bool { return r.shuttingDown.Load() }

// check runs all checkers concurrently and returns the error message of
// every failed one, keyed by checker name. It waits for every checker, which
// the per-check timeout bounds for checkers that honor their context.
func (r *Readiness) check(ctx context.Context) map[string]string {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	var (
		mu       sync.Mutex
		failures = map[string]string{}
		wg       sync.WaitGroup
	)
	for _, c := range r.checkers {
		wg.Go(func() {
			if err := c.Check(ctx); err != nil {
				mu.Lock()
				failures[c.Name()] = err.Error()
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return failures
}

type readyResponse struct {
	Status string            `json:"status" required:"true" enum:"ready,not_ready,shutting_down"`
	Checks map[string]string `json:"checks,omitempty" description:"Error of each failed check, by check name; only with not_ready."`
}

// healthResponse is the body of GET /healthz.
type healthResponse struct {
	Status string `json:"status" required:"true" enum:"ok"`
}

// handleHealthz is the liveness probe: it only proves the process serves HTTP.
func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

// handleReadyz is the readiness probe.
func (r *Readiness) handleReadyz(w http.ResponseWriter, req *http.Request) {
	if r.ShuttingDown() {
		writeJSON(w, http.StatusServiceUnavailable, readyResponse{Status: "shutting_down"})
		return
	}
	if failures := r.check(req.Context()); len(failures) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, readyResponse{Status: "not_ready", Checks: failures})
		return
	}
	writeJSON(w, http.StatusOK, readyResponse{Status: "ready"})
}
