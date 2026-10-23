package fetch

import (
	"errors"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics are the fetch Prometheus metrics. They are always non-nil; with a
// nil registerer they are simply not exported.
type metrics struct {
	bytes    prometheus.Counter
	state    *prometheus.GaugeVec
	duration *prometheus.HistogramVec
	skipped  *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer, logger *slog.Logger) *metrics {
	m := &metrics{
		bytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "enron_fetch_bytes_total",
			Help: "Bytes of the dataset archive downloaded.",
		}),
		state: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "enron_fetch_state",
			Help: "Dataset state: 1 for the current state, 0 for the others.",
		}, []string{"state"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "enron_fetch_duration_seconds",
			Help: "Duration of finished fetches by result (ready or failed).",
			// 1s to about 2.3h: the real archive takes minutes.
			Buckets: prometheus.ExponentialBuckets(1, 2, 14),
		}, []string{"result"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "enron_fetch_entries_skipped_total",
			Help: "Archive entries skipped during extraction, by reason.",
		}, []string{"reason"}),
	}
	if reg != nil {
		m.bytes = register(reg, m.bytes, logger)
		m.state = register(reg, m.state, logger)
		m.duration = register(reg, m.duration, logger)
		m.skipped = register(reg, m.skipped, logger)
	}
	m.setState(StateAbsent)
	return m
}

// register registers c, reusing an identical collector that is already
// registered. Registration problems are logged, never fatal.
func register[C prometheus.Collector](reg prometheus.Registerer, c C, logger *slog.Logger) C {
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
	logger.Warn("registering fetch metric failed", slog.Any("err", err))
	return c
}

func (m *metrics) setState(s State) {
	for _, st := range States {
		v := 0.0
		if st == s {
			v = 1
		}
		m.state.WithLabelValues(string(st)).Set(v)
	}
}

func (m *metrics) observeDuration(result State, d time.Duration) {
	m.duration.WithLabelValues(string(result)).Observe(d.Seconds())
}
