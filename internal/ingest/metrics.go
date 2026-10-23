package ingest

import (
	"errors"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics are the ingestion Prometheus metrics. They are always non-nil;
// with a nil registerer they are simply not exported.
type metrics struct {
	files         prometheus.Counter
	messages      prometheus.Counter
	skipped       *prometheus.CounterVec
	fieldErrors   *prometheus.CounterVec
	batchDuration prometheus.Histogram
	running       prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer, logger *slog.Logger) *metrics {
	m := &metrics{
		files: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "enron_ingest_files_total",
			Help: "Dataset entries seen by ingestion runs.",
		}),
		messages: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "enron_ingest_messages_total",
			Help: "Messages parsed and written (inserted or already stored) by ingestion runs.",
		}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "enron_ingest_skipped_total",
			Help: "Dataset entries skipped as not a message, by reason.",
		}, []string{"reason"}),
		fieldErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "enron_ingest_field_errors_total",
			Help: "Stored messages with a field-level problem, by field error code.",
		}, []string{"field"}),
		batchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "enron_ingest_batch_duration_seconds",
			Help: "Duration of one batch insert attempt.",
			// 5ms to about 20s.
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 13),
		}),
		running: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "enron_ingest_running",
			Help: "1 while this process runs an ingestion, else 0.",
		}),
	}
	if reg != nil {
		m.files = register(reg, m.files, logger)
		m.messages = register(reg, m.messages, logger)
		m.skipped = register(reg, m.skipped, logger)
		m.fieldErrors = register(reg, m.fieldErrors, logger)
		m.batchDuration = register(reg, m.batchDuration, logger)
		m.running = register(reg, m.running, logger)
	}
	return m
}

// register registers c, reusing an identical collector that is already
// registered (for example when NewService is called twice with one
// registry). Any other failure is logged and leaves c working but unexported.
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
	logger.Warn("registering ingestion metric failed", slog.Any("err", err))
	return c
}
