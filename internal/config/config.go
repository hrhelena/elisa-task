package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Environment variable names. They are exported so that docs and tests can
// refer to them without repeating string literals.
const (
	EnvHTTPAddr               = "HTTP_ADDR"
	EnvDatabaseURL            = "DATABASE_URL"
	EnvDataDir                = "DATA_DIR"
	EnvDatasetURL             = "DATASET_URL"
	EnvLogLevel               = "LOG_LEVEL"
	EnvShutdownTimeout        = "SHUTDOWN_TIMEOUT"
	EnvShutdownDrainDelay     = "SHUTDOWN_DRAIN_DELAY"
	EnvIngestWorkers          = "INGEST_WORKERS"
	EnvIngestBatchSize        = "INGEST_BATCH_SIZE"
	EnvIngestProgressInterval = "INGEST_PROGRESS_INTERVAL"
)

// Defaults that let the service run locally with no environment set.
const (
	DefaultHTTPAddr               = ":8080"
	DefaultDatabaseURL            = "postgres://enron:enron@localhost:5432/enron?sslmode=disable" //nolint:gosec // local dev default, not a secret.
	DefaultDataDir                = "./Dataset"
	DefaultDatasetURL             = "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz"
	DefaultLogLevel               = slog.LevelInfo
	DefaultShutdownTimeout        = 20 * time.Second
	DefaultShutdownDrainDelay     = 0
	DefaultIngestBatchSize        = 2000
	DefaultIngestProgressInterval = 5 * time.Second

	// MaxIngestBatchSize is the upper bound for INGEST_BATCH_SIZE.
	MaxIngestBatchSize = 50000
	// MaxIngestWorkers is the upper bound for INGEST_WORKERS.
	MaxIngestWorkers = 256
)

// Config holds every service setting. Later tickets only consume fields.
type Config struct {
	// HTTPAddr is the listen address of the HTTP server.
	HTTPAddr string
	// DatabaseURL is the Postgres connection string.
	DatabaseURL string
	// DataDir is the dataset root; the maildir is expected at DataDir/maildir.
	DataDir string
	// DatasetURL is where POST /fetch downloads the dataset archive from.
	DatasetURL string
	// LogLevel is the minimum level of the JSON logger.
	LogLevel slog.Level
	// ShutdownTimeout bounds draining in-flight requests and closing resources.
	ShutdownTimeout time.Duration
	// ShutdownDrainDelay is how long the service keeps serving, while
	// reporting not ready, after a shutdown signal and before it stops
	// accepting connections (so load balancers can take it out of rotation).
	ShutdownDrainDelay time.Duration
	// IngestWorkers is the number of parallel ingestion workers (1..256).
	IngestWorkers int
	// IngestBatchSize is the number of rows per insert batch (1..50000).
	IngestBatchSize int
	// IngestProgressInterval is how often ingestion progress is persisted.
	IngestProgressInterval time.Duration
}

// MaildirPath returns the directory the maildir is expected in.
func (c Config) MaildirPath() string {
	return filepath.Join(c.DataDir, "maildir")
}

// ErrInvalid is wrapped by every validation error returned from Load.
var ErrInvalid = errors.New("invalid configuration")

// LookupFunc looks up an environment variable, like os.LookupEnv.
type LookupFunc func(key string) (string, bool)

// Load reads the configuration from the process environment.
func Load() (Config, error) {
	return LoadFrom(os.LookupEnv)
}

// LoadFrom reads the configuration through lookup. Unset or empty variables
// take their default. Every invalid variable is reported, each error naming
// the variable, joined into one error that wraps ErrInvalid.
func LoadFrom(lookup LookupFunc) (Config, error) {
	p := parser{lookup: lookup}
	cfg := Config{
		HTTPAddr:               p.listenAddr(EnvHTTPAddr, DefaultHTTPAddr),
		DatabaseURL:            p.str(EnvDatabaseURL, DefaultDatabaseURL),
		DataDir:                p.str(EnvDataDir, DefaultDataDir),
		DatasetURL:             p.httpURL(EnvDatasetURL, DefaultDatasetURL),
		LogLevel:               p.level(EnvLogLevel, DefaultLogLevel),
		ShutdownTimeout:        p.duration(EnvShutdownTimeout, DefaultShutdownTimeout),
		ShutdownDrainDelay:     p.nonNegDuration(EnvShutdownDrainDelay, DefaultShutdownDrainDelay),
		IngestWorkers:          p.intRange(EnvIngestWorkers, min(runtime.NumCPU(), MaxIngestWorkers), 1, MaxIngestWorkers),
		IngestBatchSize:        p.intRange(EnvIngestBatchSize, DefaultIngestBatchSize, 1, MaxIngestBatchSize),
		IngestProgressInterval: p.duration(EnvIngestProgressInterval, DefaultIngestProgressInterval),
	}
	if len(p.errs) > 0 {
		return Config{}, fmt.Errorf("%w: %w", ErrInvalid, errors.Join(p.errs...))
	}
	return cfg, nil
}

// parser accumulates validation errors so that all of them are reported at once.
type parser struct {
	lookup LookupFunc
	errs   []error
}

func (p *parser) get(key string) (string, bool) {
	v, ok := p.lookup(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (p *parser) fail(key, value, reason string) {
	p.errs = append(p.errs, fmt.Errorf("%s=%q: %s", key, value, reason))
}

func (p *parser) str(key, def string) string {
	if v, ok := p.get(key); ok {
		return v
	}
	return def
}

// listenAddr accepts host:port with an optional host and a numeric port
// (0 picks a free port), e.g. ":8080" or "127.0.0.1:0".
func (p *parser) listenAddr(key, def string) string {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	_, port, err := net.SplitHostPort(v)
	if err != nil {
		p.fail(key, v, "must be host:port, e.g. :8080")
		return def
	}
	if n, err := strconv.Atoi(port); err != nil || n < 0 || n > 65535 {
		p.fail(key, v, "port must be a number from 0 to 65535")
		return def
	}
	return v
}

func (p *parser) httpURL(key, def string) string {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	u, err := url.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		p.fail(key, v, "must be an absolute http(s) URL")
		return def
	}
	return v
}

func (p *parser) level(key string, def slog.Level) slog.Level {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	switch strings.ToLower(v) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		p.fail(key, v, "must be one of debug, info, warn, error")
		return def
	}
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		p.fail(key, v, "must be a positive duration such as 5s or 1m")
		return def
	}
	return d
}

func (p *parser) nonNegDuration(key string, def time.Duration) time.Duration {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		p.fail(key, v, "must be a non-negative duration such as 0s or 5s")
		return def
	}
	return d
}

// intRange parses an integer in [lo, hi].
func (p *parser) intRange(key string, def, lo, hi int) int {
	v, ok := p.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	switch {
	case err != nil:
		p.fail(key, v, "must be an integer")
	case n < lo:
		p.fail(key, v, fmt.Sprintf("must be >= %d", lo))
	case n > hi:
		p.fail(key, v, fmt.Sprintf("must be <= %d", hi))
	default:
		return n
	}
	return def
}
