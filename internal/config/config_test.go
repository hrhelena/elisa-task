package config_test

import (
	"errors"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hrhelena/elisa-task/internal/config"
)

func lookupMap(env map[string]string) config.LookupFunc {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

// TestLoadDefaults covers the config half of "Defaults allow a local run".
func TestLoadDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(lookupMap(nil))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	want := config.Config{
		HTTPAddr:               ":8080",
		DatabaseURL:            config.DefaultDatabaseURL,
		DataDir:                "./Dataset",
		DatasetURL:             "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz",
		LogLevel:               slog.LevelInfo,
		ShutdownTimeout:        20 * time.Second,
		ShutdownDrainDelay:     0,
		IngestWorkers:          runtime.NumCPU(),
		IngestBatchSize:        2000,
		IngestProgressInterval: 5 * time.Second,
	}
	if cfg != want {
		t.Errorf("defaults:\n got  %+v\n want %+v", cfg, want)
	}
	if !strings.HasPrefix(cfg.DatabaseURL, "postgres://enron:enron@localhost:5432/enron") {
		t.Errorf("default DATABASE_URL = %q", cfg.DatabaseURL)
	}
	if got, want := cfg.MaildirPath(), filepath.Join("Dataset", "maildir"); got != want {
		t.Errorf("MaildirPath() = %q, want %q", got, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(lookupMap(map[string]string{
		"HTTP_ADDR":                "127.0.0.1:9000",
		"DATABASE_URL":             "postgres://db.example:5432/x?sslmode=require",
		"DATA_DIR":                 "/data",
		"DATASET_URL":              "http://mirror.example/enron.tgz",
		"LOG_LEVEL":                "DEBUG",
		"SHUTDOWN_TIMEOUT":         "3s",
		"SHUTDOWN_DRAIN_DELAY":     "5s",
		"INGEST_WORKERS":           "7",
		"INGEST_BATCH_SIZE":        "50000",
		"INGEST_PROGRESS_INTERVAL": "250ms",
	}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	want := config.Config{
		HTTPAddr:               "127.0.0.1:9000",
		DatabaseURL:            "postgres://db.example:5432/x?sslmode=require",
		DataDir:                "/data",
		DatasetURL:             "http://mirror.example/enron.tgz",
		LogLevel:               slog.LevelDebug,
		ShutdownTimeout:        3 * time.Second,
		ShutdownDrainDelay:     5 * time.Second,
		IngestWorkers:          7,
		IngestBatchSize:        50000,
		IngestProgressInterval: 250 * time.Millisecond,
	}
	if cfg != want {
		t.Errorf("overrides:\n got  %+v\n want %+v", cfg, want)
	}
}

func TestLoadEmptyValueUsesDefault(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(lookupMap(map[string]string{"HTTP_ADDR": "  ", "INGEST_WORKERS": ""}))
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.IngestWorkers != runtime.NumCPU() {
		t.Errorf("empty values should fall back to defaults, got %+v", cfg)
	}
}

// TestLoadInvalid covers the config half of "Invalid configuration fails fast":
// every invalid value is an error naming the variable.
func TestLoadInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key, value string
	}{
		{"INGEST_WORKERS", "zero"},
		{"INGEST_WORKERS", "0"},
		{"INGEST_WORKERS", "-2"},
		{"INGEST_BATCH_SIZE", "0"},
		{"INGEST_BATCH_SIZE", "50001"},
		{"INGEST_BATCH_SIZE", "lots"},
		{"LOG_LEVEL", "verbose"},
		{"SHUTDOWN_TIMEOUT", "20"},
		{"SHUTDOWN_TIMEOUT", "-1s"},
		{"SHUTDOWN_DRAIN_DELAY", "-1s"},
		{"SHUTDOWN_DRAIN_DELAY", "soon"},
		{"INGEST_PROGRESS_INTERVAL", "0s"},
		{"DATASET_URL", "ftp://example.com/x.tgz"},
		{"DATASET_URL", "not a url"},
	}
	for _, tt := range tests {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			t.Parallel()

			_, err := config.LoadFrom(lookupMap(map[string]string{tt.key: tt.value}))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !errors.Is(err, config.ErrInvalid) {
				t.Errorf("error %v does not wrap ErrInvalid", err)
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Errorf("error %q does not name %s", err, tt.key)
			}
		})
	}
}

func TestLoadZeroDrainDelay(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(lookupMap(map[string]string{"SHUTDOWN_DRAIN_DELAY": "0s"}))
	if err != nil || cfg.ShutdownDrainDelay != 0 {
		t.Errorf("SHUTDOWN_DRAIN_DELAY=0s: cfg %+v, err %v; want 0 accepted", cfg, err)
	}
}

func TestLoadReportsEveryInvalidVariable(t *testing.T) {
	t.Parallel()

	_, err := config.LoadFrom(lookupMap(map[string]string{"INGEST_WORKERS": "zero", "LOG_LEVEL": "loud"}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, key := range []string{"INGEST_WORKERS", "LOG_LEVEL"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not name %s", err, key)
		}
	}
}

func TestLoadReadsProcessEnvironment(t *testing.T) {
	t.Setenv("INGEST_BATCH_SIZE", "42")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.IngestBatchSize != 42 {
		t.Errorf("IngestBatchSize = %d, want 42", cfg.IngestBatchSize)
	}
}
