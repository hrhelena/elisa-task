package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// decodeLogLines parses newline-delimited JSON log output.
func decodeLogLines(t *testing.T, out []byte) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for line := range bytes.Lines(out) {
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		lines = append(lines, rec)
	}
	return lines
}

func TestRunLogsStartingAsJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	if err := run(t.Context(), newLogger(&buf)); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	lines := decodeLogLines(t, buf.Bytes())
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1: %s", len(lines), buf.String())
	}
	got := lines[0]
	if got["msg"] != "starting" || got["level"] != "INFO" || got["service"] != "enron-service" {
		t.Errorf("unexpected log record: %v", got)
	}
}

// TestBuildProducesStaticBinary covers the Gherkin scenario "Build produces a
// static binary": it builds with the same flags as `make build`, runs the
// binary and checks it logs a JSON line with "msg":"starting" and exits 0.
func TestBuildProducesStaticBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary; skipped in -short mode")
	}
	t.Parallel()

	bin := filepath.Join(t.TempDir(), "enron-service")
	build := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", bin, ".") //nolint:gosec // fixed command; bin is a t.TempDir path.
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}

	out, err := exec.CommandContext(t.Context(), bin).Output() //nolint:gosec // bin is built by this test into t.TempDir.
	if err != nil {
		t.Fatalf("binary exited with error: %v", err)
	}
	lines := decodeLogLines(t, out)
	if len(lines) == 0 || lines[0]["msg"] != "starting" {
		t.Fatalf(`want first log line with "msg":"starting", got %s`, out)
	}
}
