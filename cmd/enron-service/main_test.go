package main

import (
	"bytes"
	"debug/elf"
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

// buildBinary builds ./cmd/enron-service like `make build` (CGO_ENABLED=0,
// -trimpath) plus any extra env, and returns the binary path. Keep the two in
// sync.
func buildBinary(t *testing.T, env ...string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "enron-service")
	build := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", bin, ".") //nolint:gosec // fixed command; bin is a t.TempDir path.
	build.Env = append(append(os.Environ(), "CGO_ENABLED=0"), env...)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}
	return bin
}

// TestBuildProducesStaticBinary covers the static half of the Gherkin
// scenario "Build produces a static binary". It cross-builds for the
// deployment target (linux/amd64; macOS has no static executables) and
// asserts the ELF has no dynamic interpreter and no shared-library imports.
func TestBuildProducesStaticBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary; skipped in -short mode")
	}
	t.Parallel()

	bin := buildBinary(t, "GOOS=linux", "GOARCH=amd64")
	f, err := elf.Open(bin)
	if err != nil {
		t.Fatalf("opening ELF: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	for _, p := range f.Progs {
		if p.Type == elf.PT_INTERP {
			t.Error("binary has a PT_INTERP segment (dynamically linked)")
		}
	}
	libs, err := f.ImportedLibraries()
	if err != nil {
		t.Fatalf("reading imported libraries: %v", err)
	}
	if len(libs) != 0 {
		t.Errorf("binary imports shared libraries: %v", libs)
	}
}

// TestBinaryLogsStartingAsJSON covers the run half of the Gherkin scenario
// "Build produces a static binary": the built binary runs, exits 0 and logs a
// JSON line with "msg":"starting".
func TestBinaryLogsStartingAsJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary; skipped in -short mode")
	}
	t.Parallel()

	bin := buildBinary(t)
	out, err := exec.CommandContext(t.Context(), bin).Output() //nolint:gosec // bin is built by this test into t.TempDir.
	if err != nil {
		t.Fatalf("binary exited with error: %v", err)
	}
	lines := decodeLogLines(t, out)
	if len(lines) == 0 || lines[0]["msg"] != "starting" {
		t.Fatalf(`want first log line with "msg":"starting", got %s`, out)
	}
}
