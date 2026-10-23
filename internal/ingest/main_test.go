package ingest

import (
	"os"
	"testing"
)

// TestMain delegates to runTests, which differs by build tag: unit runs
// check for goroutine leaks, integration runs start a Postgres container.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}
