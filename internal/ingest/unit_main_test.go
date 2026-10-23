//go:build !integration

package ingest

import (
	"fmt"
	"os"
	"testing"

	"go.uber.org/goleak"
)

// runTests runs the unit tests and fails on leaked goroutines.
func runTests(m *testing.M) int {
	if code := m.Run(); code != 0 {
		return code
	}
	if err := goleak.Find(); err != nil {
		fmt.Fprintln(os.Stderr, "goleak:", err)
		return 1
	}
	return 0
}
