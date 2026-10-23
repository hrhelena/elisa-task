package store

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the tests through runTests, which differs by build tag (unit
// runs only run the tests; integration runs start and terminate a Postgres
// container around them), then fails the run on leaked goroutines (#34).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(testRunner{m}, leakIgnores...)
}

// testRunner adapts runTests to goleak.TestingM.
type testRunner struct{ m *testing.M }

func (r testRunner) Run() int { return runTests(r.m) }
