//go:build !integration

package store

import (
	"testing"

	"go.uber.org/goleak"
)

// leakIgnores is empty: the unit tests start no background goroutines that
// may outlive them.
var leakIgnores []goleak.Option

func runTests(m *testing.M) int { return m.Run() }
