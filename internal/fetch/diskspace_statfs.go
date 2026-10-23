//go:build linux || darwin

package fetch

import (
	"fmt"
	"syscall"
)

// freeSpace returns the bytes available to unprivileged users in dir's filesystem.
func freeSpace(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", dir, err)
	}
	return st.Bavail * uint64(st.Bsize), nil //nolint:gosec,unconvert // Bsize is positive; its type differs per OS.
}
