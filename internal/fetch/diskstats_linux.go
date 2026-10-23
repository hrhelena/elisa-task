package fetch

import (
	"fmt"
	"syscall"
)

// statDisk reports the space and inodes available to unprivileged users in
// dir's filesystem. Block counts are in units of the fragment size (Frsize),
// not the preferred I/O size (Bsize), per statfs(2).
func statDisk(dir string) (diskStats, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return diskStats{}, fmt.Errorf("statfs %s: %w", dir, err)
	}
	return diskStats{
		freeBytes:   st.Bavail * uint64(st.Frsize), //nolint:gosec // Frsize is positive.
		freeInodes:  st.Ffree,
		inodesKnown: st.Files > 0, // filesystems without fixed inode tables (btrfs, some FUSE) report 0.
	}, nil
}
