package fetch

import (
	"fmt"
	"syscall"
)

// statDisk reports the space and inodes available to unprivileged users in
// dir's filesystem. On darwin, f_bsize is the fundamental block size that
// the block counts are expressed in.
func statDisk(dir string) (diskStats, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return diskStats{}, fmt.Errorf("statfs %s: %w", dir, err)
	}
	return diskStats{
		freeBytes:   st.Bavail * uint64(st.Bsize),
		freeInodes:  st.Ffree,
		inodesKnown: st.Files > 0,
	}, nil
}
