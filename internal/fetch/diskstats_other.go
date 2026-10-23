//go:build !linux && !darwin

package fetch

// statDisk is not implemented on this platform; the checks are skipped.
func statDisk(string) (diskStats, error) {
	return diskStats{}, errDiskStatsUnsupported
}
