//go:build !linux && !darwin

package fetch

// freeSpace is not implemented on this platform; the check is skipped.
func freeSpace(string) (uint64, error) {
	return 0, errFreeSpaceUnsupported
}
