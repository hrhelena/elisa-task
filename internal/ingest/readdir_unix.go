//go:build unix

package ingest

import (
	"io/fs"
	"os"
	"syscall"
)

// readDirEntries reads all entries of the directory d, which was opened in
// an os.Root, through a plain *os.File on a duplicate of its descriptor.
//
// A File opened in an os.Root resolves the type of every entry with an extra
// lstatat call while reading the directory (os.newUnixDirent), instead of
// using the type the directory entry already carries. That is one extra
// system call per file, made by the single walker goroutine, and it made the
// walk the bottleneck of an ingestion (#49: the full-corpus walk took 1.5 to
// 2 times as long on macOS). A plain File on the same open directory uses the
// entry's type. Its name is path, so on a filesystem that reports no types,
// the fallback lstat goes through the path; a wrong type there can only make
// an open fail, because every open stays relative to a directory descriptor.
//
// If the descriptor cannot be duplicated (for example out of descriptors), d
// is read directly.
func readDirEntries(d *os.File, path string) ([]fs.DirEntry, error) {
	syscall.ForkLock.RLock() // as os does: no child may inherit fd before it is close-on-exec
	fd, err := syscall.Dup(int(d.Fd()))
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return d.ReadDir(-1) //nolint:wrapcheck // *fs.PathError already names the directory.
	}
	plain := os.NewFile(uintptr(fd), path)
	defer func() { _ = plain.Close() }() // read-only directory handle

	return plain.ReadDir(-1) //nolint:wrapcheck // as above.
}
