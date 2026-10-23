//go:build !unix

package ingest

import (
	"io/fs"
	"os"
)

// readDirEntries reads all entries of the directory d (see readdir_unix.go
// for why Unix does more).
func readDirEntries(d *os.File, _ string) ([]fs.DirEntry, error) {
	return d.ReadDir(-1) //nolint:wrapcheck // *fs.PathError already names the directory.
}
