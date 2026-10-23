package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

var (
	// ErrDatasetMissing is returned when the dataset root does not exist or
	// is not a readable directory (ADR 0011: 409 dataset_missing).
	ErrDatasetMissing = errors.New("dataset not found")
	// ErrNotRegularFile is returned by a Source's open function for an entry
	// that is not a regular file (symlink, device, socket, ...).
	ErrNotRegularFile = errors.New("not a regular file")
	// ErrUnreadableDir is returned by a Source's open function for a
	// directory the walk could not read. Its files are not visited.
	ErrUnreadableDir = errors.New("directory not readable")
)

// OpenFunc opens one entry yielded by a Source.
type OpenFunc func() (io.ReadCloser, error)

// Source yields the files of a dataset (ADR 0009: other sources can be added).
type Source interface {
	// Walk calls fn for every entry in a deterministic order. relPath uses
	// "/" separators and is relative to the dataset root. Entries that are
	// not messages are yielded too, with an open function that fails with
	// ErrNotRegularFile or ErrUnreadableDir, so that every entry is counted.
	// Walk stops at the first error from fn or ctx, and fails with
	// ErrDatasetMissing (wrapped) if the root is or becomes unavailable.
	Walk(ctx context.Context, fn func(relPath string, open OpenFunc) error) error
	// Available returns nil if the dataset root exists and is readable, else
	// an error wrapping ErrDatasetMissing.
	Available() error
}

// maildirSource walks a maildir tree on the local file system.
type maildirSource struct {
	root string
}

// NewMaildirSource returns a Source over the maildir at root
// ($DATA_DIR/maildir). Files are visited in lexical order.
func NewMaildirSource(root string) Source {
	return &maildirSource{root: root}
}

// Available requires the root itself to be a directory, not a symlink to
// one: the walk does not follow links, and POST /fetch treats a symlinked
// maildir the same way (409 maildir_not_directory).
func (s *maildirSource) Available() error {
	fi, err := os.Lstat(s.root)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatasetMissing, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("%w: %s is not a directory (%s)", ErrDatasetMissing, s.root, fi.Mode().Type())
	}
	f, err := os.Open(s.root)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDatasetMissing, err)
	}
	_ = f.Close() // read-only directory handle
	return nil
}

func (s *maildirSource) Walk(ctx context.Context, fn func(relPath string, open OpenFunc) error) error {
	if err := s.Available(); err != nil {
		return err
	}
	return filepath.WalkDir(s.root, func(path string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == s.root {
			if walkErr != nil {
				return fmt.Errorf("%w: %w", ErrDatasetMissing, walkErr)
			}
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return fmt.Errorf("relative path of %s: %w", path, err)
		}
		rel = filepath.ToSlash(rel)

		if walkErr != nil {
			// A losing root fails the run; anything else (an unreadable
			// directory, an entry removed during the walk) is one skip.
			if err := s.Available(); err != nil {
				return err
			}
			return fn(rel, failOpen(fmt.Errorf("%w: %w", ErrUnreadableDir, walkErr)))
		}
		switch {
		case d.IsDir():
			return nil
		case !d.Type().IsRegular():
			return fn(rel, failOpen(fmt.Errorf("%w: %s", ErrNotRegularFile, d.Type())))
		default:
			return fn(rel, func() (io.ReadCloser, error) { return openRegular(path) })
		}
	})
}

// openRegular opens path and checks that it is still a regular file (it may
// have been replaced since the directory was read).
func openRegular(path string) (io.ReadCloser, error) {
	f, err := os.Open(path) //nolint:gosec // path comes from walking the configured dataset root
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s", ErrNotRegularFile, fi.Mode().Type())
	}
	return f, nil
}

func failOpen(err error) OpenFunc {
	return func() (io.ReadCloser, error) { return nil, err }
}
