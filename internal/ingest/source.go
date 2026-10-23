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
	// ErrDatasetInvalid is returned when the dataset root exists but cannot
	// be used: it is a symlink (never followed), not a directory, or not
	// readable. It wraps ErrDatasetMissing, so ADR 0011's 409
	// dataset_missing still applies; errors.As with *InvalidRootError gives
	// the problem (#45).
	ErrDatasetInvalid = fmt.Errorf("%w: dataset root is not usable", ErrDatasetMissing)
	// ErrNotRegularFile is returned by a Source's open function for an entry
	// that is not a regular file (symlink, device, socket, ...).
	ErrNotRegularFile = errors.New("not a regular file")
	// ErrUnreadableDir is returned by a Source's open function for a
	// directory the walk could not read. Its files are not visited.
	ErrUnreadableDir = errors.New("directory not readable")
)

// InvalidRootError says why an existing dataset root cannot be used. It
// matches ErrDatasetInvalid and ErrDatasetMissing with errors.Is.
type InvalidRootError struct {
	Path string
	// Problem is a short explanation for an operator, for example "it is a
	// symlink, which is not followed; replace it with the directory itself".
	Problem string
	// Err is the underlying error, if any (for example a permission error).
	Err error
}

func (e *InvalidRootError) Error() string {
	return fmt.Sprintf("dataset root is not usable: %s: %s", e.Path, e.Problem)
}

// Is reports whether target is ErrDatasetInvalid or ErrDatasetMissing.
func (e *InvalidRootError) Is(target error) bool {
	return target == ErrDatasetInvalid || target == ErrDatasetMissing
}

func (e *InvalidRootError) Unwrap() error { return e.Err }

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
	// an error wrapping ErrDatasetMissing (and ErrDatasetInvalid when the
	// root exists but cannot be used).
	Available() error
}

// maildirSource walks a maildir tree on the local file system.
type maildirSource struct {
	root string
	// afterCheck, if set, runs between Walk's Available check and the walk
	// (tests use it to swap the root).
	afterCheck func()
}

// NewMaildirSource returns a Source over the maildir at root
// ($DATA_DIR/maildir). Files are visited in lexical order.
func NewMaildirSource(root string) Source {
	return &maildirSource{root: root}
}

// Available requires the root itself to be a directory, not a symlink to
// one: the walk does not follow links, and POST /fetch treats a symlinked
// maildir the same way (409 maildir_not_directory, #45). A root that does not
// exist wraps ErrDatasetMissing; one that exists but cannot be used is an
// *InvalidRootError.
func (s *maildirSource) Available() error {
	fi, err := os.Lstat(s.root)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%w: %w", ErrDatasetMissing, err)
	case err != nil:
		return &InvalidRootError{Path: s.root, Problem: "it cannot be checked: " + pathErrText(err), Err: err}
	case fi.Mode()&fs.ModeSymlink != 0:
		return &InvalidRootError{Path: s.root, Problem: "it is a symlink, which is not followed; replace it with the directory itself"}
	case !fi.IsDir():
		return &InvalidRootError{Path: s.root, Problem: fmt.Sprintf("it is not a directory (mode %s)", fi.Mode())}
	}
	f, err := os.Open(s.root)
	if err != nil {
		return &InvalidRootError{Path: s.root, Problem: "it cannot be read: " + pathErrText(err), Err: err}
	}
	_ = f.Close() // read-only directory handle
	return nil
}

// rootReplaced is the error for a root that WalkDir found not to be a
// directory after Available passed. Available is asked again for the precise
// problem; if the root was swapped back meanwhile, the run still fails rather
// than report a walk that never happened.
func (s *maildirSource) rootReplaced(d fs.DirEntry) error {
	if err := s.Available(); err != nil {
		return err
	}
	return &InvalidRootError{Path: s.root, Problem: fmt.Sprintf("it changed during the walk (found %s)", d.Type())}
}

// pathErrText returns the cause of a *fs.PathError without the operation and
// path, which the caller already names (for example "permission denied").
func pathErrText(err error) string {
	if pe, ok := errors.AsType[*fs.PathError](err); ok {
		return pe.Err.Error()
	}
	return err.Error()
}

func (s *maildirSource) Walk(ctx context.Context, fn func(relPath string, open OpenFunc) error) error {
	if err := s.Available(); err != nil {
		return err
	}
	if s.afterCheck != nil {
		s.afterCheck()
	}
	return filepath.WalkDir(s.root, func(path string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if path == s.root {
			if walkErr != nil {
				return fmt.Errorf("%w: %w", ErrDatasetMissing, walkErr)
			}
			if !d.IsDir() {
				// Replaced (for example by a symlink) after the Available
				// check: WalkDir would not descend, and the run would
				// complete with 0 files.
				return s.rootReplaced(d)
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
