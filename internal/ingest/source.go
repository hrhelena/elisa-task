package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hrhelena/elisa-task/internal/transient"
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
	// ErrTransient is returned when the dataset root could not be checked
	// because of a transient system error (out of file descriptors or
	// memory, an interrupted call). It says nothing about the dataset: retry
	// later (POST /start answers 503 unavailable, #49). It does not wrap
	// ErrDatasetMissing.
	ErrTransient = errors.New("checking the dataset root failed temporarily")
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
	// ErrDatasetMissing (wrapped) if the root is or becomes unavailable, or
	// with ErrTransient if it cannot be checked at the start. The open
	// functions may be called after Walk returns, until ctx is done.
	Walk(ctx context.Context, fn func(relPath string, open OpenFunc) error) error
	// Available returns nil if the dataset root exists and is readable, an
	// error wrapping ErrTransient if it could not be checked right now, else
	// an error wrapping ErrDatasetMissing (and ErrDatasetInvalid when the
	// root exists but cannot be used).
	Available() error
}

// maildirSource walks a maildir tree on the local file system.
//
// The walk never resolves a path from the root path again once it has opened
// the checked root directory (#49): each directory is opened as an os.Root
// relative to its parent's descriptor, and each file relative to its
// directory's. A root replaced during the walk (for example by a symlink to
// another directory) therefore cannot redirect it, and no entry can escape
// its directory. One descriptor per directory, rather than one os.Root for
// the whole tree, keeps every file open a single openat call: os.Root
// resolves a multi-component name one component at a time.
type maildirSource struct {
	root string
	// lstat and openRoot are os.Lstat and os.OpenRoot; tests replace them to
	// inject errors or to swap the root between the check and the open.
	lstat    func(name string) (fs.FileInfo, error)
	openRoot func(name string) (*os.Root, error)
}

// NewMaildirSource returns a Source over the maildir at root
// ($DATA_DIR/maildir). Files are visited in lexical order.
func NewMaildirSource(root string) Source {
	return &maildirSource{root: root, lstat: os.Lstat, openRoot: os.OpenRoot}
}

// Available requires the root itself to be a directory, not a symlink to
// one: the walk does not follow links, and POST /fetch treats a symlinked
// maildir the same way (409 maildir_not_directory, #45). A root that does not
// exist wraps ErrDatasetMissing; one that exists but cannot be used is an
// *InvalidRootError; a transient system error wraps ErrTransient.
func (s *maildirSource) Available() error {
	r, _, err := s.open()
	if err != nil {
		return err
	}
	_ = r.Close() // read-only directory handle
	return nil
}

// check validates the root path with Lstat (a symlink is never followed) and
// returns what it found.
func (s *maildirSource) check() (fs.FileInfo, error) {
	fi, err := s.lstat(s.root)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, fmt.Errorf("%w: %w", ErrDatasetMissing, err)
	case transient.Is(err):
		return nil, fmt.Errorf("%w: %w", ErrTransient, err)
	case err != nil:
		return nil, &InvalidRootError{Path: s.root, Problem: "it cannot be checked: " + pathErrText(err), Err: err}
	case fi.Mode()&fs.ModeSymlink != 0:
		return nil, &InvalidRootError{Path: s.root, Problem: "it is a symlink, which is not followed; replace it with the directory itself"}
	case !fi.IsDir():
		return nil, &InvalidRootError{Path: s.root, Problem: fmt.Sprintf("it is not a directory (mode %s)", fi.Mode())}
	}
	return fi, nil
}

// open checks the root, opens it, and makes sure that the opened directory
// is the one that was checked: os.OpenRoot follows a symlink, so a root
// swapped for one in between would otherwise be opened through it. It
// returns the root and the checked FileInfo.
func (s *maildirSource) open() (*os.Root, fs.FileInfo, error) {
	checked, err := s.check()
	if err != nil {
		return nil, nil, err
	}
	r, err := s.openRoot(s.root)
	if err != nil {
		return nil, nil, s.unreadable(err)
	}
	opened, err := r.Stat(".")
	if err != nil {
		_ = r.Close()
		return nil, nil, s.unreadable(err)
	}
	if !os.SameFile(checked, opened) {
		_ = r.Close()
		return nil, nil, s.changed("while it was being opened")
	}
	return r, checked, nil
}

// unreadable is the error for a checked root that cannot be opened or read.
func (s *maildirSource) unreadable(err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%w: %w", ErrDatasetMissing, err)
	case transient.Is(err):
		return fmt.Errorf("%w: %w", ErrTransient, err)
	}
	return &InvalidRootError{Path: s.root, Problem: "it cannot be read: " + pathErrText(err), Err: err}
}

// changed is the error for a root path that no longer names the directory
// that was checked. check is asked again for the precise problem (a symlink,
// a file, gone); if the path is a directory again (swapped back, or replaced
// by another directory), it still fails: that directory was never checked.
func (s *maildirSource) changed(when string) error {
	if _, err := s.check(); err != nil {
		return err
	}
	return &InvalidRootError{Path: s.root, Problem: "it changed " + when}
}

// lost returns an error if the root path no longer names the opened
// directory. The walk itself stays in the opened directory whatever happens
// to the path; this only decides whether an error below the root is one
// skipped entry or the end of the run. A transient error is not a loss.
func (s *maildirSource) lost(opened fs.FileInfo) error {
	cur, err := s.check()
	switch {
	case errors.Is(err, ErrTransient):
		return nil
	case err != nil:
		return err
	case !os.SameFile(opened, cur):
		return s.changed("during the walk")
	}
	return nil
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
	r, opened, err := s.open()
	if err != nil {
		return err
	}
	w := &walker{ctx: ctx, s: s, fn: fn, opened: opened}
	return w.walkDir(newDirHandle(ctx, r), "")
}

// walker is the state of one Walk.
type walker struct {
	ctx context.Context //nolint:containedctx // the context of the one Walk call that owns this walker.
	s   *maildirSource
	fn  func(relPath string, open OpenFunc) error
	// opened is the root directory as checked and opened at the start.
	opened fs.FileInfo
}

// walkDir visits the directory h, whose path relative to the root is rel ("" for
// the root), in lexical order, depth first. It owns one reference to h.
func (w *walker) walkDir(h *dirHandle, rel string) error {
	defer h.release()
	entries, err := h.readDir(w.s.root + "/" + rel)
	if err != nil {
		if rel == "" {
			return w.s.unreadable(err)
		}
		// The entries read before the error are still visited.
		if err := w.unreadableDir(rel, err); err != nil {
			return err
		}
	}
	for _, d := range entries {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		name := d.Name()
		path := name
		if rel != "" {
			path = rel + "/" + name
		}
		switch {
		case d.IsDir():
			sub, err := h.r.OpenRoot(name)
			if err != nil {
				err = w.unreadableDir(path, err)
			} else {
				err = w.walkDir(newDirHandle(w.ctx, sub), path)
			}
			if err != nil {
				return err
			}
		case !d.Type().IsRegular():
			if err := w.fn(path, failOpen(fmt.Errorf("%w: %s", ErrNotRegularFile, d.Type()))); err != nil {
				return err
			}
		default:
			if err := w.fn(path, h.opener(name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// unreadableDir handles a directory below the root that could not be opened
// or read. A lost root fails the walk; otherwise the directory is one skipped
// entry, and its files are not visited.
func (w *walker) unreadableDir(rel string, err error) error {
	if lostErr := w.s.lost(w.opened); lostErr != nil {
		return lostErr
	}
	return w.fn(rel, failOpen(fmt.Errorf("%w: %w", ErrUnreadableDir, err)))
}

// dirHandle owns the os.Root of one directory of the walk. The open
// functions a walk yields are called by other goroutines, possibly after Walk
// has returned, so the directory is closed when the walk is done with it and
// every open function yielded for its files has been called, or when the
// walk's context is done, whichever comes first.
//
// Descriptor use therefore scales with the workers: the walk holds one per
// directory on its current path, and every queued or in-flight job (the
// pipeline buffers Workers*4) pins its directory, plus each worker's open
// file. With many single-file folders that is up to about 5 descriptors per
// worker. A very high INGEST_WORKERS under a low hard descriptor limit can
// make a subdirectory's open fail with EMFILE; it then becomes one read_error
// skip and its files are not visited (README, Configuration).
type dirHandle struct {
	r *os.Root
	// refs counts the walk itself plus each open function not yet called.
	refs      atomic.Int64
	closeOnce sync.Once
	stop      func() bool
}

func newDirHandle(ctx context.Context, r *os.Root) *dirHandle {
	h := &dirHandle{r: r}
	h.refs.Store(1)
	h.stop = context.AfterFunc(ctx, h.close)
	return h
}

func (h *dirHandle) release() {
	if h.refs.Add(-1) == 0 {
		h.stop()
		h.close()
	}
}

func (h *dirHandle) close() {
	h.closeOnce.Do(func() { _ = h.r.Close() }) // read-only directory handle
}

// readDir returns the directory's entries sorted by name, and the entries
// read before an error, if any. path is the directory's path, used only by
// readDirEntries.
func (h *dirHandle) readDir(path string) ([]fs.DirEntry, error) {
	d, err := h.r.Open(".")
	if err != nil {
		return nil, err //nolint:wrapcheck // *fs.PathError already names the directory.
	}
	defer func() { _ = d.Close() }() // read-only directory handle
	entries, err := readDirEntries(d, path)
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return entries, err //nolint:wrapcheck // as above.
}

// opener returns the open function of the regular file name in this
// directory. Its first call releases its reference to the directory; a
// later call opens the file again only while the directory is still open.
func (h *dirHandle) opener(name string) OpenFunc {
	h.refs.Add(1)
	var called atomic.Bool
	return func() (io.ReadCloser, error) {
		if !called.Swap(true) {
			defer h.release()
		}
		return openRegular(h.r, name)
	}
}

// openRegular opens name inside r and checks that it is still a regular
// file (it may have been replaced since the directory was read).
func openRegular(r *os.Root, name string) (io.ReadCloser, error) {
	f, err := r.Open(name)
	if err != nil {
		return nil, err //nolint:wrapcheck // *fs.PathError already names the file.
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err //nolint:wrapcheck // as above.
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
