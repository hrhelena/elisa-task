package fetch

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
)

// Reasons an archive entry is skipped (label values of
// enron_fetch_entries_skipped_total).
const (
	skipUnsafePath     = "unsafe_path"     // absolute, escapes the target, or contains NUL.
	skipOutsideMaildir = "outside_maildir" // not under the archive's maildir/ directory.
	skipLink           = "link"            // symlink or hardlink.
	skipUnsupported    = "unsupported_type"
	skipPathConflict   = "path_conflict" // e.g. a file where a directory already is.
)

// Size limits that bound what a malicious or corrupt archive can make the
// extractor do. The real dataset is about 2.3 GB extracted.
const (
	// maxExtractedBytes caps the total size of extracted files.
	maxExtractedBytes int64 = 8 << 30
	// maxTrailingBytes caps the data after the tar end marker (normally
	// zero padding to the record size, 10 KiB).
	maxTrailingBytes int64 = 1 << 20
)

// errTooLarge is returned when the archive exceeds maxExtractedBytes.
var errTooLarge = fmt.Errorf("archive exceeds the extraction limit of %s", formatBytes(uint64(maxExtractedBytes)))

// skipLogLimit is how many skips per reason are logged individually; the rest
// are only counted (ADR 0007 sampling) and appear in the end-of-fetch totals.
const skipLogLimit = 100

// File modes for extracted entries; archive modes and ownership are ignored.
const (
	fileMode fs.FileMode = 0o644
	dirMode  fs.FileMode = 0o755
)

// extractor streams a gzip-compressed tar into dir/maildir. All writes go
// through an os.Root, so no entry can escape dir even if a path check were
// wrong; no links are ever created.
type extractor struct {
	dir     string
	logger  *slog.Logger
	metrics *metrics
	files   *atomic.Int64
	skipped map[string]int64
	// written is the total size of extracted files so far.
	written int64
	// limit is the maximum total size of extracted files.
	limit int64
}

func newExtractor(dir string, logger *slog.Logger, m *metrics, files *atomic.Int64) *extractor {
	return &extractor{dir: dir, logger: logger, metrics: m, files: files, skipped: map[string]int64{}, limit: maxExtractedBytes}
}

// extract reads the whole gzip stream from r, including the gzip trailer, so
// a truncated or corrupt download is an error. Unsafe or unsupported entries
// are skipped and counted; I/O errors on the target are returned.
func (e *extractor) extract(r io.Reader) error {
	root, err := os.OpenRoot(e.dir)
	if err != nil {
		return fmt.Errorf("opening extraction directory: %w", err)
	}
	defer func() { _ = root.Close() }()

	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("reading archive: %w", err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading archive: %w", err)
		}
		if err := e.entry(root, tr, hdr); err != nil {
			return err
		}
	}
	// Drain the zero blocks and padding after the tar end marker so the gzip
	// reader checks the CRC and size in the trailer.
	n, err := io.CopyN(io.Discard, gz, maxTrailingBytes+1)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reading archive: %w", err)
	}
	if n > maxTrailingBytes {
		return errors.New("reading archive: unexpected data after the end of the tar archive")
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("reading archive: %w", err)
	}

	e.logSkipTotals()
	if fi, err := root.Stat(MaildirName); err != nil || !fi.IsDir() {
		return errors.New("archive contains no maildir directory")
	}
	return nil
}

// entry extracts one archive entry under root.
func (e *extractor) entry(root *os.Root, tr io.Reader, hdr *tar.Header) error {
	rel, reason := targetPath(hdr.Name)
	if reason != "" {
		e.skip(reason, hdr)
		return nil
	}
	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := root.MkdirAll(rel, dirMode); err != nil {
			return e.writeErr(err, hdr)
		}
	case tar.TypeReg:
		e.written += hdr.Size
		if e.written > e.limit {
			return errTooLarge
		}
		if err := e.writeFile(root, rel, tr); err != nil {
			return e.writeErr(err, hdr)
		}
		e.files.Add(1)
	case tar.TypeSymlink, tar.TypeLink:
		e.skip(skipLink, hdr)
	default:
		e.skip(skipUnsupported, hdr)
	}
	return nil
}

func (e *extractor) writeFile(root *os.Root, rel string, r io.Reader) error {
	if dir := filepath.Dir(rel); dir != "." {
		if err := root.MkdirAll(dir, dirMode); err != nil {
			return fmt.Errorf("creating directory: %w", err)
		}
	}
	out, err := root.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	if _, err := io.Copy(out, r); err != nil {
		_ = out.Close()
		return fmt.Errorf("writing file: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("closing file: %w", err)
	}
	return nil
}

// writeErr classifies an error from writing an entry. A path conflict comes
// from the archive's content (malformed input, ADR 0007) and is skipped;
// anything else (disk full, permissions, a truncated stream) fails the fetch.
func (e *extractor) writeErr(err error, hdr *tar.Header) error {
	if errors.Is(err, fs.ErrExist) || errors.Is(err, syscall.ENOTDIR) || errors.Is(err, syscall.EISDIR) {
		e.skip(skipPathConflict, hdr)
		return nil
	}
	return fmt.Errorf("extracting %q: %w", hdr.Name, err)
}

func (e *extractor) skip(reason string, hdr *tar.Header) {
	e.skipped[reason]++
	e.metrics.skipped.WithLabelValues(reason).Inc()
	if n := e.skipped[reason]; n <= skipLogLimit {
		e.logger.Warn("skipping archive entry",
			slog.String("reason", reason), slog.String("name", hdr.Name),
			slog.String("type", string(hdr.Typeflag)), slog.String("linkname", hdr.Linkname))
		if n == skipLogLimit {
			e.logger.Warn("further skips for this reason are not logged individually", slog.String("reason", reason))
		}
	}
}

func (e *extractor) logSkipTotals() {
	if len(e.skipped) == 0 {
		return
	}
	e.logger.Warn("archive entries skipped", slog.Any("by_reason", e.skipped))
}

// targetPath maps an archive entry name to its path relative to the
// extraction directory, always under maildir/. The archive's top-level
// directory is stripped when it wraps maildir/ (for example
// "enron_mail/maildir/x" becomes "maildir/x"). A non-empty reason means the
// entry must be skipped.
func targetPath(name string) (string, string) {
	if name == "" || strings.ContainsRune(name, 0) || strings.Contains(name, `\`) {
		return "", skipUnsafePath
	}
	// Reject absolute and escaping names before cleaning, so "a/../../x" is
	// not silently turned into something harmless-looking.
	if path.IsAbs(name) || !filepath.IsLocal(filepath.FromSlash(strings.TrimSuffix(name, "/"))) {
		return "", skipUnsafePath
	}
	parts := strings.Split(path.Clean(name), "/")
	switch {
	case parts[0] == MaildirName:
	case len(parts) > 1 && parts[1] == MaildirName:
		parts = parts[1:]
	default:
		return "", skipOutsideMaildir
	}
	return filepath.Join(parts...), ""
}
