package fetch

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestTargetPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		want       string
		wantReason string
	}{
		{name: "maildir/", want: "maildir"},
		{name: "maildir/allen-p/inbox/1.", want: filepath.Join("maildir", "allen-p", "inbox", "1.")},
		{name: "./maildir/allen-p/1.", want: filepath.Join("maildir", "allen-p", "1.")},
		{name: "maildir//allen-p/./1.", want: filepath.Join("maildir", "allen-p", "1.")},
		{name: "maildir/a/../b/1.", want: filepath.Join("maildir", "b", "1.")},
		// A wrapping top-level directory is stripped.
		{name: "enron_mail_20150507/maildir/allen-p/1.", want: filepath.Join("maildir", "allen-p", "1.")},
		{name: "enron_mail_20150507/maildir/", want: "maildir"},
		// Unsafe names.
		{name: "../../evil.txt", wantReason: skipUnsafePath},
		{name: "maildir/../../evil.txt", wantReason: skipUnsafePath},
		{name: "/etc/passwd", wantReason: skipUnsafePath},
		{name: "/maildir/allen-p/1.", wantReason: skipUnsafePath},
		{name: "..", wantReason: skipUnsafePath},
		{name: "", wantReason: skipUnsafePath},
		{name: "maildir/a\x00b", wantReason: skipUnsafePath},
		{name: `maildir\..\..\evil.txt`, wantReason: skipUnsafePath},
		// Safe but not part of the dataset.
		{name: "README", wantReason: skipOutsideMaildir},
		{name: "./", wantReason: skipOutsideMaildir},
		{name: "a/b/maildir/x", wantReason: skipOutsideMaildir},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, reason := targetPath(tt.name)
			if got != tt.want || reason != tt.wantReason {
				t.Errorf("targetPath(%q) = (%q, %q), want (%q, %q)", tt.name, got, reason, tt.want, tt.wantReason)
			}
		})
	}
}

// extractInto runs the extractor on archive into a fresh directory nested in
// a parent directory (so escapes would be visible in the parent).
func extractInto(t *testing.T, archive []byte) (parent, dir string, ex *extractor, err error) {
	t.Helper()
	parent = t.TempDir()
	dir = filepath.Join(parent, "data", "tmp")
	if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
		t.Fatal(mkErr)
	}
	var files atomic.Int64
	ex = newExtractor(dir, slog.New(slog.DiscardHandler), newMetrics(nil, slog.New(slog.DiscardHandler)), &files)
	err = ex.extract(bytes.NewReader(archive))
	return parent, dir, ex, err
}

func TestExtract_PathTraversalEntryIsRejected(t *testing.T) {
	t.Parallel()
	archive := buildArchive(t,
		entry{name: "../../evil.txt", body: "evil"},
		entry{name: "maildir/../../../evil.txt", body: "evil"},
		entry{name: "/tmp/evil-abs.txt", body: "evil"},
		entry{name: "maildir/allen-p/inbox/1.", body: "ok"},
	)
	parent, dir, ex, err := extractInto(t, archive)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	assertFile(t, filepath.Join(dir, "maildir", "allen-p", "inbox", "1."), "ok")
	for _, p := range []string{
		filepath.Join(parent, "evil.txt"),
		filepath.Join(parent, "data", "evil.txt"),
		filepath.Join(dir, "evil.txt"),
		filepath.Join(dir, "tmp", "evil-abs.txt"),
	} {
		assertNotExist(t, p)
	}
	if got := ex.skipped[skipUnsafePath]; got != 3 {
		t.Errorf("skipped[%s] = %d, want 3", skipUnsafePath, got)
	}
	if got := ex.files.Load(); got != 1 {
		t.Errorf("files = %d, want 1", got)
	}
}

func TestExtract_SymlinkEntryIsSkipped(t *testing.T) {
	t.Parallel()
	archive := buildArchive(t,
		entry{name: "maildir/allen-p/inbox/1.", body: "ok"},
		entry{name: "maildir/allen-p/link", typ: tar.TypeSymlink, linkname: "/etc/passwd"},
		entry{name: "maildir/allen-p/up", typ: tar.TypeSymlink, linkname: "../../.."},
		entry{name: "maildir/allen-p/hard", typ: tar.TypeLink, linkname: "/etc/passwd"},
		// A file "through" the skipped symlink must not follow it.
		entry{name: "maildir/allen-p/up/evil.txt", body: "evil"},
	)
	parent, dir, ex, err := extractInto(t, archive)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if links := symlinksUnder(t, parent); len(links) > 0 {
		t.Errorf("symlinks created: %v", links)
	}
	assertNotExist(t, filepath.Join(dir, "maildir", "allen-p", "hard"))
	assertNotExist(t, filepath.Join(parent, "evil.txt"))
	if got := ex.skipped[skipLink]; got != 3 {
		t.Errorf("skipped[%s] = %d, want 3", skipLink, got)
	}
	// "up" was never created as a link, so up/evil.txt is a plain file inside the maildir.
	assertFile(t, filepath.Join(dir, "maildir", "allen-p", "up", "evil.txt"), "evil")
}

func TestExtract_SpecialFilesAreSkipped(t *testing.T) {
	t.Parallel()
	archive := buildArchive(t,
		entry{name: "maildir/a/1.", body: "ok"},
		entry{name: "maildir/a/fifo", typ: tar.TypeFifo},
		entry{name: "maildir/a/dev", typ: tar.TypeChar},
		entry{name: "maildir/a/blk", typ: tar.TypeBlock},
	)
	_, dir, ex, err := extractInto(t, archive)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	for _, n := range []string{"fifo", "dev", "blk"} {
		assertNotExist(t, filepath.Join(dir, "maildir", "a", n))
	}
	if got := ex.skipped[skipUnsupported]; got != 3 {
		t.Errorf("skipped[%s] = %d, want 3", skipUnsupported, got)
	}
}

func TestExtract_StripsTopLevelPrefixAndIgnoresArchiveModes(t *testing.T) {
	t.Parallel()
	archive := buildArchive(t,
		entry{name: "enron_mail_20150507/", typ: tar.TypeDir, mode: 0o777},
		entry{name: "enron_mail_20150507/README", body: "not mail"},
		entry{name: "enron_mail_20150507/maildir/", typ: tar.TypeDir, mode: 0o700},
		entry{name: "enron_mail_20150507/maildir/lay-k/sent/1.", body: "mail", mode: 0o4777},
	)
	_, dir, ex, err := extractInto(t, archive)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	file := filepath.Join(dir, "maildir", "lay-k", "sent", "1.")
	assertFile(t, file, "mail")
	assertNotExist(t, filepath.Join(dir, "enron_mail_20150507"))
	assertNotExist(t, filepath.Join(dir, "README"))
	fi, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&(os.ModeSetuid|0o111|0o022) != 0 {
		t.Errorf("file mode = %v, want at most 0644", fi.Mode())
	}
	di, err := os.Stat(filepath.Join(dir, "maildir"))
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode().Perm()&0o500 != 0o500 || di.Mode().Perm()&0o022 != 0 {
		t.Errorf("dir mode = %v, want 0755 (minus umask)", di.Mode())
	}
	if got := ex.skipped[skipOutsideMaildir]; got != 2 {
		t.Errorf("skipped[%s] = %d, want 2", skipOutsideMaildir, got)
	}
}

func TestExtract_PathConflictIsSkipped(t *testing.T) {
	t.Parallel()
	archive := buildArchive(t,
		entry{name: "maildir/a/x", body: "file"},
		entry{name: "maildir/a/x/1.", body: "under a file"},
		entry{name: "maildir/a/d/1.", body: "ok"},
		entry{name: "maildir/a/d", body: "file over a dir"},
		entry{name: "maildir/a/2.", body: "after"},
	)
	_, dir, ex, err := extractInto(t, archive)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	assertFile(t, filepath.Join(dir, "maildir", "a", "x"), "file")
	assertFile(t, filepath.Join(dir, "maildir", "a", "d", "1."), "ok")
	assertFile(t, filepath.Join(dir, "maildir", "a", "2."), "after")
	if got := ex.skipped[skipPathConflict]; got != 2 {
		t.Errorf("skipped[%s] = %d, want 2", skipPathConflict, got)
	}
}

func TestExtract_DuplicateFileLastWins(t *testing.T) {
	t.Parallel()
	archive := buildArchive(t,
		entry{name: "maildir/a/1.", body: "first, longer"},
		entry{name: "maildir/a/1.", body: "second"},
	)
	_, dir, _, err := extractInto(t, archive)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	assertFile(t, filepath.Join(dir, "maildir", "a", "1."), "second")
}

func TestExtract_Errors(t *testing.T) {
	t.Parallel()
	good := sampleArchive(t)
	corrupt := bytes.Clone(good)
	corrupt[len(corrupt)-8] ^= 0xff // flip a CRC32 byte in the gzip trailer.

	tests := []struct {
		name    string
		archive []byte
		want    string
	}{
		{name: "truncated halfway", archive: good[:len(good)/2], want: "unexpected EOF"},
		{name: "truncated trailer", archive: good[:len(good)-4], want: "unexpected EOF"},
		{name: "bad checksum", archive: corrupt, want: "checksum"},
		{name: "not gzip", archive: []byte("<html>not found</html>"), want: "reading archive"},
		{name: "empty", archive: nil, want: "reading archive"},
		{name: "no maildir", archive: buildArchive(t, entry{name: "other/1.", body: "x"}), want: "no maildir"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, _, err := extractInto(t, tt.archive)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("extract error = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestExtract_SizeLimit(t *testing.T) {
	t.Parallel()
	archive := buildArchive(t,
		entry{name: "maildir/a/1.", body: strings.Repeat("x", 600)},
		entry{name: "maildir/a/2.", body: strings.Repeat("x", 600)},
	)
	dir := t.TempDir()
	var files atomic.Int64
	ex := newExtractor(dir, slog.New(slog.DiscardHandler), newMetrics(nil, slog.New(slog.DiscardHandler)), &files)
	ex.limit = 1000
	if err := ex.extract(bytes.NewReader(archive)); !errors.Is(err, errTooLarge) {
		t.Errorf("extract error = %v, want errTooLarge", err)
	}
}

func TestExtract_TrailingGarbageAfterTarIsRejected(t *testing.T) {
	t.Parallel()
	// A gzip stream whose payload is a valid tar followed by more than
	// maxTrailingBytes of extra data.
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	if err := tw.WriteHeader(&tar.Header{Name: "maildir/1.", Typeflag: tar.TypeReg, Size: 1, Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	payload := io.MultiReader(&tarBuf, bytes.NewReader(make([]byte, maxTrailingBytes+1)))
	archive := gzipBytes(t, payload)
	_, _, _, err := extractInto(t, archive)
	if err == nil || !strings.Contains(err.Error(), "after the end of the tar archive") {
		t.Errorf("extract error = %v, want trailing data error", err)
	}
}

func gzipBytes(t *testing.T, r io.Reader) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := io.Copy(gz, r); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
