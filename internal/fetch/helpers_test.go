package fetch

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// entry is one archive member for buildArchive. A zero typ means a regular file.
type entry struct {
	name     string
	typ      byte
	body     string
	linkname string
	mode     int64
}

// buildArchive returns a tar.gz with the given entries, built in memory.
func buildArchive(t *testing.T, entries ...entry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		hdr := &tar.Header{Name: e.name, Typeflag: e.typ, Linkname: e.linkname, Mode: e.mode, ModTime: time.Unix(0, 0)}
		if hdr.Typeflag == 0 {
			hdr.Typeflag = tar.TypeReg
		}
		if hdr.Mode == 0 {
			hdr.Mode = 0o644
		}
		if hdr.Typeflag == tar.TypeReg {
			hdr.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("writing tar header %q: %v", e.name, err)
		}
		if hdr.Typeflag == tar.TypeReg {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatalf("writing tar body %q: %v", e.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("closing tar: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("closing gzip: %v", err)
	}
	return buf.Bytes()
}

// sampleArchive is the archive from the "Fresh download" scenario.
func sampleArchive(t *testing.T) []byte {
	t.Helper()
	return buildArchive(t,
		entry{name: "maildir/", typ: tar.TypeDir},
		entry{name: "maildir/allen-p/", typ: tar.TypeDir},
		entry{name: "maildir/allen-p/inbox/", typ: tar.TypeDir},
		entry{name: "maildir/allen-p/inbox/1.", body: "From: phillip.allen@enron.com\n\nhello\n"},
		entry{name: "maildir/lay-k/sent/1.", body: "From: kenneth.lay@enron.com\n\nbye\n"},
	)
}

// archiveServer serves an archive and counts requests. Its handler can be
// replaced per test.
type archiveServer struct {
	*httptest.Server
	requests atomic.Int64
}

func newArchiveServer(t *testing.T, h http.HandlerFunc) *archiveServer {
	t.Helper()
	s := &archiveServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		h(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

// serveBytes answers with the whole archive and a Content-Length.
func serveBytes(b []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		_, _ = w.Write(b)
	}
}

// gate lets a handler send part of a response and wait until the test
// releases it (or the request is canceled).
type gate struct {
	sent    chan struct{} // closed once the first part was flushed.
	release chan struct{}
	once    sync.Once
}

func newGate() *gate {
	return &gate{sent: make(chan struct{}), release: make(chan struct{})}
}

func (g *gate) open() { g.once.Do(func() { close(g.release) }) }

// serveInTwoParts sends the first half of b, flushes, waits for the gate,
// then sends the rest. If the request is canceled first it just returns.
func serveInTwoParts(b []byte, g *gate) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		half := len(b) / 2
		_, _ = w.Write(b[:half])
		w.(http.Flusher).Flush() //nolint:forcetypeassert // httptest writers flush.
		close(g.sent)
		select {
		case <-g.release:
			_, _ = w.Write(b[half:])
		case <-r.Context().Done():
		}
	}
}

// newTestFetcher returns a Fetcher on dataDir with test-friendly defaults: no
// free space check, a private registry and Close on cleanup.
func newTestFetcher(t *testing.T, dataDir, url string, client *http.Client, opts ...Option) (*Fetcher, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	opts = append([]Option{WithMinFreeBytes(0), WithMinFreeInodes(0)}, opts...)
	f := New(dataDir, url, client, slog.New(slog.DiscardHandler), reg, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return f, reg
}

// waitForState polls until the fetcher reaches want or the timeout expires.
func waitForState(t *testing.T, f *Fetcher, want State) Progress {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		p := f.Progress()
		if p.State == want {
			return p
		}
		if time.Now().After(deadline) {
			t.Fatalf("state = %q (error %q), want %q", p.State, p.Error, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertNoTempDirs fails if any .fetch-tmp-* directory is left in dataDir.
func assertNoTempDirs(t *testing.T, dataDir string) {
	t.Helper()
	entries, err := os.ReadDir(dataDir) // not filepath.Glob: dataDir may contain glob characters.
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var matches []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), TempPrefix) {
			matches = append(matches, e.Name())
		}
	}
	if len(matches) > 0 {
		t.Errorf("temporary directories left behind: %v", matches)
	}
}

// assertNotExist fails if path exists.
func assertNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); err == nil {
		t.Errorf("%s exists, want it absent", path)
	} else if !os.IsNotExist(err) {
		t.Errorf("stat %s: %v", path, err)
	}
}

// assertFile fails unless path is a regular file with the given content.
func assertFile(t *testing.T, path, want string) {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // test path.
	if err != nil {
		t.Errorf("reading %s: %v", path, err)
		return
	}
	if string(b) != want {
		t.Errorf("%s = %q, want %q", path, b, want)
	}
}

// randomBody returns n incompressible bytes, so an offset in the gzip stream
// maps roughly to the same offset in the tar.
func randomBody(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return string(b)
}

// symlinksUnder returns every symlink below dir.
func symlinksUnder(t *testing.T, dir string) []string {
	t.Helper()
	var links []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 {
			links = append(links, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return links
}
