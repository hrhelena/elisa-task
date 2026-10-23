package ingest

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Fixtures are generated into t.TempDir rather than checked into testdata/:
// they include an empty file, binary bytes, a symlink and an unreadable
// directory, which do not survive git and the pre-commit hooks reliably.

// message returns a well-formed maildir message from sender.
func message(n int, from string) string {
	return fmt.Sprintf("Message-ID: <%d.1075855687451.JavaMail.evans@thyme>\n"+
		"Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)\n"+
		"From: %s\n"+
		"To: tim.belden@enron.com\n"+
		"Subject: Report %d\n"+
		"X-From: Someone\n"+
		"\n"+
		"Body of message %d.\n", n, from, n, n)
}

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// smallFiles are the 5 files of the "small maildir" scenario.
var smallFiles = []string{
	"allen-p/_sent_mail/1.",
	"allen-p/_sent_mail/2.",
	"allen-p/inbox/1.",
	"lay-k/inbox/1.",
	"lay-k/inbox/2.",
}

// smallMaildir writes mailboxes allen-p (3 files) and lay-k (2 files).
func smallMaildir(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "maildir")
	for i, rel := range smallFiles {
		from := "phillip.allen@enron.com"
		if strings.HasPrefix(rel, "lay-k/") {
			from = "kenneth.lay@enron.com"
		}
		writeFile(t, root, rel, message(i, from))
	}
	return root
}

// malformedMaildir writes 4 valid messages, an empty file, a binary file, a
// message with an unusable sender and one with an unparseable date.
func malformedMaildir(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "maildir")
	for i := range 4 {
		writeFile(t, root, fmt.Sprintf("allen-p/inbox/%d.", i+1), message(i, "phillip.allen@enron.com"))
	}
	writeFile(t, root, "allen-p/inbox/empty.", "")
	writeFile(t, root, "allen-p/inbox/binary.", "\x00\x01\x02\xff\xfe\x89PNG\r\n\x1a\n\x00\x00")
	writeFile(t, root, "allen-p/inbox/pep.", strings.Replace(message(5, "x@y.z"), "From: x@y.z",
		"From: pep <performance.>", 1))
	writeFile(t, root, "allen-p/inbox/date.", strings.Replace(message(6, "phillip.allen@enron.com"),
		"Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)", "Date: garbage", 1))
	return root
}

// memFile is one entry of a memSource.
type memFile struct {
	rel     string
	content string
	openErr error
}

// memSource is an in-memory Source. If blockAfter > 0, Walk blocks after
// yielding that many entries until ctx ends, like a very slow file system.
type memSource struct {
	files      []memFile
	missing    bool
	blockAfter int
	delay      time.Duration // before each entry
}

func (s *memSource) Available() error {
	if s.missing {
		return fmt.Errorf("%w: memory source", ErrDatasetMissing)
	}
	return nil
}

func (s *memSource) Walk(ctx context.Context, fn func(string, OpenFunc) error) error {
	if err := s.Available(); err != nil {
		return err
	}
	for i, f := range s.files {
		if s.blockAfter > 0 && i == s.blockAfter {
			<-ctx.Done()
			return ctx.Err()
		}
		if s.delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.delay):
			}
		}
		open := func() (io.ReadCloser, error) {
			if f.openErr != nil {
				return nil, f.openErr
			}
			return io.NopCloser(strings.NewReader(f.content)), nil
		}
		if err := fn(f.rel, open); err != nil {
			return err
		}
	}
	return nil
}

// memMessages returns n valid entries spread over two mailboxes.
func memMessages(n int) []memFile {
	files := make([]memFile, n)
	for i := range files {
		files[i] = memFile{rel: fmt.Sprintf("mbox-%d/inbox/%d.", i%2, i), content: message(i, "a.b@enron.com")}
	}
	return files
}
