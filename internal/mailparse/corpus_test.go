package mailparse_test

import (
	"errors"
	"io/fs"
	"net/mail"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/hrhelena/elisa-task/internal/mailparse"
)

// TestCorpus parses every file of a local maildir and reports totals per outcome. It is skipped
// unless ENRON_MAILDIR points at the dataset, for example:
//
//	ENRON_MAILDIR=$PWD/Dataset/maildir go test -run TestCorpus -v ./internal/mailparse
//
// It fails only if Parse returns an error that is not a *SkipError.
func TestCorpus(t *testing.T) {
	root := os.Getenv("ENRON_MAILDIR")
	if root == "" {
		t.Skip("ENRON_MAILDIR not set")
	}

	fsys := os.DirFS(root)
	paths := make(chan string, 1024)
	var (
		mu       sync.Mutex
		files    int
		messages int
		lenient  int // From accepted only by the lenient fallback (ADR 0012)
		dated    int
		issues   = map[string]int{}
		skips    = map[string]int{}
		samples  = map[string][]string{}
	)
	sample := func(key, s string) {
		if len(samples[key]) < 5 {
			samples[key] = append(samples[key], s)
		}
	}

	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			for rel := range paths {
				m, err := parseFile(fsys, rel)
				mu.Lock()
				files++
				var se *mailparse.SkipError
				switch {
				case err == nil:
					messages++
					if m.From != "" {
						if _, perr := mail.ParseAddress(m.FromRaw); perr != nil {
							lenient++
							sample("lenient_sender", m.FromRaw+" -> "+m.From)
						}
					}
					if !m.Date.IsZero() {
						dated++
					}
					for _, is := range m.Issues {
						issues[string(is)]++
						sample(string(is), rel+" from="+m.FromRaw+" date="+m.DateRaw)
					}
				case errors.As(err, &se):
					skips[string(se.Reason)]++
					sample(string(se.Reason), rel+": "+se.Error())
				default:
					t.Errorf("%s: unexpected error: %v", rel, err)
				}
				mu.Unlock()
			}
		})
	}

	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			paths <- p
		}
		return nil
	})
	close(paths)
	wg.Wait()
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}

	t.Logf("files=%d messages=%d skipped=%d with_date=%d lenient_sender=%d e.g. %q",
		files, messages, files-messages, dated, lenient, samples["lenient_sender"])
	for _, m := range []map[string]int{skips, issues} {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			t.Logf("  %-18s %7d  e.g. %q", k, m[k], samples[k])
		}
	}
}

func parseFile(fsys fs.FS, name string) (*mailparse.Message, error) {
	f, err := fsys.Open(name)
	if err != nil {
		return nil, &mailparse.SkipError{Reason: mailparse.SkipReadError, Err: err}
	}
	defer f.Close()
	return mailparse.Parse(f)
}
