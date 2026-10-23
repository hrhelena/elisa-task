package ingest

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"
)

// #49 review: when a run's context ends, the maildir source closes its
// directory descriptors, so a worker that still dequeues a job fails to open
// it. That failure is not a read_error: the entry is dropped uncounted, and
// files_seen = processed + skipped still holds.
//
// Each directory closes in its own context.AfterFunc goroutine, so the test
// waits for every directory the jobs reference, not only the first (#57).
func TestProcessDropsEntriesFailingAfterCancel(t *testing.T) {
	t.Parallel()
	src := NewMaildirSource(cancelMaildir(t))
	ctx, cancel := context.WithCancel(t.Context())
	var jobs []job
	if err := src.Walk(ctx, func(rel string, open OpenFunc) error {
		jobs = append(jobs, job{rel: rel, open: open})
		return nil
	}); err != nil {
		t.Fatalf("Walk = %v", err)
	}
	probes := firstJobPerDir(t, jobs)
	if len(probes) < 2 {
		t.Fatalf("jobs span %d directories, want several", len(probes))
	}
	cancel()
	for _, j := range probes {
		waitClosed(t, j.open)
	}

	p := newTestPipeline()
	for _, j := range jobs {
		if _, ok := p.process(ctx, j); ok {
			t.Errorf("%s: processed after cancel", j.rel)
		}
	}
	got := p.c.snapshot()
	if got.FilesSeen != 0 || got.MessagesSkipped != 0 || len(got.SkipReasons) != 0 {
		t.Errorf("counters after cancel = %+v, want nothing counted", got)
	}
	if got.FilesSeen != got.MessagesProcessed+got.MessagesSkipped {
		t.Errorf("files_seen %d != processed %d + skipped %d", got.FilesSeen, got.MessagesProcessed, got.MessagesSkipped)
	}
}

// cancelMaildir writes two files in each of three directories across two
// mailboxes. Two files per directory matter: a directory also closes once
// every open function yielded for it has been called, so with a single file
// the probe in waitClosed would close the directory itself, and the test
// would pass even if cancellation closed nothing.
func cancelMaildir(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "maildir")
	for i, rel := range []string{
		"allen-p/_sent_mail/1.", "allen-p/_sent_mail/2.",
		"allen-p/inbox/1.", "allen-p/inbox/2.",
		"lay-k/inbox/1.", "lay-k/inbox/2.",
	} {
		writeFile(t, root, rel, message(i, "phillip.allen@enron.com"))
	}
	return root
}

// firstJobPerDir returns the first job of each distinct directory in jobs.
// It fails the test if a directory has fewer than two jobs (see
// cancelMaildir): only cancellation may close the directories it probes.
func firstJobPerDir(t *testing.T, jobs []job) []job {
	t.Helper()
	count := map[string]int{}
	var first []job
	for _, j := range jobs {
		dir := path.Dir(j.rel)
		if count[dir] == 0 {
			first = append(first, j)
		}
		count[dir]++
	}
	for dir, n := range count {
		if n < 2 {
			t.Fatalf("directory %s has %d job, want at least 2", dir, n)
		}
	}
	return first
}

// The same open failure in a live run is a real read_error and is counted.
func TestProcessCountsOpenFailuresInALiveRun(t *testing.T) {
	t.Parallel()
	p := newTestPipeline()
	failing := job{rel: "allen-p/inbox/1.", open: failOpen(os.ErrPermission)}
	if _, ok := p.process(t.Context(), failing); ok {
		t.Fatal("processed a file that cannot be opened")
	}
	got := p.c.snapshot()
	if got.FilesSeen != 1 || got.MessagesSkipped != 1 || got.SkipReasons["read_error"] != 1 {
		t.Errorf("counters = %+v, want one read_error skip", got)
	}
}

func newTestPipeline() *pipeline {
	s := NewService(nil, nil, Config{}, slog.New(slog.DiscardHandler), nil)
	return &pipeline{s: s, runID: 1, c: newCounters(), log: newSampler()}
}

// waitClosed waits until open fails with os.ErrClosed: the directory is
// closed by context.AfterFunc's goroutine.
func waitClosed(t *testing.T, open OpenFunc) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rc, err := open()
		if err == nil {
			_ = rc.Close()
		}
		if errors.Is(err, os.ErrClosed) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("open after cancel = %v, want os.ErrClosed", err)
		}
		time.Sleep(time.Millisecond)
	}
}
