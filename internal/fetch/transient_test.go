package fetch

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
)

// Scenario: Running out of file descriptors is transient (#49). A maildir
// check that fails with a transient errno makes POST /fetch fail with
// ErrTransient (503), starts no download and leaves the dataset state as it
// was; the next successful check reports the real state again.
func TestFetch_TransientCheckErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// setup creates the maildir (or not) and returns the expected
		// state and error before and during the fault.
		setup     func(t *testing.T, maildir string) (State, string)
		failLstat syscall.Errno // injected into lstat, if not 0
		failOpen  syscall.Errno // injected into openDir, if not 0
	}{
		{name: "ready, lstat EMFILE", setup: readyMaildir, failLstat: syscall.EMFILE},
		{name: "ready, lstat EAGAIN", setup: readyMaildir, failLstat: syscall.EAGAIN},
		{name: "ready, open EMFILE", setup: readyMaildir, failOpen: syscall.EMFILE},
		{name: "ready, open ENFILE", setup: readyMaildir, failOpen: syscall.ENFILE},
		{name: "ready, open ENOMEM", setup: readyMaildir, failOpen: syscall.ENOMEM},
		{name: "missing, lstat ENOMEM", setup: missingMaildir, failLstat: syscall.ENOMEM},
		{name: "symlink, lstat EINTR", setup: symlinkedMaildir, failLstat: syscall.EINTR},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			maildir := filepath.Join(dataDir, MaildirName)
			wantState, wantErr := tt.setup(t, maildir)
			srv := newArchiveServer(t, serveBytes(sampleArchive(t)))
			f, _ := newTestFetcher(t, dataDir, srv.URL, srv.Client())
			if p := f.Progress(); p.State != wantState || p.Error != wantErr {
				t.Fatalf("before the fault: progress = %q %q, want %q %q", p.State, p.Error, wantState, wantErr)
			}

			var faulty atomic.Bool
			faulty.Store(true)
			f.lstat = func(name string) (fs.FileInfo, error) {
				if faulty.Load() && tt.failLstat != 0 {
					return nil, &fs.PathError{Op: "lstat", Path: name, Err: tt.failLstat}
				}
				return os.Lstat(name)
			}
			f.openDir = func(name string) (*os.File, error) {
				if faulty.Load() && tt.failOpen != 0 {
					return nil, &fs.PathError{Op: "open", Path: name, Err: tt.failOpen}
				}
				return os.Open(name) //nolint:gosec // test fixture.
			}

			p, err := f.Start()
			if !errors.Is(err, ErrTransient) || errors.Is(err, ErrMaildirUnreadable) || errors.Is(err, ErrMaildirNotDirectory) {
				t.Fatalf("Start error = %v, want only ErrTransient", err)
			}
			if p.State != wantState || p.Error != wantErr {
				t.Errorf("Start progress = %q %q, want it unchanged: %q %q", p.State, p.Error, wantState, wantErr)
			}
			if p := f.Progress(); p.State != wantState || p.Error != wantErr {
				t.Errorf("Progress = %q %q, want it unchanged: %q %q", p.State, p.Error, wantState, wantErr)
			}
			if n := srv.requests.Load(); n != 0 {
				t.Errorf("requests = %d, want 0 (no download on a transient error)", n)
			}
			assertNoTempDirs(t, dataDir)

			faulty.Store(false)
			if p := f.Progress(); p.State != wantState || p.Error != wantErr {
				t.Errorf("after the fault: progress = %q %q, want %q %q", p.State, p.Error, wantState, wantErr)
			}
		})
	}
}

// A maildir that becomes unusable while checks fail transiently is reported
// once the checks succeed again: the transient error only postpones the
// refresh.
func TestProgress_RefreshesAfterTransientErrors(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	maildir := filepath.Join(dataDir, MaildirName)
	readyMaildir(t, maildir)
	f, _ := newTestFetcher(t, dataDir, "http://127.0.0.1:1/unused", nil)

	var faulty atomic.Bool
	faulty.Store(true)
	f.lstat = func(name string) (fs.FileInfo, error) {
		if faulty.Load() {
			return nil, &fs.PathError{Op: "lstat", Path: name, Err: syscall.EMFILE}
		}
		return os.Lstat(name)
	}
	if err := os.Remove(maildir); err != nil {
		t.Fatal(err)
	}
	if p := f.Progress(); p.State != StateReady {
		t.Errorf("during the fault: state = %q, want ready (unchanged)", p.State)
	}
	faulty.Store(false)
	if p := f.Progress(); p.State != StateAbsent || p.Error != "" {
		t.Errorf("after the fault: progress = %q %q, want absent", p.State, p.Error)
	}
}

func readyMaildir(t *testing.T, maildir string) (State, string) {
	t.Helper()
	if err := os.Mkdir(maildir, 0o755); err != nil { //nolint:gosec // test fixture.
		t.Fatal(err)
	}
	return StateReady, ""
}

func missingMaildir(*testing.T, string) (State, string) { return StateAbsent, "" }

func symlinkedMaildir(t *testing.T, maildir string) (State, string) {
	t.Helper()
	symlink(t, t.TempDir(), maildir)
	return StateAbsent, "maildir path exists and is not a directory: " + maildir + " is a symlink, which is not followed"
}
