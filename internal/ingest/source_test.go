package ingest

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Available tells a missing root (call POST /fetch) from an existing root
// that cannot be used (#45): only the latter is ErrDatasetInvalid, with a
// problem an operator can act on. Both are ErrDatasetMissing (ADR 0011).
func TestMaildirSourceAvailable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// make creates the root at path (or not).
		make        func(t *testing.T, path string)
		wantInvalid bool
		// wantProblem is a substring of InvalidRootError.Problem.
		wantProblem string
	}{
		{
			name: "directory",
			make: func(t *testing.T, path string) { t.Helper(); mkdir(t, path) },
		},
		{
			name:        "missing",
			make:        func(*testing.T, string) {},
			wantInvalid: false,
		},
		{
			name: "symlink to a directory",
			make: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Symlink(smallMaildir(t), path); err != nil {
					t.Fatal(err)
				}
			},
			wantInvalid: true,
			wantProblem: "it is a symlink, which is not followed",
		},
		{
			name: "dangling symlink",
			make: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Symlink("/nonexistent-target", path); err != nil {
					t.Fatal(err)
				}
			},
			wantInvalid: true,
			wantProblem: "it is a symlink",
		},
		{
			name: "regular file",
			make: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			wantInvalid: true,
			wantProblem: "it is not a directory (mode -rw-------)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), "maildir")
			tt.make(t, root)
			err := NewMaildirSource(root).Available()
			switch {
			case tt.name == "directory":
				if err != nil {
					t.Fatalf("Available = %v, want nil", err)
				}
				return
			case !errors.Is(err, ErrDatasetMissing):
				t.Fatalf("Available = %v, want ErrDatasetMissing", err)
			case errors.Is(err, ErrDatasetInvalid) != tt.wantInvalid:
				t.Fatalf("Available = %v, ErrDatasetInvalid = %v, want %v", err, !tt.wantInvalid, tt.wantInvalid)
			}
			inv, ok := errors.AsType[*InvalidRootError](err)
			if ok != tt.wantInvalid {
				t.Fatalf("Available = %v, *InvalidRootError = %v, want %v", err, ok, tt.wantInvalid)
			}
			if !ok {
				return
			}
			if inv.Path != root || !strings.Contains(inv.Problem, tt.wantProblem) {
				t.Errorf("InvalidRootError = %+v, want path %s and a problem containing %q", inv, root, tt.wantProblem)
			}
			if !strings.Contains(err.Error(), root) || !strings.Contains(err.Error(), tt.wantProblem) {
				t.Errorf("Error() = %q, want the path and the problem", err)
			}
		})
	}
}

func TestMaildirSourceAvailableUnreadableRoot(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root reads any directory")
	}
	root := filepath.Join(t.TempDir(), "maildir")
	mkdir(t, root)
	if err := os.Chmod(root, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) }) //nolint:gosec // restore so TempDir cleanup works.

	err := NewMaildirSource(root).Available()
	inv, ok := errors.AsType[*InvalidRootError](err)
	if !ok || !errors.Is(err, ErrDatasetInvalid) || !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Available = %v, want an *InvalidRootError wrapping the permission error", err)
	}
	if inv.Problem != "it cannot be read: permission denied" {
		t.Errorf("Problem = %q", inv.Problem)
	}
}

// #49: a root check that fails with a transient errno (out of file
// descriptors, ...) says nothing about the dataset. It is ErrTransient, not
// ErrDatasetMissing, so POST /start answers 503 and not 409.
func TestMaildirSourceTransientErrors(t *testing.T) {
	t.Parallel()
	inject := func(errno syscall.Errno) error {
		return &fs.PathError{Op: "open", Path: "maildir", Err: errno}
	}
	tests := []struct {
		name     string
		lstat    error // returned by lstat, if set
		openRoot error // returned by openRoot, if set
	}{
		{name: "lstat EMFILE", lstat: inject(syscall.EMFILE)},
		{name: "lstat ENFILE", lstat: inject(syscall.ENFILE)},
		{name: "lstat ENOMEM", lstat: inject(syscall.ENOMEM)},
		{name: "lstat EAGAIN", lstat: inject(syscall.EAGAIN)},
		{name: "lstat EINTR", lstat: inject(syscall.EINTR)},
		{name: "open EMFILE", openRoot: inject(syscall.EMFILE)},
		{name: "open ENFILE", openRoot: inject(syscall.ENFILE)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			src := NewMaildirSource(smallMaildir(t)).(*maildirSource)
			if tt.lstat != nil {
				src.lstat = func(string) (fs.FileInfo, error) { return nil, tt.lstat }
			}
			if tt.openRoot != nil {
				src.openRoot = func(string) (*os.Root, error) { return nil, tt.openRoot }
			}
			var visited int
			for name, err := range map[string]error{
				"Available": src.Available(),
				"Walk":      src.Walk(t.Context(), func(string, OpenFunc) error { visited++; return nil }),
			} {
				if !errors.Is(err, ErrTransient) || errors.Is(err, ErrDatasetMissing) {
					t.Errorf("%s = %v, want ErrTransient and not ErrDatasetMissing", name, err)
				}
				if _, ok := errors.AsType[*InvalidRootError](err); ok {
					t.Errorf("%s = %v, want no *InvalidRootError", name, err)
				}
			}
			if visited != 0 {
				t.Errorf("visited %d entries, want 0", visited)
			}
		})
	}
}

// #45 review: a root swapped for a symlink between the check and the open
// fails the walk instead of walking the symlink's target (or completing with
// 0 files).
func TestMaildirSourceRootSwappedBeforeOpen(t *testing.T) {
	t.Parallel()
	root := movedMaildir(t)
	intruder := intruderMaildir(t)
	src := NewMaildirSource(root).(*maildirSource)
	var once sync.Once
	src.lstat = func(name string) (fs.FileInfo, error) {
		fi, err := os.Lstat(name)
		once.Do(func() { swapForSymlink(t, root, intruder) })
		return fi, err
	}

	visited, err := walkAndRead(t, src)
	if !errors.Is(err, ErrDatasetInvalid) || !errors.Is(err, ErrDatasetMissing) {
		t.Fatalf("Walk = %v, want ErrDatasetInvalid", err)
	}
	if !strings.Contains(err.Error(), "dataset root is not usable: "+root+": it is a symlink") {
		t.Errorf("Walk error = %q, want it to name the symlinked root", err)
	}
	if len(visited) != 0 {
		t.Errorf("visited %v, want nothing", visited)
	}
}

// #49: the root is swapped for a symlink after the check and swapped back
// before the re-check. The opened directory is not the checked one, and the
// path looks fine again: the walk still fails ("it changed").
func TestMaildirSourceRootSwappedAndBackBeforeOpen(t *testing.T) {
	t.Parallel()
	root := movedMaildir(t)
	intruder := intruderMaildir(t)
	src := NewMaildirSource(root).(*maildirSource)
	var once sync.Once
	src.lstat = func(name string) (fs.FileInfo, error) {
		fi, err := os.Lstat(name)
		once.Do(func() { swapForSymlink(t, root, intruder) })
		return fi, err
	}
	src.openRoot = func(name string) (*os.Root, error) {
		r, err := os.OpenRoot(name) // opens the intruder through the symlink
		if err := os.Remove(root); err != nil {
			t.Error(err)
		}
		if err := os.Rename(root+".orig", root); err != nil {
			t.Error(err)
		}
		return r, err
	}

	visited, err := walkAndRead(t, src)
	if !errors.Is(err, ErrDatasetInvalid) {
		t.Fatalf("Walk = %v, want ErrDatasetInvalid", err)
	}
	if want := "dataset root is not usable: " + root + ": it changed while it was being opened"; err.Error() != want {
		t.Errorf("Walk error = %q, want %q", err, want)
	}
	if len(visited) != 0 {
		t.Errorf("visited %v, want nothing", visited)
	}
}

// Scenario: Root swapped mid-walk is not followed (#49). The walk goes on in
// the directory it opened: every original file is read, none of the new
// target's.
func TestMaildirSourceRootSwappedMidWalkIsNotFollowed(t *testing.T) {
	t.Parallel()
	root := movedMaildir(t)
	intruder := intruderMaildir(t)
	src := NewMaildirSource(root)

	var once sync.Once
	var visited []string
	err := src.Walk(t.Context(), func(rel string, open OpenFunc) error {
		once.Do(func() { swapForSymlink(t, root, intruder) })
		visited = append(visited, rel)
		body := read(t, rel, open)
		if strings.Contains(body, "INTRUDER") {
			t.Errorf("%s: read a file of the symlink's target", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk = %v, want nil", err)
	}
	if !slices.Equal(visited, smallFiles) {
		t.Errorf("visited %v, want the original files %v", visited, smallFiles)
	}
}

// An error below the root ends the walk only if the root path no longer
// names the opened directory; otherwise it is one skipped entry (#49).
func TestMaildirSourceRootLostMidWalk(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root reads any directory")
	}
	tests := []struct {
		name string
		// swap replaces the root path while the walk is in allen-p/.
		swap        func(t *testing.T, root string)
		wantErr     string // "" means the walk completes
		wantVisited []string
	}{
		{
			name:        "root unchanged",
			swap:        func(*testing.T, string) {},
			wantVisited: []string{"allen-p/inbox/1.", "secret", "zz-p/inbox/1."},
		},
		{
			name: "replaced by a symlink",
			swap: func(t *testing.T, root string) {
				t.Helper()
				swapForSymlink(t, root, intruderMaildir(t))
			},
			wantErr:     "it is a symlink, which is not followed",
			wantVisited: []string{"allen-p/inbox/1."},
		},
		{
			name: "replaced by another directory",
			swap: func(t *testing.T, root string) {
				t.Helper()
				if err := os.Rename(root, root+".orig"); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(intruderMaildir(t), root); err != nil {
					t.Fatal(err)
				}
			},
			wantErr:     "it changed during the walk",
			wantVisited: []string{"allen-p/inbox/1."},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), "maildir")
			writeFile(t, root, "allen-p/inbox/1.", message(1, "phillip.allen@enron.com"))
			writeFile(t, root, "zz-p/inbox/1.", message(2, "phillip.allen@enron.com"))
			unreadableDir(t, filepath.Join(root, "secret"))

			var visited []string
			err := NewMaildirSource(root).Walk(t.Context(), func(rel string, _ OpenFunc) error {
				if rel == "allen-p/inbox/1." {
					tt.swap(t, root)
				}
				visited = append(visited, rel)
				return nil
			})
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("Walk = %v, want nil", err)
			case tt.wantErr != "" && (!errors.Is(err, ErrDatasetInvalid) || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("Walk = %v, want ErrDatasetInvalid with %q", err, tt.wantErr)
			}
			if !slices.Equal(visited, tt.wantVisited) {
				t.Errorf("visited %v, want %v", visited, tt.wantVisited)
			}
		})
	}
}

// A transient error while re-checking the root after an error below it does
// not end the walk: the entry is skipped (#49).
func TestMaildirSourceTransientRecheckMidWalk(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root reads any directory")
	}
	root := filepath.Join(t.TempDir(), "maildir")
	writeFile(t, root, "allen-p/inbox/1.", message(1, "phillip.allen@enron.com"))
	unreadableDir(t, filepath.Join(root, "secret"))
	src := NewMaildirSource(root).(*maildirSource)
	var calls atomic.Int32
	src.lstat = func(name string) (fs.FileInfo, error) {
		if calls.Add(1) > 1 {
			return nil, &fs.PathError{Op: "lstat", Path: name, Err: syscall.ENOMEM}
		}
		return os.Lstat(name)
	}

	var skipped []string
	err := src.Walk(t.Context(), func(rel string, open OpenFunc) error {
		if _, err := open(); errors.Is(err, ErrUnreadableDir) {
			skipped = append(skipped, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk = %v, want nil", err)
	}
	if !slices.Equal(skipped, []string{"secret"}) {
		t.Errorf("skipped %v, want [secret]", skipped)
	}
}

// The workers open files after the walker has yielded them, possibly after
// Walk has returned: the root stays open for them, and is closed once every
// open function has been called.
func TestMaildirSourceOpenAfterWalkReturns(t *testing.T) {
	t.Parallel()
	src := NewMaildirSource(smallMaildir(t))
	opens := map[string]OpenFunc{}
	err := src.Walk(t.Context(), func(rel string, open OpenFunc) error {
		opens[rel] = open
		return nil
	})
	if err != nil {
		t.Fatalf("Walk = %v", err)
	}
	for _, rel := range smallFiles {
		if body := read(t, rel, opens[rel]); !strings.Contains(body, "Body of message") {
			t.Errorf("%s = %q", rel, body)
		}
	}
	if _, err := opens[smallFiles[0]](); !errors.Is(err, os.ErrClosed) {
		t.Errorf("open after every file was opened = %v, want os.ErrClosed (root released)", err)
	}
}

// Open functions that are never called (a canceled run) do not keep the root
// open past the walk's context.
func TestMaildirSourceRootClosedWhenContextDone(t *testing.T) {
	t.Parallel()
	src := NewMaildirSource(smallMaildir(t))
	ctx, cancel := context.WithCancel(t.Context())
	var open OpenFunc
	err := src.Walk(ctx, func(_ string, o OpenFunc) error {
		if open == nil {
			open = o
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk = %v", err)
	}
	cancel()
	// The close runs in context.AfterFunc's goroutine.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := open()
		if errors.Is(err, os.ErrClosed) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("open after cancel = %v, want os.ErrClosed", err)
		}
		time.Sleep(time.Millisecond)
	}
}

// movedMaildir returns a small maildir at <tmp>/maildir whose parent also
// has room for <root>.orig.
func movedMaildir(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "maildir")
	if err := os.Rename(smallMaildir(t), root); err != nil {
		t.Fatal(err)
	}
	return root
}

// intruderMaildir returns a maildir whose files must never be read through
// the original root: their names and bodies say INTRUDER.
func intruderMaildir(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "maildir")
	for _, rel := range append([]string{"intruder-x/inbox/1."}, smallFiles...) {
		writeFile(t, root, rel, "From: intruder@example.com\n\nINTRUDER\n")
	}
	return root
}

// swapForSymlink moves root to root.orig (the walk may still hold it open)
// and puts a symlink to target in its place.
func swapForSymlink(t *testing.T, root, target string) {
	t.Helper()
	if err := os.Rename(root, root+".orig"); err != nil {
		t.Error(err)
		return
	}
	if err := os.Symlink(target, root); err != nil {
		t.Error(err)
	}
}

// unreadableDir creates a directory the test process cannot read.
func unreadableDir(t *testing.T, path string) {
	t.Helper()
	mkdir(t, path)
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o755) }) //nolint:gosec // restore so TempDir cleanup works.
}

// walkAndRead walks src, reading every file, and returns the files visited.
func walkAndRead(t *testing.T, src Source) ([]string, error) {
	t.Helper()
	var visited []string
	err := src.Walk(t.Context(), func(rel string, open OpenFunc) error {
		visited = append(visited, rel)
		_ = read(t, rel, open)
		return nil
	})
	return visited, err
}

func read(t *testing.T, rel string, open OpenFunc) string {
	t.Helper()
	rc, err := open()
	if err != nil {
		t.Errorf("opening %s: %v", rel, err)
		return ""
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Errorf("reading %s: %v", rel, err)
	}
	return string(b)
}

func mkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o755); err != nil { //nolint:gosec // test fixture.
		t.Fatal(err)
	}
}
