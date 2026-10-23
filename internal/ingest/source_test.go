package ingest

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func mkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o755); err != nil { //nolint:gosec // test fixture.
		t.Fatal(err)
	}
}
