package transient

import (
	"errors"
	"fmt"
	"io/fs"
	"syscall"
	"testing"
)

func TestIs(t *testing.T) {
	t.Parallel()
	pathErr := func(errno syscall.Errno) error {
		return &fs.PathError{Op: "open", Path: "/data/maildir", Err: errno}
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"EMFILE", pathErr(syscall.EMFILE), true},
		{"ENFILE", pathErr(syscall.ENFILE), true},
		{"ENOMEM", pathErr(syscall.ENOMEM), true},
		{"EAGAIN", pathErr(syscall.EAGAIN), true},
		{"EINTR", pathErr(syscall.EINTR), true},
		{"bare errno", syscall.EMFILE, true},
		{"wrapped twice", fmt.Errorf("checking: %w", pathErr(syscall.EMFILE)), true},
		{"joined", errors.Join(errors.New("x"), pathErr(syscall.ENFILE)), true},
		{"ENOENT", pathErr(syscall.ENOENT), false},
		{"EACCES", pathErr(syscall.EACCES), false},
		{"ENOTDIR", pathErr(syscall.ENOTDIR), false},
		{"EIO", pathErr(syscall.EIO), false},
		{"plain error", errors.New("too many open files"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Is(tt.err); got != tt.want {
				t.Errorf("Is(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
