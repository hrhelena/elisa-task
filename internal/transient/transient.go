// Package transient classifies operating-system errors that say nothing about
// the file being checked, only about the state of the process or the host at
// that moment (#49). A dataset check that fails with one of them is retried
// later; it does not mean the dataset is missing or unusable.
package transient

import (
	"errors"
	"syscall"
)

// errnos are the transient errnos: out of file descriptors (per process or
// system-wide), out of kernel memory, a resource temporarily unavailable, and
// an interrupted system call.
var errnos = [...]syscall.Errno{syscall.EMFILE, syscall.ENFILE, syscall.ENOMEM, syscall.EAGAIN, syscall.EINTR}

// Is reports whether err, or an error it wraps, is a transient errno.
func Is(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range errnos {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}
