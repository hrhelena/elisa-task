package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// signalHelperEnv makes the test binary act as a tiny service that shuts
// down slowly on a signal (see signalHelper). Its value selects the mode.
const signalHelperEnv = "ENRON_SIGNAL_HELPER"

// Helper modes.
const (
	helperPlain = "plain"
	// helperRaiseInWindow sends itself a second SIGTERM between receiving
	// the first one and restoring default handling.
	helperRaiseInWindow = "raise-in-window"
)

// init dispatches to the helper before TestMain, so the child never runs
// test setup (such as the integration tests' Postgres container).
func init() {
	if mode := os.Getenv(signalHelperEnv); mode != "" {
		signalHelper(mode)
	}
}

// signalHelper mirrors main's lifecycle: it logs "ready" once signals are
// handled, and on the first signal logs "shutting down" and takes a minute
// to stop gracefully, logging "stopped" and exiting 0 if it gets there.
func signalHelper(mode string) {
	if mode == helperRaiseInWindow {
		beforeSignalStop = func() { _ = syscall.Kill(os.Getpid(), syscall.SIGTERM) }
	}
	ctx, stop := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	fmt.Println("ready")
	<-ctx.Done()
	fmt.Println("shutting down:", context.Cause(ctx))
	time.Sleep(time.Minute)
	fmt.Println("stopped")
	stop()
	os.Exit(0)
}

// signalProc is a running signal helper with its stdout split into lines.
type signalProc struct {
	cmd   *exec.Cmd
	lines chan string // closed at EOF
}

func startSignalHelper(t *testing.T, mode string) *signalProc {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0]) //nolint:gosec // re-runs this test binary.
	cmd.Env = []string{signalHelperEnv + "=" + mode}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting helper: %v", err)
	}
	p := &signalProc{cmd: cmd, lines: make(chan string, 16)}
	go func() {
		defer close(p.lines)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			p.lines <- sc.Text()
		}
	}()
	return p
}

// waitLine waits for a line starting with prefix and returns it.
func (p *signalProc) waitLine(t *testing.T, prefix string) string {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				t.Fatalf("helper exited before logging %q", prefix)
			}
			if strings.HasPrefix(line, prefix) {
				return line
			}
		case <-timeout:
			t.Fatalf("helper did not log %q", prefix)
		}
	}
}

func (p *signalProc) sigterm(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("sending SIGTERM: %v", err)
	}
}

// requireKilledBySIGTERM waits (bounded) for the helper to end and fails
// unless SIGTERM's default action killed it without a graceful "stopped".
func (p *signalProc) requireKilledBySIGTERM(t *testing.T) {
	t.Helper()
	var rest []string
	timeout := time.After(5 * time.Second)
	for drained := false; !drained; {
		select {
		case line, ok := <-p.lines:
			if ok {
				rest = append(rest, line)
			} else {
				drained = true
			}
		case <-timeout:
			t.Fatalf("helper still running 5s after the second SIGTERM; output: %q", rest)
		}
	}
	err := p.cmd.Wait()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("exit = %v, want termination by SIGTERM", err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
		t.Errorf("exit = %v, want killed by SIGTERM", err)
	}
	for _, line := range rest {
		if line == "stopped" {
			t.Error(`forced exit logged "stopped" as if the shutdown were graceful`)
		}
	}
}

// Scenario: Second signal forces exit, reliably. The "shutting down" line
// is the observable handover: once it is out, a second SIGTERM must kill the
// process, with no window in which it is swallowed.
func TestSecondSignalAfterShutdownStartsKillsProcess(t *testing.T) {
	t.Parallel()
	p := startSignalHelper(t, helperPlain)
	p.waitLine(t, "ready")

	p.sigterm(t)
	if line := p.waitLine(t, "shutting down"); !strings.Contains(line, "terminated signal received") {
		t.Errorf("shutdown line %q does not name the signal", line)
	}
	p.sigterm(t)
	p.requireKilledBySIGTERM(t)
}

// A second signal that arrives after the first was received but before
// default handling is restored is not lost: it is re-raised and kills the
// process.
func TestSecondSignalBeforeHandoverKillsProcess(t *testing.T) {
	t.Parallel()
	p := startSignalHelper(t, helperRaiseInWindow)
	p.waitLine(t, "ready")

	p.sigterm(t) // the helper sends the second one itself, inside the window
	p.requireKilledBySIGTERM(t)
}

// Without a signal, stop and parent cancellation release the signals and
// end notifyContext's goroutine (goleak in TestMain checks the latter).
func TestNotifyContextStopsWithoutSignal(t *testing.T) {
	t.Parallel()

	ctx, stop := notifyContext(context.Background(), syscall.SIGTERM)
	stop()
	stop() // idempotent
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Errorf("after stop, ctx.Err() = %v, want context.Canceled", ctx.Err())
	}

	parent, cancel := context.WithCancel(context.Background())
	ctx, stop = notifyContext(parent, syscall.SIGTERM)
	cancel()
	<-ctx.Done()
	stop()
}
