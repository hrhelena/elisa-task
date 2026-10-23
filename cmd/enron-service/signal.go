package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// beforeSignalStop runs in notifyContext between receiving the first signal
// and restoring default handling. Tests use it to land a second signal in
// that window; it is a no-op otherwise.
var beforeSignalStop = func() {}

// notifyContext is like signal.NotifyContext, except that the first signal
// also restores default handling of sigs, and does so before the returned
// context is canceled. Anything that observes the cancellation (such as the
// "shutting down" log line) therefore happens after the handover, so a
// second signal always takes the default action and ends the process at
// once. A second signal that arrives before the handover is re-raised under
// default handling, so there is no window in which it is lost.
//
// (signal.NotifyContext plus context.AfterFunc(ctx, stop) has such a window:
// AfterFunc runs stop on a new goroutine after ctx is already canceled, and
// a signal arriving before that goroutine runs is buffered in a channel that
// nobody reads any more.)
//
// stop releases the signals if none arrived and waits for the internal
// goroutine to return.
func notifyContext(parent context.Context, sigs ...os.Signal) (ctx context.Context, stop context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	// Room for the first signal and one more that arrives before Stop.
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, sigs...)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			signal.Stop(ch)
		case sig := <-ch:
			beforeSignalStop()
			// Once Stop returns, every signal received so far is in ch and
			// any later one gets the default action.
			signal.Stop(ch)
			select {
			case again := <-ch:
				reraise(again)
			default:
			}
			cancel(fmt.Errorf("%v signal received", sig))
		}
	}()
	return ctx, func() {
		cancel(nil)
		<-done
	}
}

// reraise sends sig to the process again, now that its default action
// (terminate) is restored.
func reraise(sig os.Signal) {
	if s, ok := sig.(syscall.Signal); ok {
		_ = syscall.Kill(os.Getpid(), s)
	}
}
