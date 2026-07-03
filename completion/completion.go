// Package completion provides a group-completion primitive for batched
// operations: a caller submits N items into one or more batchers and waits
// once for all N to complete, instead of spawning a goroutine, channel, and
// timer per item.
//
// A Group is intentionally minimal: an atomic counter and a single close
// channel. The batch dispatcher writes each item's result into a slot the
// caller owns, then calls Done() on the shared Group. The final Done() closes
// the channel Wait is parked on, publishing every slot write via the
// channel-close happens-before edge in the Go memory model.
//
// Callers must not read item result slots after Wait returns a non-nil
// error: a dispatcher may still be writing to a slot after the caller has
// given up waiting on it, so reading it back concurrently is a data race.
// Treat a Wait error as "the operation failed" and discard the slots.
package completion

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// Group tracks N outstanding batch items belonging to one logical call. The
// item that makes the counter reach zero closes done exactly once. Item
// results must be written to caller-owned slots before Done is called for
// that item; the close(done) edge publishes them to the waiting caller.
//
// A Group is used once: create it with NewGroup, call Done exactly once per
// item, and Wait (or read C()) exactly once (concurrent/repeated Wait calls
// are safe since they only ever receive from a channel, but a Group is not
// meant to be reset or reused).
type Group struct {
	remaining atomic.Int32
	done      chan struct{}
}

// NewGroup creates a Group expecting exactly n calls to Done. n must be >= 0;
// NewGroup panics if n is negative. A Group created with n == 0 starts
// already complete — C() is closed and Wait returns immediately.
func NewGroup(n int32) *Group {
	if n < 0 {
		panic(fmt.Sprintf("completion: NewGroup called with negative count %d", n))
	}

	g := &Group{done: make(chan struct{})}
	if n == 0 {
		close(g.done)
		return g
	}

	g.remaining.Store(n)

	return g
}

// Done marks one item complete. It must be called exactly once per item
// counted in NewGroup's n. Calling it more times than n panics: that is
// always a programming error (a double-signal on the same item, or a Group
// sized wrong), and by the time it happens done is already closed, so a
// second close would panic anyway — this makes the failure message clearer.
func (g *Group) Done() {
	switch v := g.remaining.Add(-1); {
	case v == 0:
		close(g.done)
	case v < 0:
		panic("completion: Group.Done called more times than the count passed to NewGroup")
	}
}

// C returns the channel that closes once every item has completed. Prefer
// Wait for the common case; C is exposed for callers that need to fold group
// completion into a larger select statement.
func (g *Group) C() <-chan struct{} {
	return g.done
}

// Wait blocks until every item has completed, ctx is done, or timeout
// elapses — whichever happens first. A timeout <= 0 disables the timeout arm
// entirely (no timer is allocated), so Wait then blocks on ctx and
// completion only, mirroring an unbounded per-item channel receive.
//
// On a non-nil return, the caller must not read item result slots: a
// dispatcher may still be writing to them after Wait has given up.
func (g *Group) Wait(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		select {
		case <-g.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("completion: Wait timed out after %s", timeout)
	}
}
