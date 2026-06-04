package batcher

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestCloseBlocksUntilBackgroundDrainComplete verifies the core shutdown
// contract: Close must not return until every item accepted before Close has
// been handed to fn — including items dispatched on background goroutines with
// a slow callback. This is the guarantee teranode's UTXO store relies on to
// flush in-flight batched writes on SIGTERM. Against the old fire-and-forget
// Close (close(b.done) only) this test fails: Close returns before the slow
// background fn finishes, so processed < total.
func TestCloseBlocksUntilBackgroundDrainComplete(t *testing.T) {
	const total = 500

	var processed atomic.Int64

	// background=true, slow callback so the residual batch is still being
	// written when Close is called.
	b := New(16, 50*time.Millisecond, func(batch []*int) {
		time.Sleep(5 * time.Millisecond)
		processed.Add(int64(len(batch)))
	}, true, WithName("close_drain"))

	for i := 0; i < total; i++ {
		v := i
		b.Put(&v)
	}

	b.Close()

	if got := processed.Load(); got != total {
		t.Fatalf("Close returned before drain completed: processed %d, want %d", got, total)
	}
}

// TestCloseIsIdempotent verifies Close can be called more than once without
// panicking on "close of closed channel" and that repeat calls still return
// only after shutdown is complete.
func TestCloseIsIdempotent(t *testing.T) {
	var processed atomic.Int64

	b := New(8, 20*time.Millisecond, func(batch []*int) {
		processed.Add(int64(len(batch)))
	}, true, WithName("close_idempotent"))

	for i := 0; i < 32; i++ {
		v := i
		b.Put(&v)
	}

	b.Close()
	b.Close() // must not panic
	b.Close() // must not panic

	if got := processed.Load(); got != 32 {
		t.Fatalf("processed %d, want 32", got)
	}
}

// TestCloseDrainPooledBackground exercises the NewWithPool background path,
// which dispatches each batch on a fresh goroutine, to confirm those
// goroutines are also awaited by Close.
func TestCloseDrainPooledBackground(t *testing.T) {
	const total = 300

	var processed atomic.Int64

	b := NewWithPool(10, 50*time.Millisecond, func(batch []*int) {
		time.Sleep(2 * time.Millisecond)
		processed.Add(int64(len(batch)))
	}, true, WithName("close_drain_pool"))

	for i := 0; i < total; i++ {
		v := i
		b.Put(&v)
	}

	b.Close()

	if got := processed.Load(); got != total {
		t.Fatalf("pooled background drain incomplete: processed %d, want %d", got, total)
	}
}
