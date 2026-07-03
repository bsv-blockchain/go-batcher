package batcher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGreedyAccumulate_NeverProducesSmallerBatchesThanNonGreedy verifies the
// core §6.2 design guarantee: greedy accumulate changes only how quickly
// items are pulled off the channel between size/timeout/trigger checks — it
// never changes flush triggers or produces a batch smaller than the
// non-greedy path would for the same arrival pattern. Here that means: when
// enough items are queued up-front to fill several batches, greedy
// accumulate still fills every batch to exactly the configured size (it does
// not fire early on a momentarily-empty channel the way drain mode does).
func TestGreedyAccumulate_NeverProducesSmallerBatchesThanNonGreedy(t *testing.T) {
	const size = 10
	const totalItems = 35 // 3 full batches + a trailing partial of 5

	var mu sync.Mutex
	var batchSizes []int
	flushed := make(chan struct{}, totalItems)

	b := New[int](size, time.Hour, func(batch []*int) {
		mu.Lock()
		batchSizes = append(batchSizes, len(batch))
		mu.Unlock()
		flushed <- struct{}{}
	}, false, WithGreedyAccumulate(true))
	defer b.Close()

	for i := 0; i < totalItems; i++ {
		v := i
		b.Put(&v)
	}

	for i := 0; i < 3; i++ {
		select {
		case <-flushed:
		case <-time.After(time.Second):
			t.Fatalf("expected full-size flush #%d under greedy accumulate", i+1)
		}
	}

	// Trailing 5 items should not flush yet (under size, no timeout/trigger).
	select {
	case <-flushed:
		t.Fatal("trailing partial batch flushed before size/timeout/trigger")
	case <-time.After(20 * time.Millisecond):
	}

	b.Trigger()
	select {
	case <-flushed:
	case <-time.After(time.Second):
		t.Fatal("expected trailing partial batch to flush on Trigger")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int{size, size, size, totalItems - 3*size}, batchSizes)
}

// TestGreedyAccumulate_TimerStillFires verifies that greedy accumulate does
// not interfere with the timeout-based flush for a batch that never reaches
// the size cap.
func TestGreedyAccumulate_TimerStillFires(t *testing.T) {
	done := make(chan []*int, 1)
	b := New[int](100, 20*time.Millisecond, func(batch []*int) {
		cp := make([]*int, len(batch))
		copy(cp, batch)
		done <- cp
	}, false, WithGreedyAccumulate(true))
	defer b.Close()

	for i := 0; i < 3; i++ {
		v := i
		b.Put(&v)
	}

	select {
	case batch := <-done:
		require.Len(t, batch, 3)
	case <-time.After(time.Second):
		t.Fatal("expected timeout-triggered flush under greedy accumulate")
	}
}

// TestGreedyAccumulate_RejectedWhenDrainModeEnabled verifies the two modes
// are mutually exclusive: enabling greedy accumulate is a no-op (with a
// logged warning) when drain mode is already on.
func TestGreedyAccumulate_RejectedWhenDrainModeEnabled(t *testing.T) {
	var warnings atomic.Int64
	logger := &countingLogger{warnCount: &warnings}

	b := New[int](10, time.Hour, func(_ []*int) {}, false, WithLogger(logger))
	defer b.Close()

	b.SetDrainMode(true)
	b.SetGreedyAccumulate(true)

	require.Equal(t, int64(1), warnings.Load(), "expected a warning when enabling greedy accumulate with drain mode on")
}

// TestGreedyAccumulate_DrainModeRejectedWhenGreedyEnabled verifies the
// reverse: enabling drain mode after greedy accumulate is already on is
// rejected with a warning, mirroring SetTickInterval/SetDrainMode's existing
// mutual-exclusion behavior.
func TestGreedyAccumulate_DrainModeRejectedWhenGreedyEnabled(t *testing.T) {
	var warnings atomic.Int64
	logger := &countingLogger{warnCount: &warnings}

	b := New[int](10, time.Hour, func(_ []*int) {}, false, WithLogger(logger))
	defer b.Close()

	b.SetGreedyAccumulate(true)
	b.SetDrainMode(true)

	require.Equal(t, int64(1), warnings.Load(), "expected a warning when enabling drain mode with greedy accumulate on")
}

// TestGreedyAccumulate_WithPutBatch verifies greedy accumulate composes with
// PutBatch: items delivered via a single multi-item envelope are eligible
// for the same greedy non-blocking drain as individually-Put items.
func TestGreedyAccumulate_WithPutBatch(t *testing.T) {
	const size = 5
	var flushCount atomic.Int64
	done := make(chan struct{})

	b := New[int](size, time.Hour, func(_ []*int) {
		if flushCount.Add(1) == 2 {
			close(done)
		}
	}, true, WithGreedyAccumulate(true))
	defer b.Close()

	items := make([]*int, 10)
	for i := range items {
		v := i
		items[i] = &v
	}
	b.PutBatch(items)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected 2 full-size flushes from a 10-item PutBatch under greedy accumulate")
	}
}

// TestGreedyAccumulate_AllItemsDelivered is a broader soak-style check that
// no items are lost or duplicated when many Put calls race against greedy
// accumulate's inner drain loop.
func TestGreedyAccumulate_AllItemsDelivered(t *testing.T) {
	const totalItems = 5000
	const size = 64

	var seen sync.Map
	var count atomic.Int64
	allDone := make(chan struct{})

	b := New[int](size, 50*time.Millisecond, func(batch []*int) {
		for _, v := range batch {
			_, dup := seen.LoadOrStore(*v, true)
			require.False(t, dup, "item %d observed more than once", *v)
		}
		if count.Add(int64(len(batch))) == totalItems {
			close(allDone)
		}
	}, true, WithGreedyAccumulate(true))
	defer b.Close()

	for i := 0; i < totalItems; i++ {
		v := i
		b.Put(&v)
	}

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("expected all %d items delivered, got %d", totalItems, count.Load())
	}
}

// countingLogger is a minimal Logger that counts Warnf calls, used to assert
// on the mutual-exclusion warning path without depending on log output format.
type countingLogger struct {
	warnCount *atomic.Int64
}

func (countingLogger) Debugf(string, ...any) {}
func (countingLogger) Infof(string, ...any)  {}
func (l countingLogger) Warnf(string, ...any) {
	l.warnCount.Add(1)
}
func (countingLogger) Errorf(string, ...any) {}
