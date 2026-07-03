package batcher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// putBatchItem is a minimal test item carrying its own identity so ordering
// and membership can be verified after a batch fn runs.
type putBatchItem struct {
	id int
}

// TestPutBatch_EmptySliceIsNoOp verifies that calling PutBatch with an empty
// slice does not enqueue anything and does not trigger a flush.
func TestPutBatch_EmptySliceIsNoOp(t *testing.T) {
	var flushes atomic.Int64
	b := New[putBatchItem](10, time.Hour, func(_ []*putBatchItem) {
		flushes.Add(1)
	}, true)
	defer b.Close()

	b.PutBatch(nil)
	b.PutBatch([]*putBatchItem{})

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int64(0), flushes.Load())
}

// TestPutBatch_SmallerThanSizeAccumulates verifies that a PutBatch call with
// fewer items than the configured size does not trigger an immediate flush —
// it accumulates exactly like N individual Put calls would.
func TestPutBatch_SmallerThanSizeAccumulates(t *testing.T) {
	var mu sync.Mutex
	var batches [][]*putBatchItem
	b := New[putBatchItem](10, time.Hour, func(batch []*putBatchItem) {
		mu.Lock()
		defer mu.Unlock()
		batches = append(batches, batch)
	}, true)
	defer b.Close()

	items := []*putBatchItem{{id: 1}, {id: 2}, {id: 3}}
	b.PutBatch(items)

	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	require.Empty(t, batches, "batch of 3 into a size-10 batcher must not flush before timeout/trigger")
	mu.Unlock()

	b.Trigger()
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 3)
}

// TestPutBatch_ExactlySizeFlushesImmediately verifies that a PutBatch call
// whose length exactly equals the configured size triggers an immediate
// flush, just as accumulating that many items one at a time would.
func TestPutBatch_ExactlySizeFlushesImmediately(t *testing.T) {
	var flushed atomic.Int64
	done := make(chan struct{})
	b := New[putBatchItem](5, time.Hour, func(batch []*putBatchItem) {
		flushed.Store(int64(len(batch)))
		close(done)
	}, true)
	defer b.Close()

	items := make([]*putBatchItem, 5)
	for i := range items {
		items[i] = &putBatchItem{id: i}
	}
	b.PutBatch(items)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected immediate flush when PutBatch fills exactly to size")
	}
	require.Equal(t, int64(5), flushed.Load())
}

// TestPutBatch_LargerThanSizeSplitsIntoFullBatches verifies the core §6.1
// design decision: a PutBatch group larger than the configured size is split
// into multiple batches, each dispatched with exactly `size` items (never
// more), preserving item order across the split.
//
// Uses background=false (synchronous dispatch) so batch *completion* order
// is deterministic and reflects split order. With background=true and no
// SetMaxConcurrent, each flushed batch runs on its own unbounded goroutine —
// cross-batch completion order is never guaranteed there (only order within
// a single batch is), so asserting it would test scheduler behavior, not
// appendItems' splitting logic.
func TestPutBatch_LargerThanSizeSplitsIntoFullBatches(t *testing.T) { //nolint:gocyclo // Sequential select-based assertions on split/flush timing; splitting would obscure the ordering being tested
	const size = 4
	const totalItems = 10 // 2 full batches of 4 + a trailing partial batch of 2

	var mu sync.Mutex
	var batches [][]*putBatchItem
	flushed := make(chan struct{}, totalItems)

	b := New[putBatchItem](size, time.Hour, func(batch []*putBatchItem) {
		mu.Lock()
		// Copy since the batcher may reuse/reset slices after fn returns.
		cp := make([]*putBatchItem, len(batch))
		copy(cp, batch)
		batches = append(batches, cp)
		mu.Unlock()
		flushed <- struct{}{}
	}, false)
	defer b.Close()

	items := make([]*putBatchItem, totalItems)
	for i := range items {
		items[i] = &putBatchItem{id: i}
	}
	b.PutBatch(items)

	// Two full batches should flush immediately (split at cap).
	for i := 0; i < 2; i++ {
		select {
		case <-flushed:
		case <-time.After(time.Second):
			t.Fatalf("expected split flush #%d to fire immediately", i+1)
		}
	}

	// The trailing 2 items should NOT flush yet (under size, no timeout/trigger).
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
	require.Len(t, batches, 3)
	require.Len(t, batches[0], size)
	require.Len(t, batches[1], size)
	require.Len(t, batches[2], totalItems-2*size)

	// Verify strict ordering is preserved across the split.
	var gotIDs []int
	for _, batch := range batches {
		for _, item := range batch {
			gotIDs = append(gotIDs, item.id)
		}
	}
	require.Len(t, gotIDs, totalItems)
	for i, id := range gotIDs {
		require.Equal(t, i, id, "item order must be preserved across a PutBatch split")
	}
}

// TestPutBatch_MultipleSizesLargerThanBatcherSize verifies a group many
// multiples of size larger than the batch cap splits into the correct number
// of full batches with none dropped or duplicated.
func TestPutBatch_MultipleSizesLargerThanBatcherSize(t *testing.T) {
	const size = 3
	const totalItems = 30 // exactly 10 full batches, no remainder

	var seen sync.Map
	var flushCount atomic.Int64
	allDone := make(chan struct{})

	b := New[putBatchItem](size, time.Hour, func(batch []*putBatchItem) {
		require.Len(t, batch, size, "every split batch must be exactly the configured size when the group is an exact multiple")
		for _, item := range batch {
			_, alreadySeen := seen.LoadOrStore(item.id, true)
			require.False(t, alreadySeen, "item %d observed more than once", item.id)
		}
		if flushCount.Add(1) == totalItems/size {
			close(allDone)
		}
	}, true)
	defer b.Close()

	items := make([]*putBatchItem, totalItems)
	for i := range items {
		items[i] = &putBatchItem{id: i}
	}
	b.PutBatch(items)

	select {
	case <-allDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected %d full batches, got %d", totalItems/size, flushCount.Load())
	}

	for i := 0; i < totalItems; i++ {
		_, ok := seen.Load(i)
		require.True(t, ok, "item %d was never dispatched", i)
	}
}

// TestPutBatchCtx_CapturesSpanLinkOnce verifies PutBatchCtx behaves like
// PutCtx for span-context capture: the context passed in is recorded once
// per call, not once per item. This test only exercises that PutBatchCtx
// does not panic and delivers all items — span content itself is covered by
// the tracer-integration tests elsewhere in this package.
func TestPutBatchCtx_DeliversAllItems(t *testing.T) {
	var count atomic.Int64
	done := make(chan struct{})
	b := New[putBatchItem](3, time.Hour, func(batch []*putBatchItem) {
		if count.Add(int64(len(batch))) == 3 {
			close(done)
		}
	}, true)
	defer b.Close()

	items := []*putBatchItem{{id: 1}, {id: 2}, {id: 3}}
	b.PutBatchCtx(t.Context(), items)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected PutBatchCtx items to flush at size cap")
	}
}

// TestPutBatch_WithPool verifies PutBatch works correctly with the
// slice-pooling constructor, exercising the pool-return path after a split.
func TestPutBatch_WithPool(t *testing.T) {
	const size = 4
	var flushCount atomic.Int64
	done := make(chan struct{})

	b := NewWithPool[putBatchItem](size, time.Hour, func(_ []*putBatchItem) {
		if flushCount.Add(1) == 2 {
			close(done)
		}
	}, true)
	defer b.Close()

	items := make([]*putBatchItem, 8)
	for i := range items {
		items[i] = &putBatchItem{id: i}
	}
	b.PutBatch(items)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected 2 full-size flushes from an 8-item PutBatch on a size-4 pooled batcher")
	}
}

// TestPutBatch_DuringDrainMode verifies that drain mode correctly handles a
// PutBatch envelope arriving mid-drain, splitting at the size cap the same
// way the non-drain path does.
func TestPutBatch_DuringDrainMode(t *testing.T) {
	const size = 5
	var mu sync.Mutex
	var batches [][]int
	flushed := make(chan struct{}, 10)

	b := New[putBatchItem](size, time.Hour, func(batch []*putBatchItem) {
		mu.Lock()
		ids := make([]int, len(batch))
		for i, it := range batch {
			ids[i] = it.id
		}
		batches = append(batches, ids)
		mu.Unlock()
		flushed <- struct{}{}
	}, true)
	b.SetDrainMode(true)
	defer b.Close()

	items := make([]*putBatchItem, 12)
	for i := range items {
		items[i] = &putBatchItem{id: i}
	}
	b.PutBatch(items)

	// Drain mode should fire as soon as items are available: expect at least
	// 2 full flushes (10 of the 12 items) plus a drain-fire of the remaining 2.
	for i := 0; i < 3; i++ {
		select {
		case <-flushed:
		case <-time.After(time.Second):
			t.Fatalf("expected flush #%d under drain mode", i+1)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, batch := range batches {
		total += len(batch)
	}
	require.Equal(t, 12, total, "no items should be lost when a PutBatch group is split under drain mode")
}
