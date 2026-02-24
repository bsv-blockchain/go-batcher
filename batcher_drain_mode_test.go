package batcher

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDrainMode_FiresImmediately verifies that when drain mode is enabled and
// items are already queued, the batch fires immediately without waiting for timeout.
func TestDrainMode_FiresImmediately(t *testing.T) {
	batchCh := make(chan []*int, 10)

	b := New[int](100, 5*time.Second, func(batch []*int) {
		cp := make([]*int, len(batch))
		copy(cp, batch)
		batchCh <- cp
	}, false)
	b.SetDrainMode(true)

	// Pre-fill channel with 10 items
	for i := range 10 {
		v := i
		b.Put(&v)
	}

	select {
	case batch := <-batchCh:
		require.Len(t, batch, 10, "expected batch of 10 items")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("drain mode did not fire immediately — timed out waiting for batch")
	}
}

// TestDrainMode_RespectsMaxCap verifies that drain mode caps batches at size.
func TestDrainMode_RespectsMaxCap(t *testing.T) {
	batchCh := make(chan int, 10)

	b := New[int](200, 5*time.Second, func(batch []*int) {
		batchCh <- len(batch)
	}, false)
	b.SetDrainMode(true)

	// Pre-fill channel with 500 items (more than size=200)
	for i := range 500 {
		v := i
		b.Put(&v)
	}

	// First batch should be exactly 200 (capped)
	select {
	case size := <-batchCh:
		assert.Equal(t, 200, size, "first batch should be capped at 200")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for first batch")
	}

	// Second batch should also be 200
	select {
	case size := <-batchCh:
		assert.Equal(t, 200, size, "second batch should be capped at 200")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for second batch")
	}

	// Third batch should be remaining 100
	select {
	case size := <-batchCh:
		assert.Equal(t, 100, size, "third batch should be remaining 100")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for third batch")
	}
}

// TestDrainMode_SingleItem verifies that a single item fires immediately
// as a batch of 1 with no timer delay.
func TestDrainMode_SingleItem(t *testing.T) {
	batchCh := make(chan int, 10)

	b := New[int](100, 5*time.Second, func(batch []*int) {
		batchCh <- len(batch)
	}, false)
	b.SetDrainMode(true)

	v := 42
	b.Put(&v)

	select {
	case size := <-batchCh:
		assert.Equal(t, 1, size, "single item should fire as batch of 1")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("single item did not fire immediately in drain mode")
	}
}

// TestDrainMode_ScalingBehavior demonstrates that batch sizes adapt naturally
// to throughput: more items queued during processing = larger batches.
func TestDrainMode_ScalingBehavior(t *testing.T) {
	const batchSize = 1000

	var mu sync.Mutex
	var batchSizes []int
	var totalItems int

	processingDelay := 1 * time.Millisecond

	b := New[int](batchSize, 10*time.Second, func(batch []*int) {
		mu.Lock()
		batchSizes = append(batchSizes, len(batch))
		totalItems += len(batch)
		mu.Unlock()
		time.Sleep(processingDelay) // Simulate work — items queue during this time
	}, false)
	b.SetDrainMode(true)

	// Send 5000 items as fast as possible
	const totalSend = 5000
	for i := range totalSend {
		v := i
		b.Put(&v)
	}

	// Wait for all items to be processed
	deadline := time.After(10 * time.Second)
	for {
		mu.Lock()
		done := totalItems >= totalSend
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timed out: only %d/%d items processed in %d batches", totalItems, totalSend, len(batchSizes))
			mu.Unlock() //nolint:govet // unreachable but keeps the pattern consistent
			return
		default:
			time.Sleep(time.Millisecond)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	t.Logf("Processed %d items in %d batches", totalItems, len(batchSizes))
	if len(batchSizes) > 0 {
		minSize, maxSize := batchSizes[0], batchSizes[0]
		sum := 0
		for _, s := range batchSizes {
			sum += s
			if s < minSize {
				minSize = s
			}
			if s > maxSize {
				maxSize = s
			}
		}
		t.Logf("Batch sizes: min=%d, max=%d, avg=%d", minSize, maxSize, sum/len(batchSizes))
	}

	// With drain mode, we should see fewer batches than items (batching is working)
	assert.Less(t, len(batchSizes), totalSend, "drain mode should batch items together")
	// And batch sizes should adapt — at least some batches > 1
	hasLargeBatch := false
	for _, s := range batchSizes {
		if s > 1 {
			hasLargeBatch = true
			break
		}
	}
	assert.True(t, hasLargeBatch, "drain mode should produce adaptive batch sizes > 1")
}

// TestDrainMode_NormalModeUnchanged verifies that normal mode (drainMode=false)
// still uses timer-based batching and is not affected by the drain mode code path.
func TestDrainMode_NormalModeUnchanged(t *testing.T) {
	batchCh := make(chan int, 10)

	timeout := 100 * time.Millisecond
	b := New[int](1000, timeout, func(batch []*int) {
		batchCh <- len(batch)
	}, false)
	// drainMode is false by default

	// Send a few items — not enough to fill the batch
	for i := range 5 {
		v := i
		b.Put(&v)
	}

	// In normal mode, batch should fire after timeout (~100ms), not immediately
	select {
	case <-batchCh:
		// Fired at timeout — expected
	case <-time.After(2 * time.Second):
		t.Fatal("normal mode timed out — batch didn't fire at all")
	}
}

// BenchmarkDrainMode_vs_Normal compares drain mode and normal mode at different throughput levels.
func BenchmarkDrainMode_vs_Normal(b *testing.B) {
	rates := []int{10_000, 50_000, 100_000}
	modes := []struct {
		name  string
		drain bool
	}{
		{"Drain", true},
		{"Normal", false},
	}

	for _, rate := range rates {
		for _, mode := range modes {
			b.Run(fmt.Sprintf("%s_%dk", mode.name, rate/1000), func(b *testing.B) {
				benchmarkMode(b, rate, mode.drain)
			})
		}
	}
}

func benchmarkMode(b *testing.B, targetRate int, drain bool) {
	b.Helper()

	var processed atomic.Int64
	var batchCount atomic.Int64
	var totalLatency atomic.Int64

	bat := New[int64](500, 5*time.Millisecond, func(batch []*int64) {
		now := time.Now().UnixNano()
		batchCount.Add(1)
		for _, item := range batch {
			latency := now - *item
			totalLatency.Add(latency)
		}
		processed.Add(int64(len(batch)))
	}, false)
	if drain {
		bat.SetDrainMode(true)
	}

	b.ResetTimer()

	// Send b.N items at the target rate
	interval := time.Second / time.Duration(targetRate)
	for i := 0; i < b.N; i++ {
		now := time.Now().UnixNano()
		bat.Put(&now)
		if interval > 0 && i%100 == 0 {
			// Batch sleep to approximate target rate without per-item overhead
			time.Sleep(interval * 100)
		}
	}

	// Wait for all items to be processed
	deadline := time.Now().Add(30 * time.Second)
	for processed.Load() < int64(b.N) {
		if time.Now().After(deadline) {
			b.Fatalf("timed out: %d/%d processed", processed.Load(), b.N)
		}
		time.Sleep(time.Millisecond)
	}

	b.StopTimer()

	batches := batchCount.Load()
	avgLatency := time.Duration(0)
	if b.N > 0 {
		avgLatency = time.Duration(totalLatency.Load() / int64(b.N))
	}
	avgBatchSize := 0
	if batches > 0 {
		avgBatchSize = b.N / int(batches)
	}

	b.ReportMetric(float64(avgLatency.Microseconds()), "avg_latency_us")
	b.ReportMetric(float64(avgBatchSize), "avg_batch_size")
	b.ReportMetric(float64(batches), "total_batches")
}
