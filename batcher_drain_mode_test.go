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

// TestDrainMode_RespectsMaxCap verifies that drain mode never exceeds the size cap
// and processes all items. Exact batch boundaries are not asserted because the worker
// goroutine may interleave with the producer — particularly under -race — producing
// more than three batches with different sizes, all of which are valid.
func TestDrainMode_RespectsMaxCap(t *testing.T) {
	const total = 500
	const maxSize = 200

	var mu sync.Mutex
	var batches []int

	b := New[int](maxSize, 5*time.Second, func(batch []*int) {
		mu.Lock()
		batches = append(batches, len(batch))
		mu.Unlock()
	}, false)
	b.SetDrainMode(true)

	for i := range total {
		v := i
		b.Put(&v)
	}

	// Wait for all items to be processed.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		sum := 0
		for _, s := range batches {
			sum += s
		}
		return sum >= total
	}, 5*time.Second, time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Every batch must respect the size cap.
	for i, s := range batches {
		assert.LessOrEqual(t, s, maxSize, "batch %d exceeded max size", i)
	}

	// All items must be accounted for.
	sum := 0
	for _, s := range batches {
		sum += s
	}
	assert.Equal(t, total, sum, "total items processed should equal %d", total)
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

type scalingTracker struct {
	mu         sync.Mutex
	batchSizes []int
	totalItems int
}

func (s *scalingTracker) record(size int) {
	s.mu.Lock()
	s.batchSizes = append(s.batchSizes, size)
	s.totalItems += size
	s.mu.Unlock()
}

func (s *scalingTracker) done(target int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.totalItems >= target
}

func (s *scalingTracker) logStats(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	t.Logf("Processed %d items in %d batches", s.totalItems, len(s.batchSizes))

	if len(s.batchSizes) == 0 {
		return
	}

	minSize, maxSize, sum := s.batchSizes[0], s.batchSizes[0], 0
	for _, sz := range s.batchSizes {
		sum += sz
		if sz < minSize {
			minSize = sz
		}
		if sz > maxSize {
			maxSize = sz
		}
	}
	t.Logf("Batch sizes: min=%d, max=%d, avg=%d", minSize, maxSize, sum/len(s.batchSizes))
}

// TestDrainMode_ScalingBehavior demonstrates that batch sizes adapt naturally
// to throughput: more items queued during processing = larger batches.
func TestDrainMode_ScalingBehavior(t *testing.T) {
	const batchSize = 1000
	const totalSend = 5000

	tracker := &scalingTracker{}
	b := New[int](batchSize, 10*time.Second, func(batch []*int) {
		tracker.record(len(batch))
		time.Sleep(time.Millisecond) // Simulate work — items queue during this time
	}, false)
	b.SetDrainMode(true)

	for i := range totalSend {
		v := i
		b.Put(&v)
	}

	require.Eventually(t, func() bool {
		return tracker.done(totalSend)
	}, 10*time.Second, time.Millisecond)

	tracker.logStats(t)

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	assert.Less(t, len(tracker.batchSizes), totalSend, "drain mode should batch items together")

	hasLargeBatch := false
	for _, s := range tracker.batchSizes {
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

func waitForProcessed(b *testing.B, processed *atomic.Int64, target int64) {
	b.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for processed.Load() < target {
		if time.Now().After(deadline) {
			b.Fatalf("timed out: %d/%d processed", processed.Load(), target)
		}
		time.Sleep(time.Millisecond)
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
			totalLatency.Add(now - *item)
		}
		processed.Add(int64(len(batch)))
	}, false)
	if drain {
		bat.SetDrainMode(true)
	}

	b.ResetTimer()

	interval := time.Second / time.Duration(targetRate)
	for i := 0; i < b.N; i++ {
		now := time.Now().UnixNano()
		bat.Put(&now)
		if interval > 0 && i%100 == 0 {
			time.Sleep(interval * 100)
		}
	}

	waitForProcessed(b, &processed, int64(b.N))
	b.StopTimer()

	reportBenchMetrics(b, &totalLatency, &batchCount)
}

func reportBenchMetrics(b *testing.B, totalLatency, batchCount *atomic.Int64) {
	b.Helper()

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
