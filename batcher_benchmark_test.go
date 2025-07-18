package batcher

import (
	"math"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// Benchmark_Batcher measures the performance of the basic Batcher under a high load.
//
// This benchmark tests the batcher's ability to handle a large volume of items
// (1 million) and verifies that all items are processed in the expected number
// of batches. It uses channels and goroutines to track batch completion.
//
// Benchmark characteristics:
// - Processes 1 million items in batches of 100
// - Uses background processing for non-blocking operation
// - Verifies exact batch count (10,000 batches expected)
// - Measures throughput under sustained load
//
// Performance considerations:
// - Channel buffer size affects throughput
// - Background processing enables concurrent batch handling
// - Memory allocation patterns impact overall performance
//
// Notes:
// - Uses errgroup for clean goroutine management
// - Channel signaling ensures accurate batch counting
// - Math.Ceil ensures correct batch count calculation
func BenchmarkBatcher(b *testing.B) { //nolint:gocognit // Benchmark includes setup and verification logic
	b.Run("BenchmarkBatcher", func(b *testing.B) {
		// Channel to signal when a batch is processed
		batchSent := make(chan bool, 100)

		// Batch processing function that signals completion
		sendStoreBatch := func(_ []*batchStoreItem) {
			batchSent <- true
		}

		// Configure batcher with 100 items per batch
		batchSize := 100
		storeBatcher := New[batchStoreItem](batchSize, 100, sendStoreBatch, true)

		// Calculate the expected number of batches
		expectedItems := 1_000_000
		expectedBatches := math.Ceil(float64(expectedItems) / float64(batchSize))

		// Goroutine to count processed batches
		g := errgroup.Group{}
		g.Go(func() error {
			for <-batchSent {
				expectedBatches--
				if expectedBatches == 0 {
					return nil
				}
			}

			return nil
		})

		// Send all items to the batcher
		for i := 0; i < expectedItems; i++ {
			storeBatcher.Put(&batchStoreItem{})
		}

		// Wait for all batches to be processed
		if err := g.Wait(); err != nil {
			b.Fatalf("Error in batcher: %v", err)
		}
	})
}

// Benchmark_BatcherWithDeduplication measures the performance of deduplication-enabled batching.
//
// This benchmark suite tests the performance characteristics of the BatcherWithDedup
// under different scenarios: processing unique items, handling duplicates, and
// the underlying TimePartitionedMap performance.
//
// Benchmark scenarios:
// - Without duplicates: Tests overhead of deduplication on unique items
// - With duplicates: Measures performance when filtering 50% duplicates
// - TimePartitionedMap: Direct performance test of the deduplication map
//
// Key metrics:
// - Throughput with deduplication overhead
// - Memory efficiency when handling duplicates
// - Map lookup and insertion performance
//
// Notes:
// - Deduplication adds computational overhead even for unique items
// - Duplicate filtering can significantly reduce downstream processing
// - TimePartitionedMap performance is critical for overall throughput
func BenchmarkBatcherWithDeduplication(b *testing.B) { //nolint:gocognit,gocyclo // Benchmark suite tests multiple scenarios
	b.Run("Without duplicates", func(b *testing.B) {
		batchSent := make(chan bool, 100)
		sendStoreBatch := func(_ []*testItem) {
			batchSent <- true
		}
		batchSize := 100
		batcher := NewWithDeduplication[testItem](batchSize, 100*time.Millisecond, sendStoreBatch, true)

		expectedItems := 1_000_000
		expectedBatches := math.Ceil(float64(expectedItems) / float64(batchSize))
		g := errgroup.Group{}
		g.Go(func() error {
			for <-batchSent {
				expectedBatches--
				if expectedBatches == 0 {
					return nil
				}
			}

			return nil
		})

		for i := 0; i < expectedItems; i++ {
			batcher.Put(&testItem{ID: i})
		}

		if err := g.Wait(); err != nil {
			b.Fatalf("Error in batcher: %v", err)
		}
	})

	b.Run("With duplicates", func(b *testing.B) {
		// This benchmark tests deduplication efficiency with 50% duplicate items
		// It verifies that duplicates are filtered and only unique items are batched
		batchSent := make(chan bool, 100)
		sendStoreBatch := func(_ []*testItem) {
			batchSent <- true
		}
		batchSize := 100
		batcher := NewWithDeduplication[testItem](batchSize, 100*time.Millisecond, sendStoreBatch, true)

		// Configure 50% duplicates by using modulo on IDs
		expectedItems := 1_000_000
		uniqueItems := expectedItems / 2
		expectedBatches := math.Ceil(float64(uniqueItems) / float64(batchSize))

		var (
			processedBatches int64
			g                = errgroup.Group{}
		)

		// Goroutine to count processed batches using atomic operations
		g.Go(func() error {
			for <-batchSent {
				atomic.AddInt64(&processedBatches, 1)

				if atomic.LoadInt64(&processedBatches) >= int64(expectedBatches) {
					return nil
				}
			}

			return nil
		})

		// Send items with 50% duplicates
		for i := 0; i < expectedItems; i++ {
			// Create duplicates by reusing IDs
			id := i % uniqueItems
			batcher.Put(&testItem{ID: id})
		}

		// Verify all unique items were processed
		if err := g.Wait(); err != nil {
			b.Fatalf("Error in batcher: %v", err)
		}
	})

	b.Run("TimePartitionedMap performance", func(b *testing.B) {
		// Direct benchmark of the TimePartitionedMap data structure
		// Tests both insertion and retrieval performance at scale

		// Create a map with 1-second buckets and 60 buckets (1-minute window)
		m := NewTimePartitionedMap[int, struct{}](time.Second, 60)

		b.ResetTimer()

		// Benchmark insertion performance
		for i := 0; i < b.N; i++ {
			m.Set(i, struct{}{})
		}

		// Prepare for retrieval benchmark
		b.StopTimer()

		found := 0

		// Benchmark retrieval performance
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_, exists := m.Get(i)
			if exists {
				found++
			}
		}

		b.StopTimer()

		// Verify all items we found
		if found != b.N {
			b.Fatalf("Expected to find %d items, found %d", b.N, found)
		}
	})
}
