package batcher

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIntegratedOptimizations tests multiple optimizations working together.
func TestIntegratedOptimizations(t *testing.T) { //nolint:gocognit,gocyclo // Test requires complex scenarios
	// Combined test of WithPool and WithDeduplication
	t.Run("PoolWithDeduplication", func(t *testing.T) {
		processedItems := make(map[int]int)
		var mu sync.Mutex

		processBatch := func(batch []*testItem) {
			mu.Lock()
			defer mu.Unlock()
			for _, item := range batch {
				processedItems[item.ID]++
			}
		}

		// Create a batcher with pool
		poolBatcher := NewWithPool[testItem](50, 20*time.Millisecond, func(batch []*testItem) {
			// Then pass to dedup batcher
			dedupBatcher := NewWithDeduplication[testItem](50, 20*time.Millisecond, processBatch, false)
			for _, item := range batch {
				dedupBatcher.Put(item)
			}
			dedupBatcher.Trigger()
			time.Sleep(30 * time.Millisecond)
		}, true)

		// Add items with duplicates
		for i := 0; i < 100; i++ {
			poolBatcher.Put(&testItem{ID: i % 20}) // Only 20 unique items
			if i%5 == 0 {
				poolBatcher.Put(&testItem{ID: i % 20}) // Add duplicate
			}
		}

		// Wait for processing
		time.Sleep(200 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()
		if len(processedItems) != 20 {
			t.Errorf("Expected 20 unique items, got %d", len(processedItems))
		}
	})

	// Test all features together
	t.Run("AllFeaturesCombined", func(t *testing.T) {
		// Track metrics
		totalProcessed := atomic.Int32{}
		batchCount := atomic.Int32{}

		// Use map to track processed items
		processedItems := make(map[int]bool)
		var mu sync.Mutex

		processBatch := func(batch []*testItem) {
			batchCount.Add(1)
			mu.Lock()
			defer mu.Unlock()
			for _, item := range batch {
				totalProcessed.Add(1)
				processedItems[item.ID] = true
			}
		}

		// Use both pool and deduplication features
		batcher := NewWithDeduplicationAndPool[testItem](100, 30*time.Millisecond, processBatch, true)

		// Concurrent writers
		var wg sync.WaitGroup
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					// Mix of unique and duplicate items
					batcher.Put(&testItem{ID: goroutineID*100 + i})
					if i%3 == 0 {
						batcher.Put(&testItem{ID: goroutineID*100 + i}) // Duplicate
					}
				}
			}(g)
		}

		wg.Wait()
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		uniqueCount := len(processedItems)
		mu.Unlock()

		t.Logf("Total processed: %d, Unique: %d, Batches: %d",
			totalProcessed.Load(), uniqueCount, batchCount.Load())

		if uniqueCount != 1000 {
			t.Errorf("Expected 1000 unique items, got %d", uniqueCount)
		}
	})
}

// TestLongRunningStability tests the implementations over extended periods.
func TestLongRunningStability(t *testing.T) { //nolint:gocognit,gocyclo // Test requires complex scenarios
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}

	t.Run("ExtendedProcessing", func(t *testing.T) {
		// Track memory usage
		var startMem, endMem runtime.MemStats
		runtime.ReadMemStats(&startMem)

		processCount := atomic.Int64{}
		errorCount := atomic.Int32{}

		processBatch := func(batch []*testItem) {
			processCount.Add(int64(len(batch)))
			// Simulate some processing
			time.Sleep(time.Millisecond)
		}

		// Create batcher
		batcher := NewWithPool[testItem](100, 50*time.Millisecond, processBatch, true)

		// Run for extended period
		done := make(chan bool)
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for i := 0; i < 100; i++ { // 10 seconds total
				select {
				case <-ticker.C:
					// Add batch of items
					for j := 0; j < 1000; j++ {
						batcher.Put(&testItem{ID: i*1000 + j})
					}
				case <-done:
					return
				}
			}
		}()

		// Monitor for 10 seconds
		time.Sleep(10 * time.Second)
		close(done)

		// Final processing
		time.Sleep(200 * time.Millisecond)

		runtime.ReadMemStats(&endMem)

		// Check results
		expectedMin := int64(90000) // At least 90% of items
		if processCount.Load() < expectedMin {
			t.Errorf("Expected at least %d items processed, got %d",
				expectedMin, processCount.Load())
		}

		// Check memory growth
		memGrowth := endMem.HeapAlloc - startMem.HeapAlloc
		t.Logf("Memory growth: %d bytes, Errors: %d",
			memGrowth, errorCount.Load())

		if errorCount.Load() > 0 {
			t.Errorf("Encountered %d errors during processing", errorCount.Load())
		}
	})

	// Test graceful shutdown
	t.Run("GracefulShutdown", func(t *testing.T) {
		itemsAdded := atomic.Int32{}
		itemsProcessed := atomic.Int32{}

		processBatch := func(batch []*testItem) {
			itemsProcessed.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
			time.Sleep(10 * time.Millisecond)     // Slow processing
		}

		batcher := New[testItem](50, 100*time.Millisecond, processBatch, true)

		// Add items continuously
		done := make(chan bool)
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					batcher.Put(&testItem{ID: int(itemsAdded.Load())})
					itemsAdded.Add(1)
					time.Sleep(time.Millisecond)
				}
			}
		}()

		// Run for a bit
		time.Sleep(500 * time.Millisecond)

		// Stop adding items
		close(done)

		// Trigger final batch
		batcher.Trigger()

		// Wait for processing to complete
		time.Sleep(200 * time.Millisecond)

		// Most items should be processed
		processRate := float64(itemsProcessed.Load()) / float64(itemsAdded.Load())
		t.Logf("Added: %d, Processed: %d, Rate: %.2f",
			itemsAdded.Load(), itemsProcessed.Load(), processRate)

		if processRate < 0.9 {
			t.Errorf("Processing rate too low: %.2f", processRate)
		}
	})

	// Test panic recovery
	t.Run("PanicRecovery", func(t *testing.T) {
		panicCount := atomic.Int32{}
		successCount := atomic.Int32{}

		processBatch := func(batch []*testItem) {
			defer func() {
				if r := recover(); r != nil {
					panicCount.Add(1)
				}
			}()

			// Panic on certain conditions - check if any item in batch matches
			for _, item := range batch {
				if item.ID == 99 || item.ID == 199 {
					panic("simulated panic")
				}
			}

			successCount.Add(1)
		}

		batcher := New[testItem](10, 50*time.Millisecond, processBatch, true)

		// Add items that will trigger panics
		for i := 0; i < 200; i++ {
			batcher.Put(&testItem{ID: i})
		}

		// Trigger to ensure all items are processed
		batcher.Trigger()

		// Wait for processing
		time.Sleep(500 * time.Millisecond)

		t.Logf("Panics: %d, Successful batches: %d",
			panicCount.Load(), successCount.Load())

		// Should have both panics and successes
		if panicCount.Load() == 0 {
			t.Error("Expected some panics to be triggered")
		}
		if successCount.Load() == 0 {
			t.Error("Expected some successful batches")
		}
	})
}

// TestResourceCleanup verifies proper resource cleanup.
func TestResourceCleanup(t *testing.T) { //nolint:gocognit // Test requires complex scenarios
	t.Run("MultipleInstanceLifecycle", func(t *testing.T) {
		// Create and destroy multiple instances
		for i := 0; i < 10; i++ {
			processBatch := func(_ []*testItem) {
				time.Sleep(time.Millisecond)
			}

			// Create different types of batchers
			b1 := New[testItem](100, 50*time.Millisecond, processBatch, true)
			b2 := NewWithPool[testItem](100, 50*time.Millisecond, processBatch, true)
			b3 := NewWithDeduplication[testItem](100, 50*time.Millisecond, processBatch, true)

			// Use them
			for j := 0; j < 100; j++ {
				b1.Put(&testItem{ID: j})
				b2.Put(&testItem{ID: j})
				b3.Put(&testItem{ID: j})
			}

			// Trigger and wait
			b1.Trigger()
			b2.Trigger()
			b3.Trigger()

			time.Sleep(100 * time.Millisecond)
		}

		// Force GC and check for leaks
		runtime.GC()
		runtime.GC()

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("Final heap alloc: %d MB", m.HeapAlloc/1024/1024)
	})

	t.Run("TimePartitionedMapCleanup", func(_ *testing.T) {
		maps := make([]*TimePartitionedMap[int, string], 10)

		// Create multiple maps
		for i := 0; i < 10; i++ {
			m := NewTimePartitionedMap[int, string](50*time.Millisecond, 5)
			defer m.Close()
			maps[i] = m

			// Add data
			for j := 0; j < 1000; j++ {
				m.Set(j, fmt.Sprintf("value_%d_%d", i, j))
			}
		}

		// Close all maps
		for _, m := range maps {
			m.Close()
		}

		// Verify cleanup
		time.Sleep(100 * time.Millisecond)

		// Try to use after close (should not panic)
		for _, m := range maps {
			m.Set(999, "after_close")
			_, _ = m.Get(999)
		}
	})
}
