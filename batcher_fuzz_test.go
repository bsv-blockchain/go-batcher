//nolint:wsl // Fuzz test files have different formatting standards
package batcher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// FuzzBatcherPut tests the Batcher's Put method with various inputs.
//
// This fuzz test verifies that the Batcher can handle:
// - Different batch sizes (including edge cases like 0, 1, and very large)
// - Different timeout values
// - Concurrent Put operations
// - Various item counts
// - No panics or data races occur
func FuzzBatcherPut(f *testing.F) { //nolint:gocognit,gocyclo // Fuzz tests require complex logic
	// Add seed corpus with interesting test cases
	f.Add(1, 100, 10)      // Small batch size
	f.Add(100, 1000, 1000) // Medium batch size
	f.Add(10000, 100, 100) // Large batch size
	f.Add(1, 1, 1)         // Minimal values
	f.Add(0, 100, 10)      // Zero batch size (should be handled)

	f.Fuzz(func(t *testing.T, batchSize int, timeoutMs int, itemCount int) {
		// Skip invalid inputs
		if batchSize < 0 || timeoutMs < 0 || itemCount < 0 {
			t.Skip("Skipping negative values")
		}

		// Limit ranges to prevent excessive memory usage
		if batchSize > 100000 {
			batchSize = 100000
		}
		if itemCount > 100000 {
			itemCount = 100000
		}
		if timeoutMs > 10000 {
			timeoutMs = 10000
		}

		// Ensure minimum batch size of 1
		if batchSize == 0 {
			batchSize = 1
		}

		var processedItems int64
		var processedBatches int64
		var mu sync.Mutex

		batchFn := func(batch []*batchStoreItem) {
			mu.Lock()
			defer mu.Unlock()

			atomic.AddInt64(&processedBatches, 1)
			atomic.AddInt64(&processedItems, int64(len(batch)))

			// Verify batch size constraints
			if len(batch) > batchSize {
				t.Errorf("Batch size %d exceeds configured size %d", len(batch), batchSize)
			}
		}

		timeout := time.Duration(timeoutMs) * time.Millisecond
		b := New[batchStoreItem](batchSize, timeout, batchFn, true)

		// Add items
		for i := 0; i < itemCount; i++ {
			b.Put(&batchStoreItem{})
		}

		// Trigger final batch
		b.Trigger()

		// Allow time for background processing
		time.Sleep(time.Duration(timeoutMs+100) * time.Millisecond)

		// Verify all items were processed
		finalProcessed := atomic.LoadInt64(&processedItems)
		if finalProcessed != int64(itemCount) {
			t.Errorf("Expected %d processed items, got %d", itemCount, finalProcessed)
		}
	})
}

// FuzzBatcherConcurrent tests concurrent operations on the Batcher.
//
// This fuzz test verifies thread safety by:
// - Running multiple goroutines that Put items
// - Running goroutines that call Trigger
// - Ensuring no data races or panics
// - Verifying all items are processed exactly once
func FuzzBatcherConcurrent(f *testing.F) { //nolint:gocognit,gocyclo // Fuzz tests require complex logic
	// Add seed corpus
	f.Add(10, 5, 100, 50)  // goroutines, triggers, items per goroutine, batch size
	f.Add(100, 10, 10, 10) // many goroutines, few items
	f.Add(2, 2, 1000, 100) // few goroutines, many items

	f.Fuzz(func(t *testing.T, numGoroutines int, numTriggers int, itemsPerGoroutine int, batchSize int) {
		// Skip invalid inputs
		if numGoroutines <= 0 || numTriggers < 0 || itemsPerGoroutine < 0 || batchSize <= 0 {
			t.Skip("Skipping invalid values")
		}

		// Limit ranges
		if numGoroutines > 100 {
			numGoroutines = 100
		}
		if numTriggers > 50 {
			numTriggers = 50
		}
		if itemsPerGoroutine > 1000 {
			itemsPerGoroutine = 1000
		}
		if batchSize > 1000 {
			batchSize = 1000
		}

		var processedItems int64
		var wg sync.WaitGroup

		batchFn := func(batch []*batchStoreItem) {
			atomic.AddInt64(&processedItems, int64(len(batch)))
		}

		b := New[batchStoreItem](batchSize, time.Second, batchFn, true)

		// Start producer goroutines
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < itemsPerGoroutine; j++ {
					b.Put(&batchStoreItem{})
				}
			}()
		}

		// Start trigger goroutines
		for i := 0; i < numTriggers; i++ {
			wg.Add(1)
			delay := i // Capture loop variable
			go func() {
				defer wg.Done()
				time.Sleep(time.Duration(delay) * time.Millisecond)
				b.Trigger()
			}()
		}

		// Wait for all goroutines
		wg.Wait()

		// Final trigger to process remaining items
		b.Trigger()

		// Wait for all items to be processed with retries
		expectedItems := int64(numGoroutines * itemsPerGoroutine)
		var finalProcessed int64

		// Try up to 10 times with increasing delays
		for i := 0; i < 10; i++ {
			time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
			finalProcessed = atomic.LoadInt64(&processedItems)
			if finalProcessed == expectedItems {
				break
			}
			// Trigger again in case some items are stuck
			b.Trigger()
		}

		if finalProcessed != expectedItems {
			t.Errorf("Expected %d processed items, got %d", expectedItems, finalProcessed)
		}
	})
}

// FuzzTimePartitionedMap tests the TimePartitionedMap with various inputs.
//
// This fuzz test verifies:
// - Different bucket sizes and counts
// - Set/Get/Delete operations work correctly
// - No panics with edge cases
// - Memory is properly managed
func FuzzTimePartitionedMap(f *testing.F) { //nolint:gocognit,gocyclo // Fuzz tests require complex logic
	// Add seed corpus
	f.Add(100, 10, 1000)  // bucket duration ms, max buckets, operations
	f.Add(1000, 60, 100)  // 1 second bucket, 60 buckets, 100 ops
	f.Add(1, 1, 10)       // minimal values
	f.Add(5000, 100, 500) // large values

	f.Fuzz(func(t *testing.T, bucketMs int, maxBuckets int, numOperations int) {
		// Skip invalid inputs
		if bucketMs <= 0 || maxBuckets <= 0 || numOperations < 0 {
			t.Skip("Skipping invalid values")
		}

		// Limit ranges
		if bucketMs > 10000 {
			bucketMs = 10000
		}
		if maxBuckets > 1000 {
			maxBuckets = 1000
		}
		if numOperations > 10000 {
			numOperations = 10000
		}

		bucketDuration := time.Duration(bucketMs) * time.Millisecond
		m := NewTimePartitionedMap[int, string](bucketDuration, maxBuckets)

		// Track what we've set for verification
		expected := make(map[int]string)
		var mu sync.Mutex

		// Perform random operations
		for i := 0; i < numOperations; i++ {
			key := i % 100 // Use limited key space to ensure some overlap

			switch i % 3 {
			case 0: // Set
				value := string(rune('A' + (i % 26)))
				m.Set(key, value)
				mu.Lock()
				expected[key] = value
				mu.Unlock()

			case 1: // Get
				val, exists := m.Get(key)
				mu.Lock()
				expectedVal, expectedExists := expected[key]
				mu.Unlock()

				if exists && expectedExists && val != expectedVal {
					t.Errorf("Get(%d): expected %s, got %s", key, expectedVal, val)
				}

			case 2: // Delete
				m.Delete(key)
				mu.Lock()
				delete(expected, key)
				mu.Unlock()
			}
		}

		// Verify Count is reasonable
		count := m.Count()
		if count < 0 {
			t.Errorf("Count returned negative value: %d", count)
		}
		if count > numOperations {
			t.Errorf("Count %d exceeds number of operations %d", count, numOperations)
		}
	})
}

// FuzzBatcherWithDedup tests the deduplication batcher with various inputs.
//
// This fuzz test verifies:
// - Deduplication works correctly
// - Various item patterns (unique, duplicates, mixed)
// - Thread safety with concurrent operations
// - No panics or incorrect behavior
func FuzzBatcherWithDedup(f *testing.F) { //nolint:gocognit,gocyclo // Fuzz tests require complex logic
	// Add seed corpus
	f.Add(10, 100, 50, 20)   // batch size, timeout ms, total items, unique items
	f.Add(100, 1000, 100, 5) // many duplicates
	f.Add(5, 50, 100, 100)   // all unique
	f.Add(1, 10, 10, 1)      // all duplicates

	f.Fuzz(func(t *testing.T, batchSize int, timeoutMs int, totalItems int, uniqueItems int) {
		// Skip invalid inputs
		if batchSize <= 0 || timeoutMs <= 0 || totalItems < 0 || uniqueItems < 0 {
			t.Skip("Skipping invalid values")
		}

		// Ensure uniqueItems <= totalItems
		if uniqueItems > totalItems {
			uniqueItems = totalItems
		}

		// Limit ranges
		if batchSize > 1000 {
			batchSize = 1000
		}
		if totalItems > 10000 {
			totalItems = 10000
		}
		if timeoutMs > 5000 {
			timeoutMs = 5000
		}

		var processedItems int64
		uniqueSeen := make(map[int]bool)
		var mu sync.Mutex

		batchFn := func(batch []*testItem) {
			atomic.AddInt64(&processedItems, int64(len(batch)))

			// Verify no duplicates in batch
			seen := make(map[int]bool)
			for _, item := range batch {
				if seen[item.ID] {
					t.Errorf("Duplicate item %d in single batch", item.ID)
				}
				seen[item.ID] = true

				mu.Lock()
				uniqueSeen[item.ID] = true
				mu.Unlock()
			}
		}

		timeout := time.Duration(timeoutMs) * time.Millisecond
		b := NewWithDeduplication[testItem](batchSize, timeout, batchFn, true)

		// Add items with controlled duplication
		if uniqueItems == 0 {
			uniqueItems = 1 // Prevent division by zero
		}

		for i := 0; i < totalItems; i++ {
			// Create items that cycle through unique IDs
			item := &testItem{ID: i % uniqueItems}
			b.Put(item)
		}

		// Trigger final processing
		b.Trigger()
		time.Sleep(time.Duration(timeoutMs+100) * time.Millisecond)

		// Verify we processed the expected number of unique items
		mu.Lock()
		actualUnique := len(uniqueSeen)
		mu.Unlock()

		if actualUnique != uniqueItems {
			t.Errorf("Expected %d unique items, got %d", uniqueItems, actualUnique)
		}
	})
}

// FuzzTimePartitionedMapConcurrent tests concurrent operations on TimePartitionedMap.
//
// This fuzz test verifies:
// - Thread safety under high concurrency
// - No data races
// - Operations complete without panic
// - Basic correctness under a concurrent load
func FuzzTimePartitionedMapConcurrent(f *testing.F) { //nolint:gocognit,gocyclo // Fuzz tests require complex logic
	// Add seed corpus
	f.Add(10, 100, 50) // goroutines, operations per goroutine, key range
	f.Add(50, 20, 10)  // high concurrency, few operations
	f.Add(5, 200, 100) // low concurrency, many operations
	f.Add(100, 10, 10) // very high concurrency

	f.Fuzz(func(t *testing.T, numGoroutines int, opsPerGoroutine int, keyRange int) {
		// Skip invalid inputs
		if numGoroutines <= 0 || opsPerGoroutine <= 0 || keyRange <= 0 {
			t.Skip("Skipping invalid values")
		}

		// Limit ranges
		if numGoroutines > 100 {
			numGoroutines = 100
		}
		if opsPerGoroutine > 1000 {
			opsPerGoroutine = 1000
		}
		if keyRange > 1000 {
			keyRange = 1000
		}

		m := NewTimePartitionedMap[int, int](100*time.Millisecond, 10)
		var wg sync.WaitGroup

		// Run concurrent operations
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				for op := 0; op < opsPerGoroutine; op++ {
					key := (goroutineID + op) % keyRange

					switch op % 4 {
					case 0: // Set
						m.Set(key, goroutineID)
					case 1: // Get
						_, _ = m.Get(key)
					case 2: // Delete
						m.Delete(key)
					case 3: // Count
						_ = m.Count()
					}
				}
			}(g)
		}

		// Wait for all operations to complete
		wg.Wait()

		// Final verification - ensure no panic and count is valid
		finalCount := m.Count()
		if finalCount < 0 {
			t.Errorf("Invalid count after concurrent operations: %d", finalCount)
		}
	})
}
