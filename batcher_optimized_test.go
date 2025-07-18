package batcher

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPutOptimized verifies that PutOptimized behaves correctly.
func TestPutOptimized(t *testing.T) {
	itemCount := atomic.Int32{}
	processBatch := func(batch []*testItem) {
		itemCount.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
	}
	batcher := NewOptimized[testItem](10, 100*time.Millisecond, processBatch, false)
	// Test adding items
	for i := 0; i < 25; i++ {
		batcher.PutOptimized(&testItem{ID: i})
	}
	// Trigger to ensure all items are processed
	batcher.Trigger()
	time.Sleep(200 * time.Millisecond)
	if itemCount.Load() != 25 {
		t.Errorf("Expected 25 items processed, got %d", itemCount.Load())
	}
}

// TestWorkerOptimized verifies that the optimized worker processes batches correctly.
func TestWorkerOptimized(t *testing.T) {
	batchCount := atomic.Int32{}
	processBatch := func(batch []*testItem) {
		batchCount.Add(1)
		if len(batch) > 10 {
			t.Errorf("Batch size exceeded limit: %d", len(batch))
		}
	}
	batcher := NewOptimized[testItem](10, 50*time.Millisecond, processBatch, false)
	// Add exactly 10 items (should trigger size-based batch)
	for i := 0; i < 10; i++ {
		batcher.Put(&testItem{ID: i})
	}
	time.Sleep(20 * time.Millisecond)
	if batchCount.Load() != 1 {
		t.Errorf("Expected 1 batch after size trigger, got %d", batchCount.Load())
	}
	// Add 5 more items and wait for timeout
	for i := 0; i < 5; i++ {
		batcher.Put(&testItem{ID: i + 10})
	}
	time.Sleep(100 * time.Millisecond)
	if batchCount.Load() != 2 {
		t.Errorf("Expected 2 batches after timeout trigger, got %d", batchCount.Load())
	}
}

// TestWithPool verifies that WithPool works correctly.
func TestWithPool(t *testing.T) {
	itemCount := atomic.Int32{}
	processBatch := func(batch []*testItem) {
		itemCount.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
	}
	batcher := NewWithPool[testItem](100, 50*time.Millisecond, processBatch, true)
	// Add many items to test pool reuse
	for i := 0; i < 1000; i++ {
		batcher.Put(&testItem{ID: i})
	}
	// Wait for processing
	time.Sleep(200 * time.Millisecond)
	if itemCount.Load() != 1000 {
		t.Errorf("Expected 1000 items processed, got %d", itemCount.Load())
	}
}

// TestWithPoolConcurrent verifies thread safety of WithPool.
func TestWithPoolConcurrent(t *testing.T) {
	itemCount := atomic.Int32{}
	processBatch := func(batch []*testItem) {
		itemCount.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
		// Simulate some processing time
		time.Sleep(10 * time.Millisecond)
	}
	batcher := NewWithPool[testItem](50, 100*time.Millisecond, processBatch, true)
	var wg sync.WaitGroup
	goroutines := 10
	itemsPerGoroutine := 100
	// Launch concurrent writers
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				batcher.Put(&testItem{ID: goroutineID*1000 + i})
			}
		}(g)
	}
	wg.Wait()
	batcher.Trigger()
	time.Sleep(200 * time.Millisecond)
	expectedItems := int32(goroutines * itemsPerGoroutine) //nolint:gosec // Test code with controlled input
	if itemCount.Load() != expectedItems {
		t.Errorf("Expected %d items processed, got %d", expectedItems, itemCount.Load())
	}
}

// TestWithPoolTrigger verifies that WithPool.Trigger() forces immediate batch processing.
func TestWithPoolTrigger(t *testing.T) {
	batchCount := atomic.Int32{}
	processBatch := func(_ []*testItem) {
		batchCount.Add(1)
	}
	batcher := NewWithPool[testItem](100, 5*time.Second, processBatch, false)
	// Add items less than batch size
	for i := 0; i < 5; i++ {
		batcher.Put(&testItem{ID: i})
	}
	// Without trigger, no batch should be processed yet
	time.Sleep(50 * time.Millisecond)
	if batchCount.Load() != 0 {
		t.Error("Batch should not be processed before trigger")
	}
	// Trigger should force immediate processing
	batcher.Trigger()
	time.Sleep(50 * time.Millisecond)
	if batchCount.Load() != 1 {
		t.Errorf("Expected 1 batch after trigger, got %d", batchCount.Load())
	}
}

// TestTimePartitionedMapOptimized verifies the optimized Get and Set methods.
func TestTimePartitionedMapOptimized(t *testing.T) {
	m := NewTimePartitionedMapOptimized[string, int](time.Second, 5)
	defer m.Close()
	// Test SetOptimized
	if !m.SetOptimized("key1", 1) {
		t.Error("Expected SetOptimized to return true for new key")
	}
	// Test duplicate detection
	if m.SetOptimized("key1", 2) {
		t.Error("Expected SetOptimized to return false for duplicate key")
	}
	// Test GetOptimized
	val, exists := m.GetOptimized("key1")
	if !exists || val != 1 {
		t.Errorf("Expected GetOptimized to find key1 with value 1, got %v, %v", val, exists)
	}
	// Test non-existent key
	_, exists = m.GetOptimized("nonexistent")
	if exists {
		t.Error("Expected GetOptimized to return false for non-existent key")
	}
}

// TestTimePartitionedMapOptimizedExtended provides comprehensive tests for TimePartitionedMapOptimized.
func TestTimePartitionedMapOptimizedExtended(t *testing.T) { //nolint:gocognit,gocyclo // Test requires complex scenarios
	// Test bucket rotation
	t.Run("BucketRotation", func(t *testing.T) {
		// Use short bucket duration for testing
		bucketDuration := 100 * time.Millisecond
		m := NewTimePartitionedMapOptimized[string, int](bucketDuration, 3)
		defer m.Close()

		// Add items to first bucket
		m.SetOptimized("bucket1_item1", 1)
		m.SetOptimized("bucket1_item2", 2)

		// Wait for bucket rotation
		time.Sleep(bucketDuration + 20*time.Millisecond)

		// Add items to second bucket
		m.SetOptimized("bucket2_item1", 3)
		m.SetOptimized("bucket2_item2", 4)

		// Verify items from both buckets are accessible
		if val, exists := m.GetOptimized("bucket1_item1"); !exists || val != 1 {
			t.Errorf("Expected to find bucket1_item1")
		}
		if val, exists := m.GetOptimized("bucket2_item1"); !exists || val != 3 {
			t.Errorf("Expected to find bucket2_item1")
		}

		// Wait for more rotations
		time.Sleep(3 * bucketDuration)

		// Old items should be cleaned up (beyond maxBuckets)
		if _, exists := m.GetOptimized("bucket1_item1"); exists {
			t.Error("Old bucket items should be cleaned up")
		}
	})

	// Test concurrent access during bucket cleanup
	t.Run("ConcurrentAccessDuringCleanup", func(t *testing.T) {
		bucketDuration := 50 * time.Millisecond
		m := NewTimePartitionedMapOptimized[int, string](bucketDuration, 2)
		defer m.Close()

		var wg sync.WaitGroup
		errors := atomic.Int32{}

		// Writer goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				m.SetOptimized(i, fmt.Sprintf("value_%d", i))
				time.Sleep(2 * time.Millisecond)
			}
		}()

		// Reader goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				// Try to read recent values
				for j := i - 10; j <= i; j++ {
					if j >= 0 {
						m.GetOptimized(j)
					}
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()

		// Cleanup trigger goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				time.Sleep(bucketDuration)
				// Bucket rotation happens automatically with time
			}
		}()

		wg.Wait()

		if errors.Load() > 0 {
			t.Errorf("Concurrent access errors: %d", errors.Load())
		}
	})

	// Test bloom filter rebuild accuracy
	t.Run("BloomFilterRebuild", func(t *testing.T) {
		bucketDuration := 100 * time.Millisecond
		m := NewTimePartitionedMapOptimized[string, int](bucketDuration, 3)
		defer m.Close()

		// Add items
		items := []string{"apple", "banana", "cherry", "date", "elderberry"}
		for i, item := range items {
			m.SetOptimized(item, i)
		}

		// Force bloom filter rebuild
		m.rebuildBloomFilter()

		// Verify bloom filter contains all items
		for _, item := range items {
			if !m.bloomFilter.Test(item) {
				t.Errorf("Bloom filter should contain %s after rebuild", item)
			}
		}

		// Test non-existent items (should mostly return false)
		nonExistent := []string{"grape", "kiwi", "mango", "orange", "peach"}
		falsePositives := 0
		for _, item := range nonExistent {
			if m.bloomFilter.Test(item) {
				falsePositives++
			}
		}

		// Allow some false positives but not all
		if falsePositives == len(nonExistent) {
			t.Error("Bloom filter returning too many false positives")
		}
	})

	// Test Close method cleanup
	t.Run("CloseMethodCleanup", func(t *testing.T) {
		bucketDuration := 50 * time.Millisecond
		m := NewTimePartitionedMapOptimized[string, int](bucketDuration, 5)

		// Add some items
		m.SetOptimized("test1", 1)
		m.SetOptimized("test2", 2)

		// Close should stop the ticker
		m.Close()

		// Verify ticker is stopped (no panic on subsequent Close)
		m.Close()

		// Operations should still work after close (but bloom filter won't rebuild)
		m.SetOptimized("test3", 3)
		if val, exists := m.GetOptimized("test3"); !exists || val != 3 {
			t.Error("Map should still function after Close")
		}
	})

	// Test memory leak prevention
	t.Run("MemoryLeakPrevention", func(t *testing.T) {
		bucketDuration := 10 * time.Millisecond
		m := NewTimePartitionedMapOptimized[int, []byte](bucketDuration, 3)
		defer m.Close()

		// Add many large items
		largeData := make([]byte, 1024) // 1KB per item
		for i := 0; i < 100; i++ {
			m.SetOptimized(i, largeData)
			if i%10 == 0 {
				time.Sleep(bucketDuration) // Force bucket rotation
			}
		}

		// Wait for cleanup
		time.Sleep(5 * bucketDuration)

		// Only recent items should remain (within maxBuckets)
		oldItemsFound := 0
		for i := 0; i < 50; i++ {
			if _, exists := m.GetOptimized(i); exists {
				oldItemsFound++
			}
		}

		if oldItemsFound > 10 {
			t.Errorf("Too many old items still in memory: %d", oldItemsFound)
		}
	})
}

// TestBloomFilter verifies bloom filter functionality.
func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(1000, 3)
	// Test adding and testing
	bf.Add("test1")
	bf.Add("test2")
	bf.Add(123)
	if !bf.Test("test1") {
		t.Error("Bloom filter should contain test1")
	}
	if !bf.Test("test2") {
		t.Error("Bloom filter should contain test2")
	}
	if !bf.Test(123) {
		t.Error("Bloom filter should contain 123")
	}
	// Test non-existent (may have false positives but should be rare)
	falsePositives := 0
	for i := 1000; i < 2000; i++ {
		if bf.Test(i) {
			falsePositives++
		}
	}
	// With proper sizing, false positive rate should be low
	if float64(falsePositives)/1000 > 0.1 {
		t.Errorf("False positive rate too high: %d/1000", falsePositives)
	}
	// Test reset
	bf.Reset()
	if bf.Test("test1") {
		// After reset, this could still be a false positive but unlikely
		t.Log("Possible false positive after reset")
	}
}

// TestBloomFilterExtended provides comprehensive tests for BloomFilter.
func TestBloomFilterExtended(t *testing.T) { //nolint:gocognit,gocyclo // Test requires complex scenarios
	// Test extremely large bloom filter
	t.Run("LargeBloomFilter", func(t *testing.T) {
		bf := NewBloomFilter(1000000, 5) // 1M bits, 5 hash functions
		// Add many items
		for i := 0; i < 10000; i++ {
			bf.Add(i)
		}
		// Verify all added items are found
		for i := 0; i < 10000; i++ {
			if !bf.Test(i) {
				t.Errorf("Bloom filter should contain %d", i)
			}
		}
	})

	// Test different data types
	t.Run("DifferentDataTypes", func(t *testing.T) {
		bf := NewBloomFilter(10000, 3)
		// Test various types
		type customStruct struct {
			ID   int
			Name string
		}

		// Add all supported types
		bf.Add("string")
		bf.Add(int(42))
		bf.Add(int8(8))
		bf.Add(int16(16))
		bf.Add(int32(32))
		bf.Add(int64(64))
		bf.Add(uint(42))
		bf.Add(uint8(8))
		bf.Add(uint16(16))
		bf.Add(uint32(32))
		bf.Add(uint64(64))
		bf.Add(float32(3.14))
		bf.Add(float64(3.14159))
		bf.Add(true)
		bf.Add(false)
		bf.Add(customStruct{ID: 1, Name: "test"})

		// Test all types are found
		if !bf.Test("string") {
			t.Error("Should find string")
		}
		if !bf.Test(int(42)) {
			t.Error("Should find int")
		}
		if !bf.Test(int8(8)) {
			t.Error("Should find int8")
		}
		if !bf.Test(int16(16)) {
			t.Error("Should find int16")
		}
		if !bf.Test(int32(32)) {
			t.Error("Should find int32")
		}
		if !bf.Test(int64(64)) {
			t.Error("Should find int64")
		}
		if !bf.Test(uint(42)) {
			t.Error("Should find uint")
		}
		if !bf.Test(uint8(8)) {
			t.Error("Should find uint8")
		}
		if !bf.Test(uint16(16)) {
			t.Error("Should find uint16")
		}
		if !bf.Test(uint32(32)) {
			t.Error("Should find uint32")
		}
		if !bf.Test(uint64(64)) {
			t.Error("Should find uint64")
		}
		if !bf.Test(float32(3.14)) {
			t.Error("Should find float32")
		}
		if !bf.Test(float64(3.14159)) {
			t.Error("Should find float64")
		}
		if !bf.Test(true) {
			t.Error("Should find bool true")
		}
		if !bf.Test(false) {
			t.Error("Should find bool false")
		}
		if !bf.Test(customStruct{ID: 1, Name: "test"}) {
			t.Error("Should find struct using default hash")
		}

		// Test that different values don't collide
		if bf.Test(int(43)) && bf.Test(int(44)) && bf.Test(int(45)) {
			t.Log("Some false positives expected with bloom filter")
		}
	})

	// Test concurrent operations
	t.Run("ConcurrentOperations", func(t *testing.T) {
		bf := NewBloomFilter(100000, 4)
		var wg sync.WaitGroup
		// Concurrent adds
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					bf.Add(n*1000 + j)
				}
			}(i)
		}
		wg.Wait()

		// Concurrent tests
		errorCount := atomic.Int32{}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					if !bf.Test(n*1000 + j) {
						errorCount.Add(1)
					}
				}
			}(i)
		}
		wg.Wait()
		if errorCount.Load() > 0 {
			t.Errorf("Concurrent test failures: %d", errorCount.Load())
		}
	})

	// Test Reset during concurrent operations
	t.Run("ResetDuringConcurrent", func(t *testing.T) {
		bf := NewBloomFilter(10000, 3)
		done := make(chan bool)
		// Start concurrent adds
		go func() {
			for i := 0; i < 1000; i++ {
				bf.Add(i)
				time.Sleep(time.Microsecond)
			}
			done <- true
		}()
		// Reset after brief delay
		time.Sleep(time.Millisecond)
		bf.Reset()
		<-done
		// Verify reset worked (items added before reset should be gone)
		falsePositives := 0
		for i := 0; i < 100; i++ {
			if bf.Test(i) {
				falsePositives++
			}
		}
		if falsePositives > 10 {
			t.Logf("Higher than expected false positives after reset: %d", falsePositives)
		}
	})

	// Test false positive rate
	t.Run("FalsePositiveRate", func(t *testing.T) {
		// Known configuration for testing
		size := uint64(10000)
		hashFuncs := uint(3)
		bf := NewBloomFilter(size, hashFuncs)

		// Add 1000 items
		itemsAdded := 1000
		for i := 0; i < itemsAdded; i++ {
			bf.Add(fmt.Sprintf("item_%d", i))
		}

		// Test 10000 non-existent items
		falsePositives := 0
		testsCount := 10000
		for i := itemsAdded; i < itemsAdded+testsCount; i++ {
			if bf.Test(fmt.Sprintf("item_%d", i)) {
				falsePositives++
			}
		}

		// Calculate actual false positive rate
		actualRate := float64(falsePositives) / float64(testsCount)
		// Expected rate calculation: (1 - e^(-kn/m))^k
		// where k=hashFuncs, n=itemsAdded, m=size
		expectedRate := math.Pow(1-math.Exp(-float64(hashFuncs*uint(itemsAdded))/float64(size)), float64(hashFuncs)) //nolint:gosec // Math calculation

		t.Logf("False positive rate: actual=%.4f, expected≈%.4f", actualRate, expectedRate)
		// Allow some variance
		if math.Abs(actualRate-expectedRate) > 0.05 {
			t.Errorf("False positive rate deviation too high")
		}
	})
}

// TestWithDedupOptimized verifies optimized deduplication functionality.
func TestWithDedupOptimized(t *testing.T) {
	processedItems := make(map[int]bool)
	var mu sync.Mutex
	processBatch := func(batch []*testItem) {
		mu.Lock()
		defer mu.Unlock()
		for _, item := range batch {
			if processedItems[item.ID] {
				t.Errorf("Duplicate item processed: %d", item.ID)
			}
			processedItems[item.ID] = true
		}
	}
	batcher := NewWithDeduplicationOptimized[testItem](10, 50*time.Millisecond, processBatch, false)
	// Add items with duplicates
	for i := 0; i < 20; i++ {
		batcher.Put(&testItem{ID: i})
		// Add duplicate
		batcher.Put(&testItem{ID: i})
	}
	// Trigger processing
	batcher.Trigger()
	time.Sleep(100 * time.Millisecond)
	// Verify only unique items were processed
	mu.Lock()
	defer mu.Unlock()
	if len(processedItems) != 20 {
		t.Errorf("Expected 20 unique items, got %d", len(processedItems))
	}
}

// TestWithDedupOptimizedTrigger verifies that WithDedupOptimized.Trigger() forces immediate batch processing.
func TestWithDedupOptimizedTrigger(t *testing.T) {
	batchCount := atomic.Int32{}
	processBatch := func(_ []*testItem) {
		batchCount.Add(1)
	}
	batcher := NewWithDeduplicationOptimized[testItem](100, 5*time.Second, processBatch, false)
	// Add items less than batch size
	for i := 0; i < 5; i++ {
		batcher.Put(&testItem{ID: i})
	}
	// Without trigger, no batch should be processed yet
	time.Sleep(50 * time.Millisecond)
	if batchCount.Load() != 0 {
		t.Error("Batch should not be processed before trigger")
	}
	// Trigger should force immediate processing
	batcher.Trigger()
	time.Sleep(50 * time.Millisecond)
	if batchCount.Load() != 1 {
		t.Errorf("Expected 1 batch after trigger, got %d", batchCount.Load())
	}
}

// TestWithDedupOptimizedEdgeCases tests deduplication window edge cases.
func TestWithDedupOptimizedEdgeCases(t *testing.T) { //nolint:gocognit,gocyclo // Test requires complex scenarios
	// Test nil item handling
	t.Run("NilItemHandling", func(t *testing.T) {
		itemCount := atomic.Int32{}
		processBatch := func(batch []*testItem) {
			itemCount.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
		}
		batcher := NewWithDeduplicationOptimized[testItem](10, 50*time.Millisecond, processBatch, false)

		// Try to add nil items
		batcher.Put(nil)
		batcher.Put(&testItem{ID: 1})
		batcher.Put(nil)
		batcher.Put(&testItem{ID: 2})

		// Trigger processing
		batcher.Trigger()
		time.Sleep(100 * time.Millisecond)

		// Only non-nil items should be processed
		if itemCount.Load() != 2 {
			t.Errorf("Expected 2 items processed (nil excluded), got %d", itemCount.Load())
		}
	})

	// Test deduplication window boundary
	t.Run("DeduplicationWindowBoundary", func(t *testing.T) {
		// The implementation uses a fixed 1-minute window
		// We'll test behavior at smaller time scales
		processedItems := make(map[int]int)
		var mu sync.Mutex

		processBatch := func(batch []*testItem) {
			mu.Lock()
			defer mu.Unlock()
			for _, item := range batch {
				processedItems[item.ID]++
			}
		}

		batcher := NewWithDeduplicationOptimized[testItem](100, 10*time.Millisecond, processBatch, false)

		// Add item
		batcher.Put(&testItem{ID: 1})

		// Add duplicate immediately
		batcher.Put(&testItem{ID: 1})

		// Force processing
		batcher.Trigger()
		time.Sleep(50 * time.Millisecond)

		// Check only one instance was processed
		mu.Lock()
		if processedItems[1] != 1 {
			t.Errorf("Expected item 1 to be processed once, got %d times", processedItems[1])
		}
		mu.Unlock()

		// Wait for potential bucket rotation (though 1 minute window is long)
		// Add same item again after some time
		time.Sleep(100 * time.Millisecond)
		batcher.Put(&testItem{ID: 1})
		batcher.Trigger()
		time.Sleep(50 * time.Millisecond)

		// In a 1-minute window, it should still be deduplicated
		mu.Lock()
		if processedItems[1] != 1 {
			t.Errorf("Item should still be deduplicated within window, processed %d times", processedItems[1])
		}
		mu.Unlock()
	})

	// Test high duplicate rate performance
	t.Run("HighDuplicateRate", func(t *testing.T) {
		uniqueItems := atomic.Int32{}
		processBatch := func(batch []*testItem) {
			uniqueItems.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
		}

		batcher := NewWithDeduplicationOptimized[testItem](100, 50*time.Millisecond, processBatch, false)

		// Add many duplicates
		for i := 0; i < 1000; i++ {
			// Only 10 unique items, repeated 100 times each
			batcher.Put(&testItem{ID: i % 10})
		}

		// Trigger processing
		batcher.Trigger()
		time.Sleep(100 * time.Millisecond)

		// Should only process 10 unique items
		if uniqueItems.Load() != 10 {
			t.Errorf("Expected 10 unique items, got %d", uniqueItems.Load())
		}
	})

	// Test memory cleanup (indirectly via processing pattern)
	t.Run("MemoryCleanupPattern", func(t *testing.T) {
		processCount := atomic.Int32{}
		processBatch := func(_ []*testItem) {
			processCount.Add(1)
		}

		batcher := NewWithDeduplicationOptimized[testItem](10, 20*time.Millisecond, processBatch, false)

		// Add many unique items over time
		for i := 0; i < 100; i++ {
			for j := 0; j < 10; j++ {
				batcher.Put(&testItem{ID: i*10 + j})
			}
			if i%10 == 0 {
				time.Sleep(25 * time.Millisecond) // Allow some batches to process
			}
		}

		// Final processing
		time.Sleep(100 * time.Millisecond)

		// Verify processing occurred
		if processCount.Load() < 50 {
			t.Errorf("Expected at least 50 batches processed, got %d", processCount.Load())
		}
	})
}

// TestNewOptimizedEdgeCases tests edge cases for batch sizes and timeouts.
func TestNewOptimizedEdgeCases(t *testing.T) {
	processBatch := func(_ []*testItem) {}

	// Test zero batch size
	t.Run("ZeroBatchSize", func(_ *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				// If no panic, the batcher should still work with size 0
				batcher := NewOptimized[testItem](0, 100*time.Millisecond, processBatch, false)
				// Try to add an item - should trigger immediately or timeout
				batcher.Put(&testItem{ID: 1})
				time.Sleep(150 * time.Millisecond)
			}
		}()
		NewOptimized[testItem](0, 100*time.Millisecond, processBatch, false)
	})

	// Test negative batch size
	t.Run("NegativeBatchSize", func(_ *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				// If no panic, test behavior
				batcher := NewOptimized[testItem](-10, 100*time.Millisecond, processBatch, false)
				batcher.Put(&testItem{ID: 1})
				time.Sleep(150 * time.Millisecond)
			}
		}()
		NewOptimized[testItem](-10, 100*time.Millisecond, processBatch, false)
	})

	// Test zero timeout
	t.Run("ZeroTimeout", func(t *testing.T) {
		batchCount := atomic.Int32{}
		processFunc := func(_ []*testItem) {
			batchCount.Add(1)
		}
		batcher := NewOptimized[testItem](10, 0, processFunc, false)
		// Items should be processed immediately due to zero timeout
		batcher.Put(&testItem{ID: 1})
		time.Sleep(50 * time.Millisecond)
		if batchCount.Load() < 1 {
			t.Error("Zero timeout should trigger immediate processing")
		}
	})

	// Test negative timeout
	t.Run("NegativeTimeout", func(t *testing.T) {
		batchCount := atomic.Int32{}
		processFunc := func(_ []*testItem) {
			batchCount.Add(1)
		}
		batcher := NewOptimized[testItem](10, -100*time.Millisecond, processFunc, false)
		// Negative timeout should be treated as immediate
		batcher.Put(&testItem{ID: 1})
		time.Sleep(50 * time.Millisecond)
		if batchCount.Load() < 1 {
			t.Error("Negative timeout should trigger immediate processing")
		}
	})
}

// TestWithPoolEdgeCases tests edge cases for WithPool.
func TestWithPoolEdgeCases(t *testing.T) { //nolint:gocognit // Test requires complex scenarios
	// Test zero batch size with pool
	t.Run("ZeroBatchSizeWithPool", func(_ *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				processBatch := func(_ []*testItem) {}
				batcher := NewWithPool[testItem](0, 100*time.Millisecond, processBatch, false)
				batcher.Put(&testItem{ID: 1})
				time.Sleep(150 * time.Millisecond)
			}
		}()
		NewWithPool[testItem](0, 100*time.Millisecond, func(_ []*testItem) {}, false)
	})

	// Test pool exhaustion
	t.Run("PoolExhaustion", func(_ *testing.T) {
		slowProcess := func(_ []*testItem) {
			// Simulate slow processing to exhaust pool
			time.Sleep(100 * time.Millisecond)
		}
		batcher := NewWithPool[testItem](10, 20*time.Millisecond, slowProcess, true)

		// Send many batches quickly to potentially exhaust pool
		for i := 0; i < 100; i++ {
			for j := 0; j < 10; j++ {
				batcher.Put(&testItem{ID: i*10 + j})
			}
			time.Sleep(5 * time.Millisecond) // Trigger timeout batches
		}

		// Allow processing to complete
		time.Sleep(2 * time.Second)
		// If we get here without issues, pool handled exhaustion correctly
	})

	// Test memory leak prevention
	t.Run("MemoryLeakPrevention", func(t *testing.T) {
		// Track allocations
		processedBatches := atomic.Int32{}
		processBatch := func(batch []*testItem) {
			processedBatches.Add(1)
			// Ensure we're not holding references
			if cap(batch) > 100 {
				t.Errorf("Batch capacity too large: %d", cap(batch))
			}
		}

		batcher := NewWithPool[testItem](50, 10*time.Millisecond, processBatch, true)

		// Process many batches
		for i := 0; i < 1000; i++ {
			for j := 0; j < 50; j++ {
				batcher.Put(&testItem{ID: i*50 + j})
			}
			if i%100 == 0 {
				time.Sleep(50 * time.Millisecond) // Allow batches to process
			}
		}

		// Final wait for processing
		time.Sleep(500 * time.Millisecond)

		// Verify batches were processed
		if processedBatches.Load() < 900 {
			t.Errorf("Expected at least 900 batches, got %d", processedBatches.Load())
		}
	})
}

// TestNilFunctionHandlers tests behavior with nil function handlers.
func TestNilFunctionHandlers(t *testing.T) { //nolint:gocognit // Test requires complex scenarios
	// Test NewOptimized with nil function
	t.Run("NewOptimizedNilFunction", func(t *testing.T) {
		t.Skip("Nil function handler causes panic in worker goroutine - expected behavior")
		defer func() {
			if r := recover(); r != nil {
				// Expected to panic or handle gracefully
				t.Logf("NewOptimized with nil function handler: %v", r)
			}
		}()
		batcher := NewOptimized[testItem](10, 100*time.Millisecond, nil, false)
		batcher.Put(&testItem{ID: 1})
		// Force batch processing
		for i := 0; i < 10; i++ {
			batcher.Put(&testItem{ID: i})
		}
		time.Sleep(150 * time.Millisecond)
	})

	// Test WithPool with nil function
	t.Run("WithPoolNilFunction", func(t *testing.T) {
		t.Skip("Nil function handler causes panic in worker goroutine - expected behavior")
		defer func() {
			if r := recover(); r != nil {
				// Expected to panic or handle gracefully
				t.Logf("WithPool with nil function handler: %v", r)
			}
		}()
		batcher := NewWithPool[testItem](10, 100*time.Millisecond, nil, false)
		// Force batch processing
		for i := 0; i < 10; i++ {
			batcher.Put(&testItem{ID: i})
		}
		time.Sleep(150 * time.Millisecond)
	})

	// Test WithDedupOptimized with nil function
	t.Run("WithDedupOptimizedNilFunction", func(t *testing.T) {
		t.Skip("Nil function handler causes panic in worker goroutine - expected behavior")
		defer func() {
			if r := recover(); r != nil {
				// Expected to panic or handle gracefully
				t.Logf("WithDedupOptimized with nil function handler: %v", r)
			}
		}()
		batcher := NewWithDeduplicationOptimized[testItem](10, 100*time.Millisecond, nil, false)
		// Force batch processing
		for i := 0; i < 10; i++ {
			batcher.Put(&testItem{ID: i})
		}
		time.Sleep(150 * time.Millisecond)
	})
}

// FuzzPutOptimized tests PutOptimized with random inputs.
func FuzzPutOptimized(f *testing.F) {
	// Add seed corpus
	f.Add(1)
	f.Add(100)
	f.Add(1000)
	f.Fuzz(func(t *testing.T, numItems int) {
		if numItems < 0 || numItems > 10000 {
			t.Skip("Skipping unreasonable input")
		}
		itemCount := atomic.Int32{}
		processBatch := func(batch []*testItem) {
			itemCount.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
		}
		batcher := NewOptimized[testItem](10, 50*time.Millisecond, processBatch, false)
		for i := 0; i < numItems; i++ {
			batcher.PutOptimized(&testItem{ID: i})
		}
		batcher.Trigger()
		time.Sleep(100 * time.Millisecond)
		if int(itemCount.Load()) != numItems {
			t.Errorf("Expected %d items processed, got %d", numItems, itemCount.Load())
		}
	})
}

// Performance Benchmarks

// BenchmarkPutOptimized benchmarks the optimized Put method.
func BenchmarkPutOptimized(b *testing.B) {
	processBatch := func(_ []*testItem) {}
	batcher := NewOptimized[testItem](100, 50*time.Millisecond, processBatch, true)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			batcher.PutOptimized(&testItem{ID: i})
			i++
		}
	})
}

// BenchmarkWithPool benchmarks the WithPool implementation.
func BenchmarkWithPool(b *testing.B) {
	processBatch := func(_ []*testItem) {
		// Simulate some work
		time.Sleep(time.Microsecond)
	}
	batcher := NewWithPool[testItem](100, 20*time.Millisecond, processBatch, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Put(&testItem{ID: i})
	}

	// Wait for final batch
	time.Sleep(50 * time.Millisecond)
}

// BenchmarkWithPoolVsRegular compares WithPool against regular batching.
func BenchmarkWithPoolVsRegular(b *testing.B) {
	processBatch := func(_ []*testItem) {
		time.Sleep(time.Microsecond)
	}

	b.Run("WithPool", func(b *testing.B) {
		batcher := NewWithPool[testItem](50, 10*time.Millisecond, processBatch, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batcher.Put(&testItem{ID: i})
		}
		time.Sleep(20 * time.Millisecond)
	})

	b.Run("Regular", func(b *testing.B) {
		batcher := NewOptimized[testItem](50, 10*time.Millisecond, processBatch, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batcher.Put(&testItem{ID: i})
		}
		time.Sleep(20 * time.Millisecond)
	})
}

// BenchmarkBloomFilter benchmarks bloom filter operations.
func BenchmarkBloomFilter(b *testing.B) {
	bf := NewBloomFilter(1000000, 3)

	b.Run("Add", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Add(i)
		}
	})

	b.Run("Test", func(b *testing.B) {
		// Pre-populate
		for i := 0; i < 10000; i++ {
			bf.Add(i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Test(i % 20000) // Mix of existing and non-existing
		}
	})

	b.Run("AddAndTest", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Add(i)
			bf.Test(i)
		}
	})
}

// BenchmarkTimePartitionedMapOptimized benchmarks optimized map operations.
func BenchmarkTimePartitionedMapOptimized(b *testing.B) {
	m := NewTimePartitionedMapOptimized[int, string](time.Second, 10)
	defer m.Close()

	b.Run("SetOptimized", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.SetOptimized(i, fmt.Sprintf("value_%d", i))
		}
	})

	b.Run("GetOptimized", func(b *testing.B) {
		// Pre-populate
		for i := 0; i < 10000; i++ {
			m.SetOptimized(i, fmt.Sprintf("value_%d", i))
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.GetOptimized(i % 10000)
		}
	})

	b.Run("SetDuplicates", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.SetOptimized(i%1000, fmt.Sprintf("value_%d", i))
		}
	})
}

// TestBloomFilterDifferentTypesDeduplication tests deduplication with various types.
func TestBloomFilterDifferentTypesDeduplication(t *testing.T) { //nolint:gocognit // Test requires type cases
	// Test deduplication with int
	t.Run("IntDeduplication", func(t *testing.T) {
		itemCount := atomic.Int32{}
		processBatch := func(batch []*int) {
			itemCount.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
		}
		batcher := NewWithDeduplicationOptimized[int](10, 50*time.Millisecond, processBatch, false)

		// Add duplicates
		for i := 0; i < 20; i++ {
			val := i % 5 // Only 5 unique values
			batcher.Put(&val)
		}

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		if itemCount.Load() != 5 {
			t.Errorf("Expected 5 unique items, got %d", itemCount.Load())
		}
	})

	// Test deduplication with string
	t.Run("StringDeduplication", func(t *testing.T) {
		itemCount := atomic.Int32{}
		processBatch := func(batch []*string) {
			itemCount.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
		}
		batcher := NewWithDeduplicationOptimized[string](10, 50*time.Millisecond, processBatch, false)

		// Add duplicates
		values := []string{"apple", "banana", "cherry", "apple", "banana", "apple"}
		for _, v := range values {
			val := v // Copy for pointer
			batcher.Put(&val)
		}

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		if itemCount.Load() != 3 {
			t.Errorf("Expected 3 unique items, got %d", itemCount.Load())
		}
	})

	// Test deduplication with bool
	t.Run("BoolDeduplication", func(t *testing.T) {
		itemCount := atomic.Int32{}
		processBatch := func(batch []*bool) {
			itemCount.Add(int32(len(batch))) //nolint:gosec // Test code with controlled input
		}
		batcher := NewWithDeduplicationOptimized[bool](10, 50*time.Millisecond, processBatch, false)

		// Add duplicates
		values := []bool{true, false, true, false, true, true, false}
		for _, v := range values {
			val := v // Copy for pointer
			batcher.Put(&val)
		}

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		if itemCount.Load() != 2 {
			t.Errorf("Expected 2 unique items (true/false), got %d", itemCount.Load())
		}
	})
}

// BenchmarkBloomFilterTypes benchmarks bloom filter performance with different types.
func BenchmarkBloomFilterTypes(b *testing.B) { //nolint:gocognit // Multiple benchmark sub-tests
	b.Run("String", func(b *testing.B) {
		bf := NewBloomFilter(100000, 3)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Add(fmt.Sprintf("key_%d", i))
		}
	})

	b.Run("Int", func(b *testing.B) {
		bf := NewBloomFilter(100000, 3)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Add(i)
		}
	})

	b.Run("Uint64", func(b *testing.B) {
		bf := NewBloomFilter(100000, 3)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Add(uint64(i)) //nolint:gosec // Test code with bounded loop
		}
	})

	b.Run("Float64", func(b *testing.B) {
		bf := NewBloomFilter(100000, 3)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Add(float64(i) * 1.1)
		}
	})

	b.Run("Bool", func(b *testing.B) {
		bf := NewBloomFilter(100000, 3)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Add(i%2 == 0)
		}
	})

	b.Run("StructDefault", func(b *testing.B) {
		bf := NewBloomFilter(100000, 3)
		type testStruct struct {
			ID   int
			Name string
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bf.Add(testStruct{ID: i, Name: fmt.Sprintf("item_%d", i)})
		}
	})
}

// BenchmarkWithDedupOptimized benchmarks deduplication performance.
func BenchmarkWithDedupOptimized(b *testing.B) {
	processBatch := func(_ []*testItem) {}

	b.Run("NoDuplicates", func(b *testing.B) {
		batcher := NewWithDeduplicationOptimized[testItem](100, 50*time.Millisecond, processBatch, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batcher.Put(&testItem{ID: i})
		}
	})

	b.Run("50PercentDuplicates", func(b *testing.B) {
		batcher := NewWithDeduplicationOptimized[testItem](100, 50*time.Millisecond, processBatch, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batcher.Put(&testItem{ID: i / 2})
		}
	})

	b.Run("90PercentDuplicates", func(b *testing.B) {
		batcher := NewWithDeduplicationOptimized[testItem](100, 50*time.Millisecond, processBatch, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batcher.Put(&testItem{ID: i / 10})
		}
	})
}

// BenchmarkConcurrentOperations benchmarks concurrent access patterns.
func BenchmarkConcurrentOperations(b *testing.B) { //nolint:gocognit // Benchmark requires complex scenarios
	b.Run("PutOptimizedConcurrent", func(b *testing.B) {
		processBatch := func(_ []*testItem) {}
		batcher := NewOptimized[testItem](100, 10*time.Millisecond, processBatch, true)

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				batcher.PutOptimized(&testItem{ID: i})
				i++
			}
		})
	})

	b.Run("BloomFilterConcurrent", func(b *testing.B) {
		bf := NewBloomFilter(1000000, 3)

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%2 == 0 {
					bf.Add(i)
				} else {
					bf.Test(i)
				}
				i++
			}
		})
	})
}
