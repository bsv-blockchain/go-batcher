package batcher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPutOptimized verifies that PutOptimized behaves correctly.
func TestPutOptimized(t *testing.T) {
	itemCount := atomic.Int32{}
	processBatch := func(batch []*testItem) {
		itemCount.Add(int32(len(batch)))
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

// TestBatcherWithPool verifies that BatcherWithPool works correctly.
func TestBatcherWithPool(t *testing.T) {
	itemCount := atomic.Int32{}
	processBatch := func(batch []*testItem) {
		itemCount.Add(int32(len(batch)))
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

// TestBatcherWithPoolConcurrent verifies thread safety of BatcherWithPool.
func TestBatcherWithPoolConcurrent(t *testing.T) {
	itemCount := atomic.Int32{}
	processBatch := func(batch []*testItem) {
		itemCount.Add(int32(len(batch)))
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

	expectedItems := int32(goroutines * itemsPerGoroutine)
	if itemCount.Load() != expectedItems {
		t.Errorf("Expected %d items processed, got %d", expectedItems, itemCount.Load())
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

// TestBatcherWithDedupOptimized verifies optimized deduplication functionality.
func TestBatcherWithDedupOptimized(t *testing.T) {
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
			itemCount.Add(int32(len(batch)))
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