package batcher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Test item that implements comparable
type testItem struct {
	ID int
}

// Test_BatcherWithDeduplication tests the batcher with deduplication functionality.
func TestBatcherWithDeduplication(t *testing.T) { //nolint:gocognit,gocyclo // Complex test with multiple scenarios
	t.Run("Basic deduplication", func(t *testing.T) {
		// Create a channel to track processed batches
		processed := make(chan []*testItem, 10)

		// Create a function to process batches
		processBatch := func(batch []*testItem) {
			processed <- batch
		}

		// Create a batcher with deduplication
		batcher := NewWithDeduplication[testItem](10, 50*time.Millisecond, processBatch, false)

		// Add items one by one
		batcher.Put(&testItem{ID: 1})
		batcher.Put(&testItem{ID: 2})
		batcher.Put(&testItem{ID: 3})
		batcher.Put(&testItem{ID: 1}) // Duplicate, should be ignored
		batcher.Put(&testItem{ID: 2}) // Duplicate, should be ignored
		batcher.Put(&testItem{ID: 4})

		// Use a timeout to ensure the test doesn't hang
		select {
		case batch := <-processed:
			// Create a map to check for duplicates and count unique items
			seen := make(map[int]bool)

			for _, item := range batch {
				if item == nil {
					continue
				}

				seen[item.ID] = true
			}

			// Check that we have the expected number of unique items
			expectedIDs := []int{1, 2, 3, 4}
			if len(seen) != len(expectedIDs) {
				t.Errorf("Expected %d unique items in batch, got %d", len(expectedIDs), len(seen))
			}

			// Check that each expected ID is present
			for _, id := range expectedIDs {
				if !seen[id] {
					t.Errorf("Expected to find ID %d in batch, but it was missing", id)
				}
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Test timed out waiting for batch processing")
		}
	})

	t.Run("Deduplication window", func(t *testing.T) {
		// Test the TimePartitionedMap directly since that's what handles the deduplication window
		bucketDuration := 100 * time.Millisecond
		m := NewTimePartitionedMap[int, struct{}](bucketDuration, 1) // 1-second window

		// Add an item
		m.Set(1, struct{}{})

		// Check that the item exists
		_, exists := m.Get(1)
		if !exists {
			t.Errorf("Expected item 1 to exist in the map")
		}

		// Wait for the window to expire
		time.Sleep(bucketDuration * 2) // Sleep for 2x bucket duration

		// Check that the item no longer exists
		_, exists = m.Get(1)
		if exists {
			t.Errorf("Expected item 1 to be expired from the map")
		}
	})

	t.Run("Nil items", func(t *testing.T) {
		// Create a channel to track processed batches
		processed := make(chan []*testItem, 10)

		// Create a function to process batches
		processBatch := func(batch []*testItem) {
			processed <- batch
		}

		// Create a batcher with deduplication
		batcher := NewWithDeduplication[testItem](5, 50*time.Millisecond, processBatch, false)

		// Add a nil item - which should not have been added
		batcher.Put(nil)

		// Wait for the batch with a timeout
		select {
		case batch := <-processed:
			// Check that the batch contains the nil item
			if len(batch) != 0 {
				t.Errorf("Expected 0 item in batch, got %d", len(batch))
			}

			if batch[0] == nil {
				t.Errorf("Expected nil item in batch, got %v", batch[0])
			}
		case <-time.After(200 * time.Millisecond):
			t.Logf("Test correctly timed out waiting for batch processing")
		}
	})

	t.Run("Automatic batch processing", func(t *testing.T) {
		// Create a channel to track processed batches
		processed := make(chan []*testItem, 10)

		// Create a function to process batches
		processBatch := func(batch []*testItem) {
			processed <- batch
		}

		// Create a batcher with deduplication and small batch size
		batcher := NewWithDeduplication[testItem](3, 1*time.Second, processBatch, false)

		// Add items to trigger automatic batch processing
		batcher.Put(&testItem{ID: 1})
		batcher.Put(&testItem{ID: 2})
		batcher.Put(&testItem{ID: 3}) // This should trigger batch processing

		// Wait for the batch with a timeout
		select {
		case batch := <-processed:
			// Create a map to check for all expected IDs
			seen := make(map[int]bool)

			for _, item := range batch {
				if item == nil {
					continue
				}

				seen[item.ID] = true
			}

			// Check that all expected IDs are present
			for id := 1; id <= 3; id++ {
				if !seen[id] {
					t.Errorf("Expected to find ID %d in batch, but it was missing", id)
				}
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Test timed out waiting for batch")
		}
	})

	t.Run("Timeout batch processing", func(t *testing.T) {
		// Create a channel to track processed batches
		processed := make(chan []*testItem, 10)

		// Create a function to process batches
		processBatch := func(batch []*testItem) {
			processed <- batch
		}

		// Create a batcher with deduplication and short timeout
		batcher := NewWithDeduplication[testItem](10, 50*time.Millisecond, processBatch, false)

		// Add some items but not enough to trigger batch processing
		batcher.Put(&testItem{ID: 1})
		batcher.Put(&testItem{ID: 2})

		// Wait for the timeout to trigger batch processing
		select {
		case batch := <-processed:
			// Create a map to check for all expected IDs
			seen := make(map[int]bool)

			for _, item := range batch {
				if item == nil {
					continue
				}

				seen[item.ID] = true
			}

			// Check that all expected IDs are present
			for id := 1; id <= 2; id++ {
				if !seen[id] {
					t.Errorf("Expected to find ID %d in batch, but it was missing", id)
				}
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Test timed out waiting for batch")
		}
	})
}

// Test_TimePartitionedMap comprehensively tests the TimePartitionedMap data structure.
//
// This test suite validates the time-based partitioning, automatic expiration,
// and concurrent access patterns of the TimePartitionedMap. It ensures that
// the map correctly manages time buckets and provides accurate deduplication.
//
// Test cases included:
// - Key in different bucket: Tests that keys can exist in multiple buckets
// - Delete function: Validates deletion of individual keys
// - Delete from multiple buckets: Tests deletion across bucket boundaries
// - Multiple buckets with same key: Ensures proper handling of duplicate keys
// - Expired buckets cleanup: Verifies automatic removal of old buckets
// - Count method: Tests accurate counting across all buckets
// - Max buckets limit: Validates bucket limit enforcement
// - Large scale tests: Performance testing with many items
// - Concurrent access: Thread-safety validation
//
// Notes:
// - Uses various bucket durations to test different scenarios
// - Sleep durations are carefully chosen for deterministic behavior
// - Concurrent tests use errgroup for proper synchronization
func TestTimePartitionedMap(t *testing.T) { //nolint:gocognit,gocyclo // Comprehensive test suite with multiple scenarios
	t.Run("Key in different bucket", func(t *testing.T) {
		// Create a map with small bucket duration for testing
		bucketDuration := 100 * time.Millisecond
		m := NewTimePartitionedMap[int, string](bucketDuration, 3) // 3 buckets of 100 ms each

		// Add an item to the first bucket
		m.Set(1, "bucket1")

		// Wait for time to move to the next bucket
		time.Sleep(bucketDuration * 4) // Sleep for 4x bucket duration to ensure we're in a new bucket

		// Add the same key to the second bucket
		m.Set(1, "bucket2")

		// Get should return the value from the most recent bucket
		val, exists := m.Get(1)

		if !exists {
			t.Errorf("Expected key 1 to exist in the map")
		}

		if val != "bucket2" {
			t.Errorf("Expected value 'bucket2', got '%s'", val)
		}
	})

	t.Run("Delete function", func(t *testing.T) {
		// Create a map with multiple buckets
		bucketDuration := 100 * time.Millisecond
		m := NewTimePartitionedMap[int, string](bucketDuration, 3)

		// Add items to different buckets
		m.Set(1, "value1")
		m.Set(2, "value2")

		// Delete an item
		deleted := m.Delete(1)
		if !deleted {
			t.Errorf("Delete should return true when item exists")
		}

		// Verify item was deleted
		_, exists := m.Get(1)
		if exists {
			t.Errorf("Expected key 1 to be deleted from the map")
		}

		// Another item should still exist
		val, exists := m.Get(2)

		if !exists {
			t.Errorf("Expected key 2 to still exist in the map")
		}

		if val != "value2" {
			t.Errorf("Expected value 'value2', got '%s'", val)
		}

		// Delete non-existent item
		deleted = m.Delete(3)

		if deleted {
			t.Errorf("Delete should return false when item doesn't exist")
		}
	})

	t.Run("Delete from multiple buckets", func(t *testing.T) {
		// Create a map with multiple buckets
		bucketDuration := 100 * time.Millisecond
		m := NewTimePartitionedMap[int, string](bucketDuration, 3)

		// Add the same key to multiple buckets
		m.Set(1, "bucket1")

		time.Sleep(bucketDuration * 2) // Sleep for 2x bucket duration
		m.Set(1, "bucket2")

		time.Sleep(bucketDuration * 2) // Sleep for 2x bucket duration
		m.Set(1, "bucket3")

		// Delete the key
		deleted := m.Delete(1)
		if !deleted {
			t.Errorf("Delete should return true when item exists")
		}

		// Verify item was deleted from all buckets
		_, exists := m.Get(1)
		if exists {
			t.Errorf("Expected key 1 to be deleted from all buckets")
		}
	})

	t.Run("Multiple buckets with same key", func(t *testing.T) {
		// Create a map with multiple small buckets
		bucketDuration := 100 * time.Millisecond
		m := NewTimePartitionedMap[int, string](bucketDuration, 3)

		// Add key to first bucket
		m.Set(1, "bucket1")

		// Wait for time to move to the second bucket
		time.Sleep(bucketDuration * 2) // Sleep for 2x bucket duration

		// Add same key to second bucket
		m.Set(1, "bucket2")

		// Wait for time to move to the third bucket
		time.Sleep(bucketDuration * 2) // Sleep for 2x bucket duration

		// Add same key to third bucket
		m.Set(1, "bucket3")

		// Get should return the value from the most recent bucket
		val, exists := m.Get(1)
		if !exists {
			t.Errorf("Expected key 1 to exist in the map")
		}

		if val != "bucket3" {
			t.Errorf("Expected value 'bucket3', got '%s'", val)
		}

		// Delete the key
		m.Delete(1)

		// Key should no longer exist
		_, exists = m.Get(1)
		if exists {
			t.Errorf("Expected key 1 to be deleted from the map")
		}
	})

	t.Run("Expired buckets cleanup", func(t *testing.T) {
		// Create a map with larger bucket duration to make test more stable
		bucketDuration := 500 * time.Millisecond
		m := NewTimePartitionedMap[int, string](bucketDuration, 2)

		// Add items to the first bucket
		m.Set(1, "value1")
		m.Set(2, "value2")

		// Wait for half a bucket duration before adding to the second bucket
		time.Sleep(bucketDuration / 2)

		// Add items to the second bucket
		m.Set(3, "value3")
		m.Set(4, "value4")

		// Verify all items exist initially
		for i := 1; i <= 4; i++ {
			_, exists := m.Get(i)
			require.True(t, exists, "key %d should exist initially", i)
		}

		// Wait for the first bucket to expire and force a cleanup
		time.Sleep(bucketDuration * 2)

		// Add items to the third bucket to trigger cleanup of the first bucket
		m.Set(5, "value5")
		m.Set(6, "value6")

		// Force a second cleanup attempt to ensure consistency
		time.Sleep(bucketDuration / 2)
		m.Set(7, "value7")

		// The First bucket items should be gone
		for i := 1; i <= 2; i++ {
			_, exists := m.Get(i)
			require.False(t, exists, "key %d should be expired", i)
		}

		// Later items should still exist
		val5, exists5 := m.Get(5)
		require.True(t, exists5, "key 5 should exist")
		require.Equal(t, "value5", val5)

		// Add more items to ensure cleanup
		m.Set(8, "value8")
		m.Set(9, "value9")

		// Wait for the second bucket to expire and force a cleanup
		time.Sleep(bucketDuration * 2)

		// Add items to the fourth bucket to trigger cleanup of the second bucket
		m.Set(10, "value10")
		m.Set(11, "value11")

		// Force a second cleanup attempt to ensure consistency
		time.Sleep(bucketDuration / 2)
		m.Set(12, "value12")

		// Second bucket items should be gone
		for i := 5; i <= 6; i++ {
			_, exists := m.Get(i)
			require.False(t, exists, "key %d should be expired", i)
		}

		// Later items should still exist
		val10, exists10 := m.Get(10)
		require.True(t, exists10, "key 10 should exist")
		require.Equal(t, "value10", val10)
	})

	t.Run("Count method", func(t *testing.T) {
		// Create a map
		bucketDuration := 100 * time.Millisecond
		m := NewTimePartitionedMap[int, string](bucketDuration, 3)

		// Initially count should be 0
		if count := m.Count(); count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}

		// Add items
		setOK := m.Set(1, "value1")
		require.True(t, setOK)

		setOK = m.Set(2, "value2")
		require.True(t, setOK)

		// Count should be 2
		if count := m.Count(); count != 2 {
			t.Errorf("Expected count 2, got %d", count)
		}

		setOK = m.Set(1, "value1-updated")
		require.False(t, setOK) // should not be set, since it already exists

		// Count should still be 2 (not 3) since we're replacing a key
		if count := m.Count(); count != 2 {
			t.Errorf("Expected count 2, got %d", count)
		}

		// Delete an item
		m.Delete(1)

		// Count should be 1
		if count := m.Count(); count != 1 {
			t.Errorf("Expected count 1, got %d", count)
		}

		// Delete all items to ensure the count is 0
		m.Delete(2)

		// Count should be 0 after all items are deleted
		if count := m.Count(); count != 0 {
			t.Errorf("Expected count 0 after deletion, got %d", count)
		}
	})

	t.Run("Max buckets limit", func(t *testing.T) {
		// Create a map with only 2 buckets
		bucketDuration := 100 * time.Millisecond
		m := NewTimePartitionedMap[int, string](bucketDuration, 2)

		// Add items to the first bucket
		setOK := m.Set(1, "bucket1")
		require.True(t, setOK)

		// Wait and add to the second bucket
		time.Sleep(bucketDuration * 2) // Sleep for 2x bucket duration

		setOK = m.Set(2, "bucket2")
		require.True(t, setOK)

		// Wait and add to the third bucket (should cause the first bucket to be removed)
		time.Sleep(bucketDuration * 2) // Sleep for 2x bucket duration

		setOK = m.Set(3, "bucket3")
		require.True(t, setOK)

		// Force a cleanup by accessing the map
		setOK = m.Set(4, "bucket4") // This will trigger a cleanup
		require.True(t, setOK)

		// The First bucket item should be gone due to the max buckets limit
		_, exists1 := m.Get(1)
		val3, exists3 := m.Get(3)
		val4, exists4 := m.Get(4)

		if exists1 {
			t.Errorf("Expected key 1 to be removed due to max buckets limit")
		}

		if !exists3 || val3 != "bucket3" {
			t.Errorf("Expected key 3 to exist with value 'bucket3'")
		}

		if !exists4 || val4 != "bucket4" {
			t.Errorf("Expected key 4 to exist with value 'bucket4'")
		}
	})

	t.Run("large test", func(t *testing.T) {
		// Create a map with 1-second buckets and 60 buckets (1-minute window)
		m := NewTimePartitionedMap[int, struct{}](time.Second, 60)

		var (
			nrItems = 100_000
			setOK   bool
			found   = 0
			exists  bool
		)

		// Insert a large number of items
		for i := 0; i < nrItems; i++ {
			setOK = m.Set(i, struct{}{})
			require.True(t, setOK)
		}

		for i := 0; i < nrItems; i++ {
			_, exists = m.Get(i)
			if exists {
				found++
			}
		}

		if found != nrItems {
			t.Fatalf("Expected to find %d items, found %d", nrItems, found)
		}
	})

	t.Run("large concurrent test", func(t *testing.T) {
		// Create a map with 1-second buckets and 60 buckets (1-minute window)
		m := NewTimePartitionedMap[int, struct{}](time.Second, 60)

		var (
			g       = errgroup.Group{}
			nrItems = 100_000
			found   = atomic.Int64{}
		)

		// Insert a large number of items
		for i := 0; i < nrItems; i++ {
			g.Go(func() error {
				setOK := m.Set(i, struct{}{})
				require.True(t, setOK)

				return nil
			})
		}

		require.NoError(t, g.Wait())

		for i := 0; i < nrItems; i++ {
			g.Go(func() error {
				_, exists := m.Get(i)
				if exists {
					found.Add(1)
				} else {
					t.Logf("Item %d not found", i)
				}

				return nil
			})
		}

		require.NoError(t, g.Wait())

		if found.Load() != int64(nrItems) {
			t.Fatalf("Expected to find %d items, found %d", 1_000_000, found.Load())
		}
	})
}

// TestTimePartitionedMapOptimized verifies the optimized Get and Set methods.
func TestTimePartitionedMapOptimized(t *testing.T) {
	m := NewTimePartitionedMap[string, int](time.Second, 5)
	defer m.Close()
	// Test Set with bloom filter
	if !m.Set("key1", 1) {
		t.Error("Expected Set to return true for new key")
	}
	// Test duplicate detection
	if m.Set("key1", 2) {
		t.Error("Expected Set to return false for duplicate key")
	}
	// Test Get with bloom filter
	val, exists := m.Get("key1")
	if !exists || val != 1 {
		t.Errorf("Expected Get to find key1 with value 1, got %v, %v", val, exists)
	}
	// Test non-existent key
	_, exists = m.Get("nonexistent")
	if exists {
		t.Error("Expected Get to return false for non-existent key")
	}
}

// TestWithDeduplicationAndPool tests the new pooling version.
func TestWithDeduplicationAndPool(t *testing.T) {
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
	batcher := NewWithDeduplicationAndPool[testItem](10, 50*time.Millisecond, processBatch, false)
	defer batcher.Close()
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
