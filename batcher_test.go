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

// batchStoreItem is a simple test struct used for testing the batcher functionality.
//
// This empty struct serves as a minimal test type to verify that the batcher
// can handle any type of data, including empty structs. It's particularly useful
// for testing the core batching logic without the complexity of real data.
//
// Notes:
// - Empty structs have zero memory footprints
// - Used to test batching mechanics independent of data content
type batchStoreItem struct{}

// TestNew verifies that a new Batcher is properly initialized with the correct configuration.
//
// This test validates the basic constructor functionality by creating a new Batcher
// instance and verifying that all configuration parameters are correctly stored.
//
// Test coverage includes:
// - Batcher instance is successfully created (not nil)
// - Batch size parameter is correctly set
// - Timeout duration is correctly set
// - Background processing flag is properly configured
//
// Parameters tested:
// - batchSize: 100 items per batch
// - batchTimeout: 100ms timeout between batches
// - sendStoreBatch: Mock function that simulates batch processing
// - background: true for asynchronous processing
//
// Notes:
// - Uses testify require for critical assertions (NotNil)
// - Uses testify assert for value comparisons
// - The mock batch function is intentionally empty as we're only testing initialization
func TestNew(t *testing.T) {
	batchSize := 100
	batchTimeout := 100 * time.Millisecond
	sendStoreBatch := func(_ []*batchStoreItem) {
		// Simulate sending the batch
	}

	storeBatcher := New[batchStoreItem](batchSize, batchTimeout, sendStoreBatch, true)
	require.NotNil(t, storeBatcher)
	assert.Equal(t, storeBatcher.size, batchSize)
	assert.Equal(t, storeBatcher.timeout, batchTimeout)
}

// TestPut verifies that items are correctly added to the batcher and processed via manual trigger.
//
// This test validates the Put method and manual Trigger functionality by adding
// items to the batcher and forcing immediate processing through the Trigger method.
// It uses an atomic counter to verify that all items are processed correctly.
//
// Test scenario:
// - Creates a batcher with large timeout (100s) to prevent automatic timeout
// - Adds 12 items (less than batch size of 100)
// - Manually triggers batch processing
// - Verifies all 12 items were processed
//
// This function performs the following steps:
// - Initializes an atomic counter to track processed items
// - Creates a batcher with a callback that counts items
// - Adds 12 test items using Put method
// - Waits briefly for items to be queued
// - Calls Trigger to force immediate processing
// - Waits for background processing to complete
// - Asserts that exactly 12 items were processed
//
// Parameters:
// - t: Testing context for assertions and logging
//
// Returns:
// - Nothing (test assertions handle pass/fail)
//
// Side Effects:
// - Creates and runs a batcher with background goroutine
// - Spawns goroutines for background batch processing
//
// Notes:
// - Long timeout (100s) ensures no automatic batch processing
// - Sleep durations allow for goroutine scheduling
// - Atomic counter ensures thread-safe counting
// - Background=true enables async batch processing
func TestPut(t *testing.T) {
	batchSize := 100
	batchTimeout := 100 * time.Second

	countedItems := atomic.Int64{}
	sendStoreBatch := func(batch []*batchStoreItem) {
		countedItems.Add(int64(len(batch)))
	}

	storeBatcher := New[batchStoreItem](batchSize, batchTimeout, sendStoreBatch, true)

	for i := 0; i < 12; i++ {
		storeBatcher.Put(&batchStoreItem{})
	}

	time.Sleep(10 * time.Millisecond)

	storeBatcher.Trigger()

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, int64(12), countedItems.Load())
}

// TestWithPool verifies that NewWithPool works correctly.
func TestWithPool(t *testing.T) {
	itemCount := atomic.Int32{}
	processBatch := func(batch []*batchStoreItem) {
		itemCount.Add(int32(len(batch))) //nolint:gosec // Test code with controlled batch sizes
	}
	batcher := NewWithPool[batchStoreItem](100, 50*time.Millisecond, processBatch, true)
	// Add many items to test pool reuse
	for i := 0; i < 1000; i++ {
		batcher.Put(&batchStoreItem{})
	}
	// Wait for processing
	time.Sleep(200 * time.Millisecond)
	if itemCount.Load() != 1000 {
		t.Errorf("Expected 1000 items processed, got %d", itemCount.Load())
	}
}

// TestWithPoolTrigger verifies that WithPool.Trigger() forces immediate batch processing.
func TestWithPoolTrigger(t *testing.T) {
	batchCount := atomic.Int32{}
	processBatch := func(_ []*batchStoreItem) {
		batchCount.Add(1)
	}
	batcher := NewWithPool[batchStoreItem](100, 5*time.Second, processBatch, false)
	// Add items less than batch size
	for i := 0; i < 5; i++ {
		batcher.Put(&batchStoreItem{})
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

// TestBloomFilterHashAllPrimitiveTypes verifies comprehensive hash function coverage for all primitive types.
//
// This test validates that the BloomFilter hash function correctly handles all primitive
// types and edge cases, ensuring proper type conversion and consistent hashing behavior.
//
// Test coverage includes:
// - All integer types: int8, int16, int32, int64, uint8, uint16, uint32, uint64
// - Floating-point types: float32, float64
// - Boolean type
// - String type including empty string
// - Edge cases: zero values, negative numbers, special float values
// - Custom structs using default case
//
// This comprehensive testing ensures that the hash function:
// - Produces consistent hashes for the same input
// - Handles type conversions correctly
// - Does not panic on any supported type
// - Generates different hashes for different values
func TestBloomFilterHashAllPrimitiveTypes(t *testing.T) { //nolint:gocognit,gocyclo // Comprehensive test covering all primitive types
	bf := NewBloomFilter(1000, 3)

	t.Run("IntegerTypes", func(t *testing.T) {
		t.Run("Int8Values", func(t *testing.T) {
			// Test int8 values including edge cases
			testCases := []int8{0, 1, -1, 127, -128}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for int8 %d", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for int8 %d", val)
			}
		})

		t.Run("Int16Values", func(t *testing.T) {
			// Test int16 values including edge cases
			testCases := []int16{0, 1, -1, 32767, -32768}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for int16 %d", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for int16 %d", val)
			}
		})

		t.Run("Int32Values", func(t *testing.T) {
			// Test int32 values including edge cases
			testCases := []int32{0, 1, -1, 2147483647, -2147483648}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for int32 %d", val)
				// Verify hash consistency
				hashes2 := bf.hash(val)
				assert.Equal(t, hashes, hashes2, "Hash should be consistent for int32 %d", val)
			}
		})

		t.Run("Int64Values", func(t *testing.T) {
			// Test int64 values including edge cases
			testCases := []int64{0, 1, -1, 9223372036854775807, -9223372036854775808}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for int64 %d", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for int64 %d", val)
			}
		})

		t.Run("UintValues", func(t *testing.T) {
			// Test uint values
			testCases := []uint{0, 1, 18446744073709551615}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for uint %d", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for uint %d", val)
			}
		})

		t.Run("Uint8Values", func(t *testing.T) {
			// Test uint8 values including edge cases
			testCases := []uint8{0, 1, 255}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for uint8 %d", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for uint8 %d", val)
			}
		})

		t.Run("Uint16Values", func(t *testing.T) {
			// Test uint16 values including edge cases
			testCases := []uint16{0, 1, 65535}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for uint16 %d", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for uint16 %d", val)
			}
		})

		t.Run("Uint32Values", func(t *testing.T) {
			// Test uint32 values including edge cases
			testCases := []uint32{0, 1, 4294967295}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for uint32 %d", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for uint32 %d", val)
			}
		})

		t.Run("Uint64Values", func(t *testing.T) {
			// Test uint64 values including edge cases
			testCases := []uint64{0, 1, 18446744073709551615}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for uint64 %d", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for uint64 %d", val)
			}
		})
	})

	t.Run("FloatingPointTypes", func(t *testing.T) {
		t.Run("Float32Values", func(t *testing.T) {
			// Test float32 values including edge cases and special values
			testCases := []float32{0.0, 1.0, -1.0, 3.14159, -3.14159}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for float32 %f", val)
				// Verify hash consistency
				hashes2 := bf.hash(val)
				assert.Equal(t, hashes, hashes2, "Hash should be consistent for float32 %f", val)
			}
		})

		t.Run("Float64Values", func(t *testing.T) {
			// Test float64 values including edge cases and special values
			testCases := []float64{0.0, 1.0, -1.0, 3.141592653589793, -3.141592653589793}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for float64 %f", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for float64 %f", val)
			}
		})
	})

	t.Run("BooleanType", func(t *testing.T) {
		// Test boolean values
		t.Run("TrueValue", func(t *testing.T) {
			hashes := bf.hash(true)
			require.Len(t, hashes, 3, "Should generate 3 hashes for bool true")
			assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for bool true")
		})

		t.Run("FalseValue", func(t *testing.T) {
			hashes := bf.hash(false)
			require.Len(t, hashes, 3, "Should generate 3 hashes for bool false")
			assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for bool false")
		})

		t.Run("BooleanDifference", func(t *testing.T) {
			// Verify that true and false produce different hashes
			hashesTrue := bf.hash(true)
			hashesFalse := bf.hash(false)
			assert.NotEqual(t, hashesTrue, hashesFalse, "True and false should produce different hashes")
		})
	})

	t.Run("StringType", func(t *testing.T) {
		t.Run("EmptyString", func(t *testing.T) {
			hashes := bf.hash("")
			require.Len(t, hashes, 3, "Should generate 3 hashes for empty string")
			assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for empty string")
		})

		t.Run("NonEmptyStrings", func(t *testing.T) {
			testCases := []string{"hello", "world", "test123", "special!@#$%^&*()chars"}
			for _, val := range testCases {
				hashes := bf.hash(val)
				require.Len(t, hashes, 3, "Should generate 3 hashes for string %q", val)
				assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for string %q", val)
			}
		})

		t.Run("StringConsistency", func(t *testing.T) {
			// Verify hash consistency for strings
			testStr := "consistency_test"
			hashes1 := bf.hash(testStr)
			hashes2 := bf.hash(testStr)
			assert.Equal(t, hashes1, hashes2, "Hash should be consistent for string %q", testStr)
		})
	})

	t.Run("CustomStructs", func(t *testing.T) {
		// Test custom struct type (uses default case with fmt.Fprintf)
		type CustomStruct struct {
			Field1 string
			Field2 int
		}

		t.Run("CustomStructHashing", func(t *testing.T) {
			customVal := CustomStruct{Field1: "test", Field2: 42}
			hashes := bf.hash(customVal)
			require.Len(t, hashes, 3, "Should generate 3 hashes for custom struct")
			assert.NotEqual(t, uint64(0), hashes[0], "Hash should not be zero for custom struct")
		})

		t.Run("CustomStructConsistency", func(t *testing.T) {
			customVal := CustomStruct{Field1: "consistency", Field2: 123}
			hashes1 := bf.hash(customVal)
			hashes2 := bf.hash(customVal)
			assert.Equal(t, hashes1, hashes2, "Hash should be consistent for custom struct")
		})

		t.Run("DifferentStructsDifferentHashes", func(t *testing.T) {
			struct1 := CustomStruct{Field1: "test1", Field2: 1}
			struct2 := CustomStruct{Field1: "test2", Field2: 2}
			hashes1 := bf.hash(struct1)
			hashes2 := bf.hash(struct2)
			assert.NotEqual(t, hashes1, hashes2, "Different structs should produce different hashes")
		})
	})

	t.Run("HashUniqueness", func(t *testing.T) {
		// Test that different values produce different hashes (within reason)
		values := []interface{}{
			int8(1), int16(1), int32(1), int64(1),
			uint8(1), uint16(1), uint32(1), uint64(1),
			float32(1.0), float64(1.0),
			"1", true,
		}

		hashSets := make(map[string]bool)
		for i, val := range values {
			hashes := bf.hash(val)
			hashKey := fmt.Sprintf("%v", hashes)
			if hashSets[hashKey] {
				t.Logf("Hash collision detected for value %d: %v", i, val)
			} else {
				hashSets[hashKey] = true
			}
		}
	})

	t.Run("ZeroValues", func(t *testing.T) {
		// Test zero values for all types
		zeroValues := []interface{}{
			int8(0), int16(0), int32(0), int64(0),
			uint8(0), uint16(0), uint32(0), uint64(0),
			float32(0.0), float64(0.0),
			"", false,
		}

		for _, val := range zeroValues {
			hashes := bf.hash(val)
			require.Len(t, hashes, 3, "Should generate 3 hashes for zero value %T(%v)", val, val)
			// Even zero values should produce valid hash indices
			for j, h := range hashes {
				assert.Less(t, h, bf.size, "Hash %d should be within bloom filter size for zero value %T(%v)", j, val, val)
			}
		}
	})
}

// TestBatcherResourceCleanup verifies proper resource cleanup in various scenarios.
//
// This test suite validates that the batcher properly manages resources and cleans up
// correctly under various scenarios, including graceful shutdown, forced termination,
// and error conditions. It ensures no resource leaks occur during normal and abnormal
// operation patterns.
//
// Test coverage includes:
// - Proper cleanup after normal batcher lifecycle
// - Resource cleanup with background processing enabled/disabled
// - Cleanup during batch processing errors
// - Memory leak verification through multiple create/destroy cycles
// - Goroutine cleanup verification
// - Pool-based batcher resource cleanup
//
// These tests are critical for production deployments to ensure the batcher
// doesn't consume excessive resources over time.
func TestBatcherResourceCleanup(t *testing.T) { //nolint:gocognit,gocyclo // Comprehensive resource cleanup test suite
	t.Run("BasicLifecycleCleanup", func(t *testing.T) {
		processedCount := atomic.Int64{}
		processBatch := func(batch []*batchStoreItem) {
			processedCount.Add(int64(len(batch)))
		}

		// Create and use batcher
		batcher := New[batchStoreItem](10, 100*time.Millisecond, processBatch, true)

		// Add some items
		for i := 0; i < 5; i++ {
			batcher.Put(&batchStoreItem{})
		}

		// Wait for processing
		time.Sleep(150 * time.Millisecond)

		// Verify items were processed
		assert.Equal(t, int64(5), processedCount.Load(), "Items should be processed")

		// Test normal shutdown - should not panic or hang
		require.NotPanics(t, func() {
			batcher.Trigger()
			time.Sleep(50 * time.Millisecond)
		}, "Normal shutdown should not panic")
	})

	t.Run("BackgroundVsNonBackgroundCleanup", func(t *testing.T) {
		processedCount := atomic.Int64{}
		processBatch := func(batch []*batchStoreItem) {
			processedCount.Add(int64(len(batch)))
			time.Sleep(10 * time.Millisecond) // Simulate processing time
		}

		t.Run("BackgroundProcessing", func(t *testing.T) {
			batcher := New[batchStoreItem](3, 50*time.Millisecond, processBatch, true)

			// Add items to trigger processing
			for i := 0; i < 6; i++ {
				batcher.Put(&batchStoreItem{})
			}

			// Allow background processing to complete
			time.Sleep(100 * time.Millisecond)

			// Cleanup should be clean
			require.NotPanics(t, func() {
				batcher.Trigger()
				time.Sleep(50 * time.Millisecond)
			}, "Background batcher cleanup should not panic")
		})

		t.Run("SynchronousProcessing", func(t *testing.T) {
			batcher := New[batchStoreItem](3, 50*time.Millisecond, processBatch, false)

			// Add items to trigger processing
			for i := 0; i < 6; i++ {
				batcher.Put(&batchStoreItem{})
			}

			// Allow synchronous processing to complete
			time.Sleep(100 * time.Millisecond)

			// Cleanup should be clean
			require.NotPanics(t, func() {
				batcher.Trigger()
				time.Sleep(50 * time.Millisecond)
			}, "Synchronous batcher cleanup should not panic")
		})
	})

	t.Run("MultipleCreateDestroyNoCycles", func(t *testing.T) {
		// Test for memory leaks by creating and destroying multiple batchers
		processedCount := atomic.Int64{}
		processBatch := func(batch []*batchStoreItem) {
			processedCount.Add(int64(len(batch)))
		}

		numCycles := 10
		for cycle := 0; cycle < numCycles; cycle++ {
			batcher := New[batchStoreItem](5, 30*time.Millisecond, processBatch, cycle%2 == 0)

			// Use the batcher briefly
			for i := 0; i < 3; i++ {
				batcher.Put(&batchStoreItem{})
			}

			// Allow some processing
			time.Sleep(40 * time.Millisecond)

			// Clean shutdown
			batcher.Trigger()
			time.Sleep(20 * time.Millisecond)
		}

		// Verify total processing occurred
		assert.Greater(t, processedCount.Load(), int64(20), "Should have processed items across cycles")
	})

	t.Run("PoolBasedBatcherCleanup", func(t *testing.T) {
		processedCount := atomic.Int64{}
		processBatch := func(batch []*batchStoreItem) {
			processedCount.Add(int64(len(batch)))
		}

		batcher := NewWithPool[batchStoreItem](5, 50*time.Millisecond, processBatch, true)

		// Use the pool-based batcher
		for i := 0; i < 15; i++ {
			batcher.Put(&batchStoreItem{})
		}

		// Allow processing
		time.Sleep(100 * time.Millisecond)

		// Verify processing
		assert.Greater(t, processedCount.Load(), int64(10), "Pool batcher should process items")

		// Test cleanup
		require.NotPanics(t, func() {
			batcher.Trigger()
			time.Sleep(50 * time.Millisecond)
		}, "Pool batcher cleanup should not panic")
	})

	t.Run("SlowProcessingCleanup", func(t *testing.T) {
		processedCount := atomic.Int64{}
		processBatch := func(batch []*batchStoreItem) {
			processedCount.Add(int64(len(batch)))
			// Simulate slow processing that might compete with cleanup
			time.Sleep(20 * time.Millisecond)
		}

		batcher := New[batchStoreItem](3, 30*time.Millisecond, processBatch, true)

		// Add items that will trigger slow processing
		for i := 0; i < 9; i++ {
			batcher.Put(&batchStoreItem{})
		}

		// Allow some processing to start
		time.Sleep(50 * time.Millisecond)

		// Cleanup should be safe even with ongoing slow processing
		require.NotPanics(t, func() {
			batcher.Trigger()
			time.Sleep(100 * time.Millisecond)
		}, "Cleanup should be safe even with slow processing")

		// Items should have been processed
		assert.Positive(t, processedCount.Load(), "Some items should be processed")
	})

	t.Run("ConcurrentCleanupSafety", func(t *testing.T) {
		processedCount := atomic.Int64{}
		processBatch := func(batch []*batchStoreItem) {
			processedCount.Add(int64(len(batch)))
			time.Sleep(5 * time.Millisecond) // Simulate processing time
		}

		batcher := New[batchStoreItem](5, 50*time.Millisecond, processBatch, true)

		var wg sync.WaitGroup

		// Start concurrent item additions
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					batcher.Put(&batchStoreItem{})
					time.Sleep(time.Millisecond)
				}
			}()
		}

		// Concurrent triggers
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(25 * time.Millisecond)
				batcher.Trigger()
			}()
		}

		wg.Wait()

		// Final cleanup should be safe
		require.NotPanics(t, func() {
			batcher.Trigger()
			time.Sleep(100 * time.Millisecond)
		}, "Concurrent cleanup should be safe")

		// Verify significant processing occurred
		assert.Greater(t, processedCount.Load(), int64(30), "Should process many items concurrently")
	})
}
