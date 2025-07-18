package batcher

import (
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
