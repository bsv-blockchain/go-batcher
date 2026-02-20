// Package main provides comprehensive tests for the examples package demonstrating go-batcher usage.
//
// This test file validates the helper functions and core functionality demonstrated in the
// examples package, ensuring that the example code works correctly and handles edge cases properly.
//
// Test coverage includes:
// - cryptoRandInt function validation (bounds checking, error handling)
// - generateRandomEvent function validation (field generation, consistency)
// - EventProcessor.ProcessBatch method validation (processing logic, concurrency safety)
// - Integration scenarios and edge cases
//
// These tests ensure that the example code is not only demonstrative but also robust
// and production-ready, following Go testing best practices with testify assertions.
package main

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCryptoRandIntBounds verifies that cryptoRandInt generates values within expected bounds.
//
// This test validates the core functionality of the cryptoRandInt function by:
// - Testing various maximum values to ensure proper bound checking
// - Verifying that generated values are always within [0, maxValue) range
// - Testing edge cases like maxValue = 1 and larger values
// - Ensuring consistent behavior across multiple invocations
//
// The test is deterministic despite using random generation by testing bounds
// rather than specific values, making it suitable for CI/CD environments.
func TestCryptoRandIntBounds(t *testing.T) { //nolint:gocognit // Comprehensive bounds testing with multiple scenarios
	t.Run("SingleBound", func(t *testing.T) {
		// Test with maxValue = 1, should always return 0
		for i := 0; i < 10; i++ {
			result := cryptoRandInt(1)
			assert.Equal(t, 0, result, "cryptoRandInt(1) should always return 0")
		}
	})

	t.Run("SmallBounds", func(t *testing.T) {
		// Test with small bounds
		maxValues := []int{2, 5, 10, 100}
		for _, maxVal := range maxValues {
			for i := 0; i < 50; i++ {
				result := cryptoRandInt(maxVal)
				assert.GreaterOrEqual(t, result, 0, "Result should be >= 0 for maxValue %d", maxVal)
				assert.Less(t, result, maxVal, "Result should be < maxValue %d", maxVal)
			}
		}
	})

	t.Run("LargeBounds", func(t *testing.T) {
		// Test with larger bounds
		maxValues := []int{1000, 10000, 100000}
		for _, maxVal := range maxValues {
			for i := 0; i < 20; i++ {
				result := cryptoRandInt(maxVal)
				assert.GreaterOrEqual(t, result, 0, "Result should be >= 0 for maxValue %d", maxVal)
				assert.Less(t, result, maxVal, "Result should be < maxValue %d", maxVal)
			}
		}
	})

	t.Run("ConsistentBehavior", func(t *testing.T) {
		// Verify that function doesn't panic and behaves consistently
		maxVal := 50
		results := make([]int, 100)
		for i := 0; i < 100; i++ {
			results[i] = cryptoRandInt(maxVal)
		}

		// All results should be within bounds
		for i, result := range results {
			assert.GreaterOrEqual(t, result, 0, "Result %d should be >= 0", i)
			assert.Less(t, result, maxVal, "Result %d should be < maxValue", i)
		}

		// Verify we get some distribution (not all the same value)
		// This is probabilistic but with 100 samples from [0,50), we should see variation
		unique := make(map[int]bool)
		for _, result := range results {
			unique[result] = true
		}
		assert.Greater(t, len(unique), 1, "Should generate more than one unique value in 100 samples")
	})
}

// TestGenerateRandomEventStructure verifies that generateRandomEvent creates properly structured events.
//
// This test validates the generateRandomEvent function by:
// - Checking that all required fields are populated
// - Verifying field types and constraints
// - Testing that events have reasonable variety in generated values
// - Ensuring consistent structure across multiple invocations
// - Validating metadata map structure and content
//
// The test focuses on structure validation rather than specific random values,
// making it deterministic and suitable for automated testing environments.
func TestGenerateRandomEventStructure(t *testing.T) {
	t.Run("BasicStructure", func(t *testing.T) {
		event := generateRandomEvent()

		// Verify all required fields are present and non-empty
		require.NotNil(t, event, "Generated event should not be nil")
		assert.NotEmpty(t, event.UserID, "UserID should not be empty")
		assert.NotEmpty(t, event.EventType, "EventType should not be empty")
		assert.False(t, event.Timestamp.IsZero(), "Timestamp should not be zero")
		require.NotNil(t, event.Metadata, "Metadata should not be nil")
		assert.NotEmpty(t, event.Metadata, "Metadata should not be empty")
	})

	t.Run("ValidFieldValues", func(t *testing.T) {
		// Valid user IDs and event types from the implementation
		validUserIDs := map[string]bool{
			"user001": true, "user002": true, "user003": true,
			"user004": true, "user005": true,
		}
		validEventTypes := map[string]bool{
			"page_view": true, "button_click": true, "form_submit": true,
			"api_call": true, "login": true,
		}

		// Test multiple events to ensure values are from valid sets
		for i := 0; i < 20; i++ {
			event := generateRandomEvent()

			assert.True(t, validUserIDs[event.UserID], "UserID %q should be from valid set", event.UserID)
			assert.True(t, validEventTypes[event.EventType], "EventType %q should be from valid set", event.EventType)

			// Verify timestamp is recent (within last second)
			now := time.Now()
			assert.WithinDuration(t, now, event.Timestamp, time.Second, "Timestamp should be recent")
		}
	})

	t.Run("MetadataStructure", func(t *testing.T) {
		event := generateRandomEvent()

		// Verify metadata contains expected keys
		require.Contains(t, event.Metadata, "session_id", "Metadata should contain session_id")
		require.Contains(t, event.Metadata, "ip_address", "Metadata should contain ip_address")

		// Verify metadata values are properly formatted
		sessionID := event.Metadata["session_id"]
		assert.Contains(t, sessionID, "session_", "Session ID should contain 'session_' prefix")

		ipAddress := event.Metadata["ip_address"]
		assert.Contains(t, ipAddress, "192.168.1.", "IP address should contain '192.168.1.' prefix")
	})

	t.Run("EventVariety", func(t *testing.T) {
		// Generate multiple events and verify we get variety
		events := make([]*UserEvent, 50)
		for i := 0; i < 50; i++ {
			events[i] = generateRandomEvent()
		}

		// Collect unique values
		uniqueUserIDs := make(map[string]bool)
		uniqueEventTypes := make(map[string]bool)
		uniqueSessionIDs := make(map[string]bool)

		for _, event := range events {
			uniqueUserIDs[event.UserID] = true
			uniqueEventTypes[event.EventType] = true
			uniqueSessionIDs[event.Metadata["session_id"]] = true
		}

		// With 50 events, we should see multiple different values
		assert.Greater(t, len(uniqueUserIDs), 1, "Should generate multiple different user IDs")
		assert.Greater(t, len(uniqueEventTypes), 1, "Should generate multiple different event types")
		assert.Greater(t, len(uniqueSessionIDs), 10, "Should generate many different session IDs")
	})
}

// TestEventProcessorProcessBatch verifies the EventProcessor.ProcessBatch method functionality.
//
// This test validates the batch processing logic by:
// - Testing proper event counting and storage
// - Verifying thread safety with concurrent access
// - Testing batch processing with various batch sizes
// - Ensuring processing time simulation works correctly
// - Validating atomic counter operations
//
// The test covers both single-threaded and multi-threaded scenarios to ensure
// the processor can handle concurrent batch processing safely.
func TestEventProcessorProcessBatch(t *testing.T) {
	t.Run("SingleBatchProcessing", func(t *testing.T) {
		processor := &EventProcessor{}

		// Create test events
		events := []*UserEvent{
			{UserID: "user1", EventType: "test1", Timestamp: time.Now()},
			{UserID: "user2", EventType: "test2", Timestamp: time.Now()},
			{UserID: "user3", EventType: "test3", Timestamp: time.Now()},
		}

		// Process the batch
		processor.ProcessBatch(events)

		// Verify processing results
		assert.Equal(t, int64(3), atomic.LoadInt64(&processor.processedCount), "Should process 3 events")

		processor.mu.Lock()
		processedEvents := processor.processedEvents
		processor.mu.Unlock()

		require.Len(t, processedEvents, 3, "Should store 3 processed events")
		assert.Equal(t, events[0], processedEvents[0], "First event should match")
		assert.Equal(t, events[1], processedEvents[1], "Second event should match")
		assert.Equal(t, events[2], processedEvents[2], "Third event should match")
	})

	t.Run("MultipleBatchProcessing", func(t *testing.T) {
		processor := &EventProcessor{}

		// Process first batch
		batch1 := []*UserEvent{
			{UserID: "user1", EventType: "batch1_event1", Timestamp: time.Now()},
			{UserID: "user2", EventType: "batch1_event2", Timestamp: time.Now()},
		}
		processor.ProcessBatch(batch1)

		// Process second batch
		batch2 := []*UserEvent{
			{UserID: "user3", EventType: "batch2_event1", Timestamp: time.Now()},
			{UserID: "user4", EventType: "batch2_event2", Timestamp: time.Now()},
			{UserID: "user5", EventType: "batch2_event3", Timestamp: time.Now()},
		}
		processor.ProcessBatch(batch2)

		// Verify cumulative results
		assert.Equal(t, int64(5), atomic.LoadInt64(&processor.processedCount), "Should process 5 total events")

		processor.mu.Lock()
		processedEvents := processor.processedEvents
		processor.mu.Unlock()

		require.Len(t, processedEvents, 5, "Should store 5 total processed events")

		// Verify events are stored in order
		assert.Equal(t, "batch1_event1", processedEvents[0].EventType)
		assert.Equal(t, "batch1_event2", processedEvents[1].EventType)
		assert.Equal(t, "batch2_event1", processedEvents[2].EventType)
		assert.Equal(t, "batch2_event2", processedEvents[3].EventType)
		assert.Equal(t, "batch2_event3", processedEvents[4].EventType)
	})

	t.Run("EmptyBatchProcessing", func(t *testing.T) {
		processor := &EventProcessor{}

		// Process empty batch
		processor.ProcessBatch([]*UserEvent{})

		// Verify no processing occurred
		assert.Equal(t, int64(0), atomic.LoadInt64(&processor.processedCount), "Should process 0 events")

		processor.mu.Lock()
		processedEvents := processor.processedEvents
		processor.mu.Unlock()

		assert.Empty(t, processedEvents, "Should store 0 processed events")
	})

	t.Run("ConcurrentBatchProcessing", func(t *testing.T) {
		processor := &EventProcessor{}

		// Number of concurrent goroutines and events per goroutine
		numGoroutines := 10
		eventsPerGoroutine := 5
		totalExpectedEvents := numGoroutines * eventsPerGoroutine

		var wg sync.WaitGroup

		// Start concurrent batch processing
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				// Create batch for this goroutine
				batch := make([]*UserEvent, eventsPerGoroutine)
				for j := 0; j < eventsPerGoroutine; j++ {
					batch[j] = &UserEvent{
						UserID:    "user" + strconv.Itoa(goroutineID*eventsPerGoroutine+j),
						EventType: "concurrent_test",
						Timestamp: time.Now(),
					}
				}

				// Process the batch
				processor.ProcessBatch(batch)
			}(i)
		}

		// Wait for all goroutines to complete
		wg.Wait()

		// Verify final results
		finalCount := atomic.LoadInt64(&processor.processedCount)
		assert.Equal(t, int64(totalExpectedEvents), finalCount,
			"Should process %d total events concurrently", totalExpectedEvents)

		processor.mu.Lock()
		processedEvents := processor.processedEvents
		processor.mu.Unlock()

		assert.Len(t, processedEvents, totalExpectedEvents,
			"Should store %d total processed events", totalExpectedEvents)
	})

	t.Run("ProcessingTimeSimulation", func(t *testing.T) {
		processor := &EventProcessor{}

		// Create a batch that should take at least some processing time
		batchSize := 5
		events := make([]*UserEvent, batchSize)
		for i := 0; i < batchSize; i++ {
			events[i] = &UserEvent{
				UserID:    "timing_user",
				EventType: "timing_test",
				Timestamp: time.Now(),
			}
		}

		// Measure processing time
		start := time.Now()
		processor.ProcessBatch(events)
		elapsed := time.Since(start)

		// The implementation sleeps for len(events)*10 milliseconds
		expectedMinTime := time.Duration(batchSize*10) * time.Millisecond
		assert.GreaterOrEqual(t, elapsed, expectedMinTime,
			"Processing should take at least %v for %d events", expectedMinTime, batchSize)

		// Verify processing completed
		assert.Equal(t, int64(batchSize), atomic.LoadInt64(&processor.processedCount),
			"Should process %d events", batchSize)
	})

	t.Run("NilEventsHandling", func(t *testing.T) {
		processor := &EventProcessor{}

		// Create batch with nil events (edge case)
		batch := []*UserEvent{
			{UserID: "user1", EventType: "test", Timestamp: time.Now()},
			nil,
			{UserID: "user2", EventType: "test", Timestamp: time.Now()},
		}

		// This should not panic and should process the batch
		require.NotPanics(t, func() {
			processor.ProcessBatch(batch)
		}, "Processing batch with nil events should not panic")

		// Verify processing occurred (the implementation counts slice length, not non-nil items)
		assert.Equal(t, int64(3), atomic.LoadInt64(&processor.processedCount), "Should count 3 items including nil")
	})
}

// TestEventProcessorThreadSafety verifies that EventProcessor is thread-safe.
//
// This test validates concurrent access patterns by:
// - Running multiple goroutines that process batches simultaneously
// - Verifying that the final count matches expected total
// - Ensuring no data races occur during concurrent processing
// - Testing that the mutex properly protects shared state
//
// This test is designed to catch race conditions and ensure the processor
// can handle high-concurrency scenarios safely.
func TestEventProcessorThreadSafety(t *testing.T) { //nolint:gocognit // Complex concurrent test with multiple verification steps
	processor := &EventProcessor{}

	numWorkers := 20
	batchesPerWorker := 10
	eventsPerBatch := 3
	totalExpectedEvents := numWorkers * batchesPerWorker * eventsPerBatch

	var wg sync.WaitGroup

	// Start multiple workers processing batches concurrently
	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for batch := 0; batch < batchesPerWorker; batch++ {
				// Create unique events for this worker and batch
				events := make([]*UserEvent, eventsPerBatch)
				for i := 0; i < eventsPerBatch; i++ {
					events[i] = &UserEvent{
						UserID:    "worker" + strconv.Itoa(workerID) + "_event" + strconv.Itoa(i),
						EventType: "thread_safety_test",
						Timestamp: time.Now(),
						Metadata: map[string]string{
							"worker_id": strconv.Itoa(workerID),
							"batch_id":  strconv.Itoa(batch),
						},
					}
				}

				// Process the batch
				processor.ProcessBatch(events)

				// Add small delay to increase chance of race conditions if they exist
				time.Sleep(time.Microsecond)
			}
		}(worker)
	}

	// Wait for all workers to complete
	wg.Wait()

	// Verify final results
	finalCount := atomic.LoadInt64(&processor.processedCount)
	assert.Equal(t, int64(totalExpectedEvents), finalCount,
		"Should process exactly %d events with %d workers", totalExpectedEvents, numWorkers)

	processor.mu.Lock()
	processedEvents := processor.processedEvents
	processor.mu.Unlock()

	assert.Len(t, processedEvents, totalExpectedEvents,
		"Should store exactly %d processed events", totalExpectedEvents)

	// Verify no nil events were stored (data integrity check)
	for i, event := range processedEvents {
		assert.NotNil(t, event, "Event %d should not be nil", i)
		if event != nil {
			assert.NotEmpty(t, event.UserID, "Event %d should have non-empty UserID", i)
			assert.Equal(t, "thread_safety_test", event.EventType, "Event %d should have correct EventType", i)
		}
	}
}
