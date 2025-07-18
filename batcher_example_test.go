//nolint:wsl // Example files have different formatting standards
package batcher_test

import (
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-batcher"
)

// ExampleNew demonstrates creating a basic batcher for processing items in groups.
func ExampleNew() {
	// Create a batcher that processes items in batches of 5 or every 100 ms
	b := batcher.New[string](5, 100*time.Millisecond, func(batch []*string) {
		fmt.Printf("Processing batch of %d items\n", len(batch))
		for _, item := range batch {
			fmt.Printf("  - %s\n", *item)
		}
	}, false) // false = synchronous processing

	// Add items to the batcher
	items := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i := range items {
		b.Put(&items[i])
	}

	// Wait for timeout to ensure the batch is processed
	time.Sleep(150 * time.Millisecond)

	// Output:
	// Processing batch of 5 items
	//   - apple
	//   - banana
	//   - cherry
	//   - date
	//   - elderberry
}

// ExampleNew_backgroundProcessing demonstrates using background processing for non-blocking batch handling.
func ExampleNew_backgroundProcessing() {
	// Create a batcher with background processing enabled
	b := batcher.New[int](3, 50*time.Millisecond, func(batch []*int) {
		sum := 0
		for _, item := range batch {
			sum += *item
		}
		fmt.Printf("Batch sum: %d\n", sum)
	}, true) // true = background processing

	// Add items that will trigger immediate batch processing
	nums := []int{1, 2, 3}
	for i := range nums {
		b.Put(&nums[i])
	}

	// Give background goroutine time to process
	time.Sleep(10 * time.Millisecond)

	// Output:
	// Batch sum: 6
}

// ExampleBatcher_Put demonstrates adding items to a batcher for batch processing.
func ExampleBatcher_Put() {
	processed := make(chan int, 1)

	b := batcher.New[string](2, time.Second, func(batch []*string) {
		fmt.Printf("Batch received: %d items\n", len(batch))
		processed <- len(batch)
	}, false)

	// Add two items to trigger batch by size
	item1 := "first"
	item2 := "second"
	b.Put(&item1)
	b.Put(&item2)

	// Wait for processing
	<-processed

	// Output:
	// Batch received: 2 items
}

// ExampleBatcher_Trigger demonstrates manually triggering batch processing.
func ExampleBatcher_Trigger() {
	processed := make(chan struct{})

	b := batcher.New[string](10, time.Hour, func(batch []*string) {
		fmt.Printf("Processing %d items\n", len(batch))
		for _, item := range batch {
			fmt.Printf("  %s\n", *item)
		}
		close(processed)
	}, false)

	// Add items that won't trigger automatic batching
	items := []string{"one", "two", "three"}
	for i := range items {
		b.Put(&items[i])
	}

	// Small delay to ensure items are queued
	time.Sleep(5 * time.Millisecond)

	// Manually trigger processing
	b.Trigger()

	// Wait for processing to complete
	<-processed

	// Output:
	// Processing 3 items
	//   one
	//   two
	//   three
}

// ExampleNewWithDeduplication demonstrates creating a batcher that automatically removes duplicate items.
func ExampleNewWithDeduplication() {
	processed := make(chan struct{})

	b := batcher.NewWithDeduplication[string](5, 100*time.Millisecond, func(batch []*string) {
		fmt.Printf("Unique items in batch: %d\n", len(batch))
		for _, item := range batch {
			fmt.Printf("  - %s\n", *item)
		}
		close(processed)
	}, false)

	// Add items including duplicates
	items := []string{"apple", "banana", "apple", "cherry", "banana"}
	for _, item := range items {
		// Create a new variable for each iteration to get a unique pointer
		itemCopy := item
		b.Put(&itemCopy)
	}

	// Small delay to ensure items are queued
	time.Sleep(5 * time.Millisecond)

	// Trigger to process the batch
	b.Trigger()

	// Wait for processing to complete
	<-processed

	// Output:
	// Unique items in batch: 3
	//   - apple
	//   - banana
	//   - cherry
}

// ExampleBatcherWithDedup_Put demonstrates adding items to a deduplicating batcher.
func ExampleBatcherWithDedup_Put() {
	processed := make(chan int, 1)

	// Use a larger batch size and longer timeout to prevent premature processing
	b := batcher.NewWithDeduplication[int](100, 5*time.Second, func(batch []*int) {
		fmt.Printf("Processing batch with %d unique items\n", len(batch))
		processed <- len(batch)
	}, false)

	// Add duplicate items - the set should have 4 unique values: 1, 2, 3, 4
	items := []int{1, 2, 3, 2, 1, 4}
	for _, item := range items {
		itemCopy := item
		b.Put(&itemCopy)
	}

	// Small delay to ensure all items are processed through deduplication
	time.Sleep(50 * time.Millisecond)

	// Trigger processing
	b.Trigger()

	// Wait for processing
	<-processed

	// Output:
	// Processing batch with 4 unique items
}

// Example_timeoutProcessing demonstrates batch processing triggered by timeout.
func Example_timeoutProcessing() {
	processed := make(chan struct{})

	b := batcher.New[string](10, 50*time.Millisecond, func(batch []*string) {
		fmt.Printf("Batch processed by timeout with %d items\n", len(batch))
		close(processed)
	}, false)

	// Add only 2 items (won't trigger size-based batching)
	item1, item2 := "first", "second"
	b.Put(&item1)
	b.Put(&item2)

	// Wait for timeout to trigger
	<-processed

	// Output:
	// Batch processed by timeout with 2 items
}

// Example_bulkDatabaseWrites demonstrates using a batcher for an optimizing database writes.
func Example_bulkDatabaseWrites() {
	type User struct {
		ID   int
		Name string
	}

	// Simulate a database bulk insert function
	bulkInsert := func(users []*User) {
		fmt.Printf("Bulk inserting %d users into database\n", len(users))
		for _, u := range users {
			fmt.Printf("  User{ID: %d, Name: %s}\n", u.ID, u.Name)
		}
	}

	// Create a batcher for database operations
	b := batcher.New[User](3, 100*time.Millisecond, bulkInsert, true)

	// Simulate incoming user registrations
	users := []User{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	for i := range users {
		b.Put(&users[i])
	}

	// Give time for background processing
	time.Sleep(20 * time.Millisecond)

	// Output:
	// Bulk inserting 3 users into database
	//   User{ID: 1, Name: Alice}
	//   User{ID: 2, Name: Bob}
	//   User{ID: 3, Name: Charlie}
}

// Example_eventAggregation demonstrates using a batcher for aggregating events.
func Example_eventAggregation() {
	type Event struct {
		Type      string
		Timestamp time.Time
	}

	processed := make(chan struct{})

	// Create an event aggregator
	b := batcher.NewWithDeduplication[Event](5, 50*time.Millisecond, func(batch []*Event) {
		fmt.Printf("Processing %d unique events\n", len(batch))
		// Group events by type
		eventTypes := make(map[string]int)
		for _, e := range batch {
			eventTypes[e.Type]++
		}
		// Sort types for consistent output
		types := []string{"click", "scroll", "view"}
		for _, eventType := range types {
			if count, ok := eventTypes[eventType]; ok {
				fmt.Printf("  %s: %d occurrences\n", eventType, count)
			}
		}
		close(processed)
	}, false)

	// Generate events (including duplicates)
	now := time.Now()
	events := []Event{
		{Type: "click", Timestamp: now},
		{Type: "view", Timestamp: now},
		{Type: "click", Timestamp: now}, // duplicate
		{Type: "scroll", Timestamp: now},
		{Type: "view", Timestamp: now.Add(time.Second)}, // different timestamp
	}

	for i := range events {
		b.Put(&events[i])
	}

	// Small delay to ensure items are queued
	time.Sleep(5 * time.Millisecond)

	// Trigger processing
	b.Trigger()
	<-processed

	// Output:
	// Processing 4 unique events
	//   click: 1 occurrences
	//   scroll: 1 occurrences
	//   view: 2 occurrences
}
