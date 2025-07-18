// Package batcher provides high-performance batching functionality for aggregating items before processing.
//
// This package implements an efficient batching mechanism that collects items and processes them in groups,
// either when a specified batch size is reached or when a timeout expires. This approach is particularly
// useful for optimizing I/O operations, reducing API calls, or aggregating events for bulk processing.
//
// Key features include:
// - Generic type support allowing batching of any data type
// - Configurable batch size and timeout for flexible processing strategies
// - Background processing option to avoid blocking the caller
// - Manual trigger capability for immediate batch processing
// - Thread-safe concurrent operations
//
// The package is structured to provide two main components:
// - Basic Batcher: Simple batching with size and timeout-based triggers
// - BatcherWithDedup: Extended functionality with built-in deduplication using time-partitioned maps
//
// Usage examples:
// Basic batching for database writes:
//
//	batcher := New[User](100, 5*time.Second, func(batch []*User) {
//	    db.BulkInsert(batch)
//	}, true)
//	batcher.Put(&User{Name: "John"})
//
// Important notes:
// - The batcher runs a background goroutine that continues indefinitely
// - Items are passed by pointer to avoid unnecessary copying
// - The processing function is called synchronously or asynchronously based on the background flag
// - Batches are processed when size is reached, timeout expires, or Trigger() is called
//
// This package is part of the go-batcher library and provides efficient batch processing
// capabilities for high-throughput applications.
package batcher

import (
	"time"
)

// Batcher is a generic batching utility that aggregates items and processes them in groups.
//
// The Batcher collects items of type T and invokes a processing function when either:
// - The batch reaches the configured size limit
// - The timeout duration expires since the last batch was processed
// - A manual trigger is invoked via the Trigger() method
//
// Type parameters:
// - T: The type of items to be batched (can be any type)
//
// Fields:
// - fn: The callback function that processes completed batches
// - size: Maximum number of items in a batch before automatic processing
// - timeout: Maximum duration to wait before processing an incomplete batch
// - batch: Internal slice holding the current batch of items
// - ch: Buffered channel for receiving items to batch
// - triggerCh: Channel for manual batch processing triggers
// - background: If true, batch processing happens in a separate goroutine
//
// Notes:
// - The Batcher is thread-safe and can be used concurrently
// - Items are passed by pointer to avoid copying
// - The internal worker goroutine runs indefinitely
type Batcher[T any] struct {
	fn         func([]*T)
	size       int
	timeout    time.Duration
	batch      []*T
	ch         chan *T
	triggerCh  chan struct{}
	background bool
}

// New creates a new Batcher instance with the specified configuration.
//
// This function initializes a Batcher that collects items and processes them in batches
// according to the configured size and timeout parameters. The Batcher starts a background
// worker goroutine that continuously monitors for items to batch.
//
// Parameters:
//   - size: Maximum number of items per batch. When this limit is reached, the batch is immediately processed
//   - timeout: Maximum duration to wait before processing an incomplete batch. Prevents items from waiting indefinitely
//   - fn: Callback function that processes each batch. Receives a slice of pointers to the batched items
//   - background: If true, the fn callback is executed in a separate goroutine (non-blocking)
//     If false, the fn callback blocks the worker until completion
//
// Returns:
// - *Batcher[T]: A configured and running Batcher instance ready to accept items
//
// Side Effects:
// - Starts a background worker goroutine that runs indefinitely
// - Creates internal channels for item processing and manual triggers
//
// Notes:
// - The internal channel buffer is sized at 64x the batch size for performance
// - The batch slice is pre-allocated with the specified size for efficiency
// - The worker goroutine cannot be stopped once started (runs indefinitely)
// - Passing background=true is recommended for I/O-bound operations to avoid blocking
func New[T any](size int, timeout time.Duration, fn func(batch []*T), background bool) *Batcher[T] {
	b := &Batcher[T]{
		fn:         fn,
		size:       size,
		timeout:    timeout,
		batch:      make([]*T, 0, size),
		ch:         make(chan *T, size*64),
		triggerCh:  make(chan struct{}),
		background: background,
	}

	go b.worker()

	return b
}

// Put adds an item to the batch for processing.
//
// This method sends the item to the internal batching channel where it will be collected
// by the worker goroutine. The item will be included in the next batch that is processed,
// which occurs when the batch size is reached; the timeout expires, or Trigger() is called.
//
// Parameters:
// - item: Pointer to the item to be batched. Must not be nil
// - _: Variadic int parameter for payload size (ignored in this implementation, kept for API compatibility)
//
// Returns:
// - Nothing
//
// Side Effects:
// - Sends the item through the internal channel to the worker goroutine
// - May trigger batch processing if this item completes a full batch
//
// Notes:
// - This method will block if the internal channel buffer is full (64x batch size)
// - Items are processed in the order they are received
// - The variadic parameter exists for interface compatibility but is not used
func (b *Batcher[T]) Put(item *T, _ ...int) { // Payload size is not used in this implementation
	b.ch <- item
}

// Trigger forces immediate processing of the current batch.
//
// This method sends a signal to the worker goroutine to process whatever items are
// currently in the batch, regardless of size or timeout constraints. This is useful
// for ensuring all pending items are processed before shutdown or when you need
// immediate processing for application-specific reasons.
//
// Parameters:
// - None
//
// Returns:
// - Nothing
//
// Side Effects:
// - Causes the worker goroutine to immediately process the current batch
// - Resets the timeout timer after processing
//
// Notes:
// - If the batch is empty, the trigger signal is still sent but no processing occurs
// - This method is non-blocking and returns immediately
// - Multiple rapid triggers are safe but may result in processing empty or small batches
func (b *Batcher[T]) Trigger() {
	b.triggerCh <- struct{}{}
}

// worker is the core processing loop that manages batch aggregation and processing.
//
// This function runs as a background goroutine and continuously monitors three conditions
// for batch processing: size limit reached, timeout expired, or manual trigger received.
// It uses a nested loop structure with goto statements for efficient batch processing.
//
// This function performs the following steps:
// - Creates a new timeout timer for each batch cycle
// - Monitors three channels simultaneously using select:
//   - Item channel: Receives new items to add to the current batch
//   - Timeout channel: Fires when the timeout duration expires
//   - Trigger channel: Receives manual trigger signals
//
// - Processes the batch when any trigger condition is met
// - Resets the batch and starts a new cycle
//
// Parameters:
// - None (operates on Batcher receiver fields)
//
// Returns:
// - Nothing (runs indefinitely)
//
// Side Effects:
// - Consumes items from the internal channels
// - Invokes the batch processing function (fn) with completed batches
// - May spawn new goroutines if background processing is enabled
//
// Notes:
// - This function runs indefinitely and cannot be stopped
// - Uses goto for performance optimization to avoid deep nesting
// - Empty batches are not processed (checked before invoking fn)
// - The batch slice is reallocated after each processing to avoid memory issues
// - When background=true, batch processing is non-blocking and concurrent
func (b *Batcher[T]) worker() { //nolint:gocognit // Worker function handles multiple channels and conditions
	for {
		expire := time.After(b.timeout)

		for {
			select {
			case item := <-b.ch:
				b.batch = append(b.batch, item)

				if len(b.batch) == b.size {
					goto saveBatch
				}

			case <-expire:
				goto saveBatch

			case <-b.triggerCh:
				goto saveBatch
			}
		}

	saveBatch:
		if len(b.batch) > 0 {
			batch := b.batch

			if b.background {
				go b.fn(batch)
			} else {
				b.fn(batch)
			}

			b.batch = make([]*T, 0, b.size)
		}
	}
}
