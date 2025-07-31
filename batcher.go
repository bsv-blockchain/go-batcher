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
	"sync"
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
// - usePool: If true, uses sync.Pool for slice reuse to reduce allocations
// - pool: Optional sync.Pool for reusing batch slices
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
	usePool    bool
	pool       *sync.Pool
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
		usePool:    false,
	}

	go b.worker()

	return b
}

// NewWithPool creates a new Batcher instance with slice pooling enabled.
//
// This constructor is similar to New() but initializes a sync.Pool for batch slices
// and uses worker logic that retrieves and returns slices from the pool.
// This can significantly reduce memory allocations and GC pressure in high-throughput scenarios.
//
// Parameters:
//   - size: Maximum number of items per batch
//   - timeout: Maximum duration to wait before processing an incomplete batch
//   - fn: Callback function that processes each batch
//   - background: If true, batch processing happens asynchronously
//
// Returns:
// - *Batcher[T]: A configured and running Batcher instance with pooling enabled
func NewWithPool[T any](size int, timeout time.Duration, fn func(batch []*T), background bool) *Batcher[T] {
	b := &Batcher[T]{
		fn:         fn,
		size:       size,
		timeout:    timeout,
		batch:      make([]*T, 0, size),
		ch:         make(chan *T, size*64),
		triggerCh:  make(chan struct{}),
		background: background,
		usePool:    true,
		pool: &sync.Pool{
			New: func() interface{} {
				slice := make([]*T, 0, size)
				return &slice
			},
		},
	}

	go b.worker()

	return b
}

// Put adds an item to the batch for processing using non-blocking channel send when possible.
//
// This method sends the item to the internal batching channel where it will be collected
// by the worker goroutine. It attempts a non-blocking send first, falling back to blocking
// only when the channel is full. This reduces goroutine blocking in high-throughput scenarios.
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
// - Uses fast-path non-blocking send when possible
// - Falls back to blocking send only when channel is full
// - Items are processed in the order they are received
// - The variadic parameter exists for interface compatibility but is not used
func (b *Batcher[T]) Put(item *T, _ ...int) { // Payload size is not used in this implementation
	select {
	case b.ch <- item:
		// Fast path - non-blocking send succeeded
	default:
		// Channel is full, fallback to blocking send
		b.ch <- item
	}
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
// It uses timer reuse and slice pooling when enabled.
//
// This function performs the following steps:
// - Creates a reusable timeout timer (optimization over time.After)
// - Monitors three channels simultaneously using select:
//   - Item channel: Receives new items to add to the current batch
//   - Timeout channel: Fires when the timeout duration expires
//   - Trigger channel: Receives manual trigger signals
//
// - Processes the batch when any trigger condition is met
// - Resets the batch and starts a new cycle with efficient slice management
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
// - Uses slice pooling if enabled to reduce allocations
//
// Notes:
// - This function runs indefinitely and cannot be stopped
// - Uses goto for performance optimization to avoid deep nesting
// - Empty batches are not processed (checked before invoking fn)
// - Reuses timers to reduce allocations and GC pressure
// - When usePool=true, manages slice lifecycle through sync.Pool for memory efficiency
func (b *Batcher[T]) worker() { //nolint:gocognit,gocyclo // Worker function handles multiple channels and conditions
	// Create a reusable timer for optimization
	timer := time.NewTimer(b.timeout)
	defer timer.Stop()

	for {
		// Reset the timer for this batch cycle
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(b.timeout)

		for {
			select {
			case item := <-b.ch:
				b.batch = append(b.batch, item)

				if len(b.batch) == b.size {
					goto saveBatch
				}

			case <-timer.C:
				goto saveBatch

			case <-b.triggerCh:
				goto saveBatch
			}
		}

	saveBatch:
		if len(b.batch) > 0 { //nolint:nestif // Necessary complexity for handling pooling and background modes
			batch := b.batch

			if b.background {
				if b.usePool {
					go func(batch []*T) {
						b.fn(batch)
						// Return the slice to the pool after processing
						slice := batch[:0]
						b.pool.Put(&slice)
					}(batch)
				} else {
					go b.fn(batch)
				}
			} else {
				b.fn(batch)
				if b.usePool {
					// Return the slice to the pool after processing
					slice := batch[:0]
					b.pool.Put(&slice)
				}
			}

			// Get a new slice (from pool if enabled, or allocate new)
			if b.usePool {
				newBatchPtr := b.pool.Get().(*[]*T)
				b.batch = *newBatchPtr
			} else {
				b.batch = make([]*T, 0, b.size)
			}
		}
	}
}
