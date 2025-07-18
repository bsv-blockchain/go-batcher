package batcher

import (
	"sync"
	"time"
)

// PutOptimized adds an item to the batch using a non-blocking channel send when possible.
// This optimized version attempts a non-blocking send first, falling back to blocking
// only when the channel is full. This can reduce goroutine blocking in high-throughput
// scenarios where the channel often has capacity.
func (b *Batcher[T]) PutOptimized(item *T, _ ...int) {
	select {
	case b.ch <- item:
		// Fast path - non-blocking send succeeded
	default:
		// Channel is full, fallback to blocking send
		b.ch <- item
	}
}

// NewOptimized creates a new Batcher instance with optimized worker implementation.
// This constructor is identical to New() but uses workerOptimized() which reuses
// timers to reduce allocations and GC pressure.
func NewOptimized[T any](size int, timeout time.Duration, fn func(batch []*T), background bool) *Batcher[T] {
	b := &Batcher[T]{
		fn:         fn,
		size:       size,
		timeout:    timeout,
		batch:      make([]*T, 0, size),
		ch:         make(chan *T, size*64),
		triggerCh:  make(chan struct{}),
		background: background,
	}
	go b.workerOptimized()
	return b
}

// workerOptimized is an optimized version of the worker loop that reuses timers.
// Instead of creating a new timer for each batch cycle with time.After(), this
// implementation creates a single timer and reuses it with Reset(). This reduces
// allocations and garbage collection overhead.
func (b *Batcher[T]) workerOptimized() { //nolint:gocognit,gocyclo // Worker function handles multiple channels and conditions
	// Create a reusable timer
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

// WithPool extends Batcher with slice pooling for reduced allocations.
// This implementation uses sync.Pool to reuse batch slices instead of allocating
// new ones for each batch. This can significantly reduce memory allocations and
// GC pressure in high-throughput scenarios.
type WithPool[T any] struct {
	fn         func([]*T)
	size       int
	timeout    time.Duration
	batch      []*T
	ch         chan *T
	triggerCh  chan struct{}
	background bool
	pool       *sync.Pool
}

// NewWithPool creates a new WithPool instance that uses sync.Pool for slice reuse.
// This constructor is similar to New() but initializes a sync.Pool for batch slices
// and uses workerWithPool() which retrieves and returns slices from the pool.
func NewWithPool[T any](size int, timeout time.Duration, fn func(batch []*T), background bool) *WithPool[T] {
	b := &WithPool[T]{
		fn:         fn,
		size:       size,
		timeout:    timeout,
		batch:      make([]*T, 0, size),
		ch:         make(chan *T, size*64),
		triggerCh:  make(chan struct{}),
		background: background,
		pool: &sync.Pool{
			New: func() interface{} {
				slice := make([]*T, 0, size)
				return &slice
			},
		},
	}
	go b.workerWithPool()
	return b
}

// Put adds an item to the batch (method for WithPool).
func (b *WithPool[T]) Put(item *T, _ ...int) {
	b.ch <- item
}

// Trigger forces immediate processing of the current batch (method for WithPool).
func (b *WithPool[T]) Trigger() {
	b.triggerCh <- struct{}{}
}

// workerWithPool is the worker loop for WithPool that uses sync.Pool for slice reuse.
// This implementation retrieves batch slices from the pool and returns them after use,
// reducing the number of allocations needed for batch processing.
func (b *WithPool[T]) workerWithPool() { //nolint:gocognit,gocyclo // Worker function handles multiple channels and conditions
	// Create a reusable timer (combining timer optimization with pool usage)
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
		if len(b.batch) > 0 {
			batch := b.batch
			if b.background {
				go func(batch []*T) {
					b.fn(batch)
					// Return the slice to the pool after processing
					slice := batch[:0]
					b.pool.Put(&slice)
				}(batch)
			} else {
				b.fn(batch)
				// Return the slice to the pool after processing
				slice := batch[:0]
				b.pool.Put(&slice)
			}
			// Get a new slice from the pool
			newBatchPtr := b.pool.Get().(*[]*T)
			b.batch = *newBatchPtr
		}
	}
}
