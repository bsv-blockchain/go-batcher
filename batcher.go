// Package batcher provides high-performance batching functionality for aggregating items before processing.
//
// This package implements an efficient batching mechanism that collects items and processes them in groups,
// either when a specified batch size is reached or when a timeout expires. This approach is particularly
// useful for optimizing I/O operations, reducing API calls, or aggregating events for bulk processing.
//
// Key features include:
//   - Generic type support allowing batching of any data type
//   - Configurable batch size and timeout for flexible processing strategies
//   - Background processing option to avoid blocking the caller
//   - Manual trigger capability for immediate batch processing
//   - Thread-safe concurrent operations
//   - Zero idle CPU usage: Lazy timer activation only when items are batched
//   - Graceful shutdown support with Close() for clean goroutine lifecycle
//   - Optional observability hooks (Logger / Metrics / OpenTelemetry Tracer) via WithLogger,
//     WithMetrics, WithTracer, WithName options. All default to no-op when omitted, so users
//     outside teranode pay zero observability cost.
//
// Performance characteristics:
//   - Idle state: 0% CPU usage (no timers running when batch is empty)
//   - Active state: Full performance with low-latency batching (configurable timeout)
//   - Peak performance: Maintains throughput regardless of load
//   - Memory efficient: Optional slice pooling to reduce GC pressure
//
// The package is structured to provide two main components:
//   - Basic Batcher: Simple batching with size and timeout-based triggers
//   - BatcherWithDedup: Extended functionality with built-in deduplication using time-partitioned maps
//
// Usage examples:
// Basic batching for database writes:
//
//	batcher := New[User](100, 5*time.Second, func(batch []*User) {
//	    db.BulkInsert(batch)
//	}, true)
//	batcher.Put(&User{Name: "John"})
//	defer batcher.Close() // Graceful shutdown
//
// With observability wired in:
//
//	m := batcher.NewPrometheusMetrics(reg, "myservice", "batcher")
//	b := batcher.New(100, 5*time.Second, fn, true,
//	    batcher.WithName("user_writes"),
//	    batcher.WithLogger(logger),
//	    batcher.WithMetrics(m),
//	    batcher.WithTracer(otel.Tracer("batcher")),
//	)
//	b.PutCtx(ctx, &User{Name: "John"}) // span context becomes a link on the batch span
//
// Important notes:
//   - The batcher runs a background goroutine managed via context
//   - Items are passed by pointer to avoid unnecessary copying
//   - The processing function is called synchronously or asynchronously based on the background flag
//   - Batches are processed when size is reached, timeout expires, or Trigger() is called
//   - Call Close() for graceful shutdown to process remaining items and prevent goroutine leaks
//
// This package is part of the go-batcher library and provides efficient batch processing
// capabilities for high-throughput applications with minimal resource consumption.
package batcher

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// errBatchPanic is the sentinel error wrapped around a recovered panic value
// when reporting it through the OpenTelemetry span and (eventually) elsewhere.
var errBatchPanic = errors.New("batcher: panic in batch fn")

// itemEnvelope wraps a queued item together with the OpenTelemetry SpanContext
// captured at PutCtx time. The SpanContext is a 24-byte value type, so the
// envelope adds bounded memory overhead per channel slot. An invalid (zero)
// SpanContext means no span link will be emitted for this item.
type itemEnvelope[T any] struct {
	item *T
	sc   trace.SpanContext
}

// workItem is the payload handed from the worker goroutine to the persistent
// dispatch pool created by SetMaxConcurrent. Bundling the trace links and
// reason avoids per-batch closures and keeps observability intact for the
// pool path.
type workItem[T any] struct {
	batch  []*T
	links  []trace.Link
	reason string
}

// Batcher is a generic batching utility that aggregates items and processes them in groups.
//
// The Batcher collects items of type T and invokes a processing function when either:
//   - The batch reaches the configured size limit
//   - The timeout duration expires since the last batch was processed
//   - A manual trigger is invoked via the Trigger() method
//
// Type parameters:
//   - T: The type of items to be batched (can be any type)
//
// Fields:
//   - fn: The callback function that processes completed batches
//   - size: Maximum number of items in a batch before automatic processing
//   - timeout: Maximum duration to wait before processing an incomplete batch
//   - batch: Internal slice holding the current batch of items
//   - ch: Buffered channel of itemEnvelope values, carrying each item plus an optional SpanContext for tracing
//   - triggerCh: Channel for manual batch processing triggers
//   - background: If true, batch processing happens in a separate goroutine
//   - usePool: If true, uses sync.Pool for slice reuse to reduce allocations
//   - pool: Optional sync.Pool for reusing batch slices
//   - cfg: Resolved observability configuration (logger, bound metrics, tracer, name)
//
// Notes:
//   - The Batcher is thread-safe and can be used concurrently
//   - Items are passed by pointer to avoid copying
//   - The internal worker goroutine runs indefinitely until Close() is called
type Batcher[T any] struct {
	fn            func([]*T)
	size          int
	timeout       time.Duration
	batch         []*T
	ch            chan itemEnvelope[T]
	triggerCh     chan struct{}
	background    bool
	usePool       bool
	pool          *sync.Pool
	done          chan struct{}
	drainMode     atomic.Bool
	maxConcurrent int
	cfg           *config
	// workCh is non-nil iff SetMaxConcurrent(n>0) was called. When non-nil,
	// the worker dispatches batches to a fixed pool of N persistent goroutines
	// over an unbuffered (rendezvous) channel — sender blocks until a worker
	// is ready, providing the same backpressure as the previous semaphore.
	// The worker goroutine is the sole sender; close happens after final-batch
	// flush in the shutdown branch.
	workCh chan workItem[T]
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
//   - opts: Optional observability configuration (WithLogger, WithMetrics, WithTracer, WithName).
//     Omitted options default to fully no-op behavior.
//
// Returns:
//   - *Batcher[T]: A configured and running Batcher instance ready to accept items
//
// Side Effects:
//   - Starts a background worker goroutine that runs until Close() is called
//   - Creates internal channels for item processing and manual triggers
//
// Notes:
//   - The internal channel buffer is sized at 64x the batch size for performance
//   - The batch slice is pre-allocated with the specified size for efficiency
//   - Passing background=true is recommended for I/O-bound operations to avoid blocking
func New[T any](size int, timeout time.Duration, fn func(batch []*T), background bool, opts ...Option) *Batcher[T] {
	b := &Batcher[T]{
		fn:         fn,
		size:       size,
		timeout:    timeout,
		batch:      make([]*T, 0, size),
		ch:         make(chan itemEnvelope[T], size*64),
		triggerCh:  make(chan struct{}),
		background: background,
		usePool:    false,
		done:       make(chan struct{}),
		cfg:        applyOptions(opts),
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
//   - opts: Optional observability configuration (see New)
//
// Returns:
//   - *Batcher[T]: A configured and running Batcher instance with pooling enabled
func NewWithPool[T any](size int, timeout time.Duration, fn func(batch []*T), background bool, opts ...Option) *Batcher[T] {
	b := &Batcher[T]{
		fn:         fn,
		size:       size,
		timeout:    timeout,
		batch:      make([]*T, 0, size),
		ch:         make(chan itemEnvelope[T], size*64),
		triggerCh:  make(chan struct{}),
		background: background,
		usePool:    true,
		pool: &sync.Pool{
			New: func() interface{} {
				slice := make([]*T, 0, size)
				return &slice
			},
		},
		done: make(chan struct{}),
		cfg:  applyOptions(opts),
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
// Use PutCtx instead if you have an active OpenTelemetry span context you want
// linked to the eventual batch span. Put behaves identically to PutCtx with
// context.Background().
//
// Parameters:
//   - item: Pointer to the item to be batched. Must not be nil
//   - _: Variadic int parameter for payload size (ignored in this implementation, kept for API compatibility)
//
// Returns:
//   - Nothing
//
// Side Effects:
//   - Sends the item through the internal channel to the worker goroutine
//   - May trigger batch processing if this item completes a full batch
//
// Notes:
//   - Uses fast-path non-blocking send when possible
//   - Falls back to blocking send only when channel is full
//   - Items are processed in the order they are received
//   - The variadic parameter exists for interface compatibility but is not used
func (b *Batcher[T]) Put(item *T, _ ...int) {
	b.enqueue(itemEnvelope[T]{item: item})
}

// PutCtx is like Put but captures the SpanContext from ctx so the eventual
// batch span carries it as a link. If ctx has no active span the call behaves
// identically to Put.
//
// Parameters:
//   - ctx: Context whose span (if any) is recorded as a link on the batch span
//   - item: Pointer to the item to be batched. Must not be nil
//   - _: Variadic int parameter for payload size (kept for API compatibility, not used)
func (b *Batcher[T]) PutCtx(ctx context.Context, item *T, _ ...int) {
	b.enqueue(itemEnvelope[T]{
		item: item,
		sc:   trace.SpanContextFromContext(ctx),
	})
}

// enqueue is the shared hot path for Put and PutCtx. It tries a non-blocking
// channel send first and only times the blocking fallback when the buffer is
// full so the common case is allocation- and timer-free.
func (b *Batcher[T]) enqueue(env itemEnvelope[T]) {
	b.cfg.metricsBound.Enqueued()
	select {
	case b.ch <- env:
		return
	default:
	}
	start := time.Now()
	b.ch <- env
	b.cfg.metricsBound.EnqueueBlocked(time.Since(start))
}

// Trigger forces immediate processing of the current batch.
//
// This method sends a signal to the worker goroutine to process whatever items are
// currently in the batch, regardless of size or timeout constraints. This is useful
// for ensuring all pending items are processed before shutdown or when you need
// immediate processing for application-specific reasons.
//
// Parameters:
//   - None
//
// Returns:
//   - Nothing
//
// Side Effects:
//   - Causes the worker goroutine to immediately process the current batch
//   - Resets the timeout timer after processing
//
// Notes:
//   - If the batch is empty, the trigger signal is still sent but no processing occurs
//   - Multiple rapid triggers are safe but may result in processing empty or small batches
func (b *Batcher[T]) Trigger() {
	b.triggerCh <- struct{}{}
}

// Close gracefully shuts down the batcher, allowing pending items to be processed.
//
// This method signals the worker goroutine to stop accepting new items and process
// any remaining items in the queue before exiting. It provides a clean shutdown
// mechanism that prevents goroutine leaks and ensures all queued items are flushed.
//
// Parameters:
//   - None
//
// Returns:
//   - Nothing
//
// Side Effects:
//   - Cancels the internal done channel, signaling the worker to begin shutdown
//   - The worker will process all items currently in the channel
//   - The worker will flush any partial batch before exiting
//   - The internal channel is closed after draining, preventing further Put() calls
//
// Notes:
//   - This method returns immediately without waiting for shutdown to complete
//
// IMPORTANT: Do not call Put() / PutCtx() after Close() has been called. The
// channel is closed during shutdown, and any further send will panic with
// "send on closed channel". Callers must ensure proper synchronization.
func (b *Batcher[T]) Close() {
	close(b.done)
}

// SetDrainMode enables or disables drain mode.
//
// When drain mode is enabled, the worker drains all currently-available items
// from the channel (up to the size cap) and fires immediately — instead of
// accumulating to the size threshold or waiting for the timeout.
//
// This produces adaptive batch sizes that naturally scale with throughput:
// at low throughput, batches are small (even single-item) with near-zero latency;
// at high throughput, batches grow larger as more items queue during processing.
func (b *Batcher[T]) SetDrainMode(enabled bool) {
	b.drainMode.Store(enabled)
}

// SetMaxConcurrent limits the number of concurrent in-flight background batch
// processing goroutines. When n>0 the batcher launches N persistent worker
// goroutines that consume batches from an unbuffered channel; the worker
// blocks on dispatch until one is ready, providing natural backpressure.
//
// This prevents unbounded goroutine accumulation when the batch processing
// function (e.g., a gRPC call) is slower than the incoming item rate, and
// avoids spawning a fresh goroutine per batch on the hot path.
//
// A value of 0 (default) means no limit — batches dispatch on a fresh
// goroutine each (the original unbounded-concurrency behavior).
// Must be called before items are added to the batcher.
func (b *Batcher[T]) SetMaxConcurrent(n int) {
	if n <= 0 {
		return
	}
	b.maxConcurrent = n
	b.workCh = make(chan workItem[T]) // rendezvous: backpressure equivalent to a size-N semaphore
	b.cfg.logger.Infof("batcher %q: starting persistent worker pool of size %d", b.cfg.name, n)
	for i := 0; i < n; i++ {
		go func() {
			for w := range b.workCh {
				// dispatchAndRecord recovers from panics, so WorkerFinished
				// is reached on every iteration without a defer. Avoiding the
				// defer keeps the hot path closure-free.
				b.cfg.metricsBound.WorkerStarted()
				b.dispatchAndRecord(w.batch, w.links, w.reason)
				if b.usePool {
					slice := w.batch[:0]
					b.pool.Put(&slice)
				}
				b.cfg.metricsBound.WorkerFinished()
			}
		}()
	}
}

// worker is the core processing loop that manages batch aggregation and processing.
//
// This function runs as a background goroutine and continuously monitors three conditions
// for batch processing: size limit reached, timeout expired, or manual trigger received.
// It uses timer reuse and slice pooling when enabled, and maintains a worker-local slice
// of trace.Link records gathered from items enqueued via PutCtx.
//
// This function performs the following steps:
//   - Creates a reusable timeout timer (optimization over time.After)
//   - Monitors three channels simultaneously using select:
//   - Item channel: Receives new items to add to the current batch
//   - Timeout channel: Fires when the timeout duration expires
//   - Trigger channel: Receives manual trigger signals
//   - Processes the batch when any trigger condition is met
//   - Resets the batch and starts a new cycle with efficient slice management
//
// Side Effects:
//   - Consumes items from the internal channels
//   - Invokes the batch processing function (fn) with completed batches
//   - May spawn new goroutines if background processing is enabled
//   - Uses slice pooling if enabled to reduce allocations
//
// Notes:
//   - Uses goto for performance optimization to avoid deep nesting
//   - Empty batches are not processed (checked before invoking fn)
//   - Reuses timers to reduce allocations and GC pressure
//   - When usePool=true, manages slice lifecycle through sync.Pool for memory efficiency
//   - Each batch dispatch is wrapped in defer/recover so a panic from the user fn is
//     logged, recorded as a metric and span error, and the worker keeps running
func (b *Batcher[T]) worker() { //nolint:gocognit,gocyclo // Worker function handles multiple channels and conditions
	var timer *time.Timer
	var timerCh <-chan time.Time // nil channel blocks forever, enabling lazy timer activation
	var batchLinks []trace.Link

	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		var reason string

		select {
		case <-b.done:
			// Shutdown: drain channel and process remaining items.
			close(b.ch)
			for env := range b.ch {
				b.batch = append(b.batch, env.item)
				if env.sc.IsValid() {
					batchLinks = append(batchLinks, trace.Link{SpanContext: env.sc})
				}
			}
			if len(b.batch) > 0 {
				b.cfg.logger.Infof("batcher %q: draining %d items on shutdown", b.cfg.name, len(b.batch))
				b.dispatchAndRecord(b.batch, batchLinks, ReasonShutdown)
			}
			// We are the sole sender to workCh; closing here lets the
			// persistent dispatch goroutines drain any in-flight batch and
			// exit cleanly. No-op when SetMaxConcurrent was not configured.
			if b.workCh != nil {
				close(b.workCh)
			}
			return

		case env := <-b.ch:
			b.batch = append(b.batch, env.item)
			if env.sc.IsValid() {
				batchLinks = append(batchLinks, trace.Link{SpanContext: env.sc})
			}

			if b.drainMode.Load() {
				// Drain all available items up to size cap, then fire immediately.
				for len(b.batch) < b.size {
					select {
					case env := <-b.ch:
						b.batch = append(b.batch, env.item)
						if env.sc.IsValid() {
							batchLinks = append(batchLinks, trace.Link{SpanContext: env.sc})
						}
					default:
						reason = ReasonDrain
						goto saveBatch
					}
				}
				reason = ReasonDrain
				goto saveBatch
			}

			// Lazy timer activation: start on first item only. Reuse the same
			// Timer across batches to avoid a per-batch allocation; safe under
			// Go 1.23+ semantics where Reset on a stopped/expired timer will
			// not deliver a stale value.
			if len(b.batch) == 1 {
				if timer == nil {
					timer = time.NewTimer(b.timeout)
				} else {
					timer.Reset(b.timeout)
				}
				timerCh = timer.C
			}

			if len(b.batch) == b.size {
				if timer != nil {
					timer.Stop()
					timerCh = nil
				}
				reason = ReasonSize
				goto saveBatch
			}

		case <-timerCh: // Only fires when timerCh != nil (batch has items).
			timerCh = nil
			reason = ReasonTimeout
			goto saveBatch

		case <-b.triggerCh:
			if timer != nil {
				timer.Stop()
				timerCh = nil
			}
			reason = ReasonManual
			goto saveBatch
		}

		continue

	saveBatch:
		if len(b.batch) > 0 { //nolint:nestif // Necessary complexity for handling pooling and background modes
			batch := b.batch
			links := batchLinks

			if b.background {
				if b.workCh != nil {
					// Hand the batch to a persistent worker. The unbuffered
					// channel blocks here when all N workers are busy,
					// applying backpressure up through b.ch and Put().
					semStart := time.Now()
					select {
					case b.workCh <- workItem[T]{batch: batch, links: links, reason: reason}:
						if wait := time.Since(semStart); wait > time.Microsecond {
							b.cfg.metricsBound.BackpressureWait(wait)
						}
					case <-b.done:
						// Shutdown while waiting for a worker — process synchronously.
						b.cfg.logger.Warnf("batcher %q: dispatching batch of %d items inline due to shutdown while waiting for worker slot", b.cfg.name, len(batch))
						b.dispatchAndRecord(batch, links, reason)
						if b.usePool {
							slice := batch[:0]
							b.pool.Put(&slice)
						}
						return
					}
				} else if b.usePool {
					// Unbounded concurrency path: spawn a fresh goroutine per batch.
					go func(batch []*T, links []trace.Link, reason string) {
						b.dispatchAndRecord(batch, links, reason)
						slice := batch[:0]
						b.pool.Put(&slice)
					}(batch, links, reason)
				} else {
					go func(batch []*T, links []trace.Link, reason string) {
						b.dispatchAndRecord(batch, links, reason)
					}(batch, links, reason)
				}
			} else {
				b.dispatchAndRecord(batch, links, reason)
				if b.usePool {
					slice := batch[:0]
					b.pool.Put(&slice)
				}
			}

			// Reset for the next batch. We must not reuse the underlying
			// arrays of `batch` / `links` because background dispatch may
			// still be reading them — allocate fresh slices instead. The
			// pool path takes care of `batch` separately.
			if b.usePool {
				newBatchPtr := b.pool.Get().(*[]*T)
				b.batch = *newBatchPtr
			} else {
				b.batch = make([]*T, 0, b.size)
			}
			batchLinks = nil
		}
	}
}

// dispatchAndRecord runs the user fn under a tracer span, records timing and
// trigger metrics, and recovers from panics so the worker survives.
func (b *Batcher[T]) dispatchAndRecord(batch []*T, links []trace.Link, reason string) {
	b.cfg.metricsBound.BatchTriggered(reason)

	startOpts := []trace.SpanStartOption{
		trace.WithAttributes(
			attribute.String("batcher.name", b.cfg.name),
			attribute.Int("batcher.batch_size", len(batch)),
			attribute.String("batcher.reason", reason),
		),
	}
	if len(links) > 0 {
		startOpts = append(startOpts, trace.WithLinks(links...))
	}
	_, span := b.cfg.tracer.Start(context.Background(), b.cfg.name+".flush", startOpts...)

	start := time.Now()
	defer func() {
		dur := time.Since(start)
		if r := recover(); r != nil {
			err := fmt.Errorf("%w: %v", errBatchPanic, r)
			stack := debug.Stack()
			b.cfg.logger.Errorf("batcher %q: panic in batch fn: %v\n%s", b.cfg.name, r, stack)
			b.cfg.metricsBound.PanicRecovered()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		b.cfg.metricsBound.BatchProcessed(len(batch), dur)
		span.End()
	}()

	b.fn(batch)
}
