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
// - Zero idle CPU usage: Lazy timer activation only when items are batched
// - Graceful shutdown support with Close() for clean goroutine lifecycle
// - Optional observability hooks (Logger / Metrics / OpenTelemetry Tracer) via WithLogger,
//   WithMetrics, WithTracer, WithName options. All default to no-op when omitted, so users
//   outside teranode pay zero observability cost.
//
// Performance characteristics:
// - Idle state: 0% CPU usage (no timers running when batch is empty)
// - Active state: Full performance with low-latency batching (configurable timeout)
// - Peak performance: Maintains throughput regardless of load
// - Memory efficient: Optional slice pooling to reduce GC pressure
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
package batcher

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// itemEnvelope wraps a queued item together with the OpenTelemetry SpanContext
// captured at PutCtx time. The SpanContext is a 24-byte value type, so the
// envelope adds bounded memory overhead per channel slot. An invalid (zero)
// SpanContext means no span link will be emitted for this item.
type itemEnvelope[T any] struct {
	item *T
	sc   trace.SpanContext
}

// Batcher is a generic batching utility that aggregates items and processes them in groups.
//
// The Batcher collects items of type T and invokes a processing function when either:
// - The batch reaches the configured size limit
// - The timeout duration expires since the last batch was processed
// - A manual trigger is invoked via the Trigger() method
//
// Type parameters:
// - T: The type of items to be batched (can be any type)
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
	sem           chan struct{}
	cfg           *config
}

// New creates a new Batcher instance with the specified configuration.
//
// Parameters:
//   - size: Maximum number of items per batch. When this limit is reached, the batch is immediately processed
//   - timeout: Maximum duration to wait before processing an incomplete batch. Prevents items from waiting indefinitely
//   - fn: Callback function that processes each batch. Receives a slice of pointers to the batched items
//   - background: If true, the fn callback is executed in a separate goroutine (non-blocking)
//   - opts: Optional observability configuration (WithLogger, WithMetrics, WithTracer, WithName).
//     Omitted options default to fully no-op behaviour.
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

// Put adds an item to the batch for processing using a non-blocking channel send when possible.
//
// Use PutCtx instead if you have an active OpenTelemetry span context you want
// linked to the eventual batch span. Put behaves identically to PutCtx with
// context.Background().
//
// Parameters:
// - item: Pointer to the item to be batched. Must not be nil.
// - _: Variadic int parameter for payload size (kept for API compatibility, not used).
func (b *Batcher[T]) Put(item *T, _ ...int) {
	b.enqueue(itemEnvelope[T]{item: item})
}

// PutCtx is like Put but captures the SpanContext from ctx so the eventual
// batch span carries it as a link. If ctx has no active span the call behaves
// identically to Put.
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
func (b *Batcher[T]) Trigger() {
	b.triggerCh <- struct{}{}
}

// Close gracefully shuts down the batcher, allowing pending items to be processed.
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
func (b *Batcher[T]) SetDrainMode(enabled bool) {
	b.drainMode.Store(enabled)
}

// SetMaxConcurrent limits the number of concurrent in-flight background batch
// processing goroutines. When the limit is reached, the worker blocks until a
// slot is available, which naturally applies backpressure through the channel.
//
// A value of 0 (default) means no limit. Must be called before items are added
// to the batcher.
func (b *Batcher[T]) SetMaxConcurrent(n int) {
	if n > 0 {
		b.maxConcurrent = n
		b.sem = make(chan struct{}, n)
	}
}

// worker is the core processing loop that manages batch aggregation and processing.
//
// It maintains a worker-local slice of trace.Link records gathered from items
// enqueued via PutCtx; the slice is passed to the batch span when the batch
// is dispatched. Each dispatch is wrapped in a defer/recover so a panic from
// the user fn is logged, recorded as a metric, and the worker keeps running
// (previously a panic silently killed the goroutine).
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

			// Lazy timer activation: start on first item only.
			if len(b.batch) == 1 {
				timer = time.NewTimer(b.timeout)
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
				// Acquire semaphore slot if max concurrent is set.
				if b.sem != nil {
					semStart := time.Now()
					select {
					case b.sem <- struct{}{}:
						if wait := time.Since(semStart); wait > time.Microsecond {
							b.cfg.metricsBound.BackpressureWait(wait)
						}
					case <-b.done:
						// Shutdown while waiting for semaphore — process synchronously.
						b.dispatchAndRecord(batch, links, reason)
						return
					}
				}

				if b.usePool {
					go func(batch []*T, links []trace.Link, reason string) {
						defer func() {
							if b.sem != nil {
								<-b.sem
							}
						}()
						b.dispatchAndRecord(batch, links, reason)
						slice := batch[:0]
						b.pool.Put(&slice)
					}(batch, links, reason)
				} else {
					go func(batch []*T, links []trace.Link, reason string) {
						defer func() {
							if b.sem != nil {
								<-b.sem
							}
						}()
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
			err := fmt.Errorf("%v", r)
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
