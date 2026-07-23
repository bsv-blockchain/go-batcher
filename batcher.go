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
//
// items is non-nil for envelopes enqueued via PutBatch/PutBatchCtx and holds
// the whole group; item is unused in that case. Exactly one of item/items is
// populated per envelope.
type itemEnvelope[T any] struct {
	item  *T
	items []*T
	sc    trace.SpanContext
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
	fn               func([]*T)
	size             int
	timeout          time.Duration
	batch            []*T
	ch               chan itemEnvelope[T]
	triggerCh        chan struct{}
	background       bool
	usePool          bool
	pool             *sync.Pool
	done             chan struct{}
	drainMode        atomic.Bool
	greedyAccumulate atomic.Bool
	maxConcurrent    int
	cfg              *config
	// workCh is non-nil iff SetMaxConcurrent(n>0) was called. When non-nil,
	// the worker dispatches batches to a fixed pool of N persistent goroutines
	// over an unbuffered (rendezvous) channel — sender blocks until a worker
	// is ready, providing the same backpressure as the previous semaphore.
	// The worker goroutine is the sole sender; close happens after final-batch
	// flush in the shutdown branch.
	workCh chan workItem[T]

	// tickInterval enables fixed-interval ("steady cadence") trigger mode when > 0.
	// Set via SetTickInterval. When wired, the worker fires the current batch every
	// tickInterval regardless of size (empty ticks are skipped) and the lazy first-item
	// timeout is suppressed. Mutually exclusive with drain mode.
	tickInterval time.Duration
	// ticker holds the active *time.Ticker when tick mode is enabled, nil otherwise.
	// Loaded by the worker once per outer loop iteration so callers may install the
	// ticker via SetTickInterval at any time without a data race. Atomic load on the
	// hot path is a sub-nanosecond cost.
	ticker atomic.Pointer[time.Ticker]

	// finished is closed by the worker goroutine when it has fully returned —
	// i.e. after the shutdown drain has dispatched the residual batch AND all
	// background dispatch goroutines have completed (see inflight). Close()
	// blocks on this channel so callers can rely on every queued item having
	// been handed to fn before Close returns.
	finished chan struct{}
	// inflight counts background batch dispatches that run on goroutines other
	// than the worker (the fresh-goroutine paths and the persistent worker
	// pool). The worker waits on it during shutdown so Close does not return
	// while a background fn call is still writing. Add happens on the worker
	// goroutine before the dispatch is handed off; Done happens when fn
	// returns. Synchronous dispatches (non-background, and the shutdown drain
	// itself) run inline on the worker and need no tracking.
	inflight sync.WaitGroup
	// closeOnce guards close(done) so Close is idempotent — a second call must
	// not panic with "close of closed channel".
	closeOnce sync.Once
	// modeMu serializes the mutually-exclusive mode setters (SetDrainMode,
	// SetGreedyAccumulate, SetTickInterval) so their check-then-set on the
	// independent mode atomics is linearizable — concurrent calls can no longer
	// interleave two checks before either store and leave conflicting modes on.
	modeMu sync.Mutex
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
	if size <= 0 {
		panic("batcher: size must be greater than 0")
	}
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
		finished:   make(chan struct{}),
		cfg:        applyOptions(opts),
	}
	if b.cfg.greedyAccumulate {
		b.greedyAccumulate.Store(true)
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
	if size <= 0 {
		panic("batcher: size must be greater than 0")
	}
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
		done:     make(chan struct{}),
		finished: make(chan struct{}),
		cfg:      applyOptions(opts),
	}
	if b.cfg.greedyAccumulate {
		b.greedyAccumulate.Store(true)
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

// PutBatch adds a group of items to the batcher in a single channel send,
// instead of one send per item. This is the multi-item counterpart to Put:
// use it when a caller already has N items ready together (e.g. all inputs
// of one transaction) to cut the per-item channel-lock and collector-select
// overhead down to a single operation for the whole group.
//
// The group is treated as an ordered unit by the collector: if it is smaller
// than the configured batch size it simply accumulates like N individual
// Put calls; if appending it would exceed the size cap, the collector splits
// it into full-size batches (each handed to fn in order) plus at most one
// trailing partial batch that continues accumulating normally. Item order is
// preserved across the split. Note that "in order" refers to dispatch order:
// with background=true (and especially without SetMaxConcurrent) each split
// batch runs fn on its own goroutine, so cross-batch fn completion order is
// not guaranteed — only item order within a batch, and the order in which
// batches are handed off, are.
// Batch size is always honored — a PutBatch call never produces a batch
// larger than the batcher's configured size, even when len(items) > size.
//
// Use PutBatchCtx instead if you have an active OpenTelemetry span context
// you want linked to the eventual batch span(s). Passing a nil or empty
// slice is a no-op.
//
// PutBatch snapshots the slice: the pointers are copied into an internal
// buffer before the call returns, so the caller is free to reuse, pool, or
// overwrite the slice immediately afterward. The pointed-to items themselves
// are shared, exactly as with Put — do not mutate an item after enqueuing it.
//
// A group larger than the batch size is split and dispatched by the single
// worker goroutine as ceil(len(items)/size) batches. That work is done in one
// pass: with background=true it is spread across the dispatch pool, but with
// background=false each split batch runs fn inline on the worker, so a single
// very large PutBatch can keep the worker (and therefore Trigger/timeouts)
// busy for len(items)/size × fn-duration. Prefer sizes that keep any one call
// bounded, or use background dispatch, for very large groups.
//
// Parameters:
//   - items: The group of items to enqueue together. Must not contain nil pointers.
//   - _: Variadic int parameter for payload size (ignored, kept for API compatibility)
func (b *Batcher[T]) PutBatch(items []*T, _ ...int) {
	if len(items) == 0 {
		return
	}
	// Snapshot the caller's slice: the worker reads it later on its own
	// goroutine, so retaining the caller's backing array would race a caller
	// that reuses or pools the slice after this call returns.
	snapshot := make([]*T, len(items))
	copy(snapshot, items)
	b.enqueue(itemEnvelope[T]{items: snapshot})
}

// PutBatchCtx is like PutBatch but captures the SpanContext from ctx so the
// batch span(s) produced from this group carry it as a link. When a group
// larger than the batch size is split into multiple dispatched batches (see
// PutBatch), only the first resulting batch carries the link — see the
// package-level notes on split batches. If ctx has no active span the call
// behaves identically to PutBatch.
func (b *Batcher[T]) PutBatchCtx(ctx context.Context, items []*T, _ ...int) {
	if len(items) == 0 {
		return
	}
	// See PutBatch: snapshot so the caller may reuse the slice after the call.
	snapshot := make([]*T, len(items))
	copy(snapshot, items)
	b.enqueue(itemEnvelope[T]{
		items: snapshot,
		sc:    trace.SpanContextFromContext(ctx),
	})
}

// enqueue is the shared hot path for Put/PutCtx/PutBatch/PutBatchCtx. It
// tries a non-blocking channel send first and only times the blocking
// fallback when the buffer is full so the common case is allocation- and
// timer-free.
func (b *Batcher[T]) enqueue(env itemEnvelope[T]) {
	// enqueued_total counts items submitted, so a PutBatch envelope accounts
	// for its whole group — not one per channel send — to avoid undercounting.
	if n := len(env.items); n > 0 {
		for i := 0; i < n; i++ {
			b.cfg.metricsBound.Enqueued()
		}
	} else {
		b.cfg.metricsBound.Enqueued()
	}
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
//   - This method BLOCKS until shutdown is complete: the worker has drained the
//     channel, dispatched the residual batch through fn, and every background
//     dispatch goroutine has finished. On return, all items accepted before
//     Close have been handed to fn. Bound the wait with your own timeout
//     (e.g. run Close in a goroutine and select on a context) if fn can hang.
//   - Close is idempotent: calling it more than once is safe and will not panic.
//     Concurrent callers all block until shutdown completes.
//
// IMPORTANT: Do not call Put() / PutCtx() / PutBatch() / PutBatchCtx() after
// Close() has been called. The channel is closed during shutdown, and any
// further send will panic with "send on closed channel". Callers must ensure
// proper synchronization.
func (b *Batcher[T]) Close() {
	b.closeOnce.Do(func() {
		close(b.done)
	})
	// Wait for the worker to fully unwind. finished is closed only after the
	// shutdown drain and inflight.Wait() in worker(), so a returned Close
	// guarantees every queued item has been handed to fn. Receiving from a
	// closed channel returns immediately, so repeat/concurrent callers are fine.
	<-b.finished
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
	b.modeMu.Lock()
	defer b.modeMu.Unlock()
	if enabled && b.ticker.Load() != nil {
		b.cfg.logger.Warnf("batcher %q: SetDrainMode(true) rejected — tick mode is enabled", b.cfg.name)
		return
	}
	if enabled && b.greedyAccumulate.Load() {
		b.cfg.logger.Warnf("batcher %q: SetDrainMode(true) rejected — greedy accumulate is enabled", b.cfg.name)
		return
	}
	b.drainMode.Store(enabled)
}

// SetGreedyAccumulate enables or disables greedy accumulate mode.
//
// When enabled, after receiving an item the worker opportunistically drains
// any additional items already waiting on the channel (up to the size cap)
// into the SAME batch before re-entering select — trading a per-item 5-arm
// select for a cheap non-blocking 2-arm receive for every item after the
// first in a burst. Unlike drain mode, greedy accumulate never fires a
// batch early: it only changes how quickly items already queued are pulled
// off the channel between the existing size/timeout/trigger flush points.
// A batch under greedy accumulate is never smaller than the same arrival
// pattern would produce without it.
//
// Mutually exclusive with drain mode (which does fire early): enabling
// either while the other is on logs a warning and is a no-op, matching
// SetDrainMode's existing exclusion with tick mode. Safe to call at
// construction via WithGreedyAccumulate or afterward via this method,
// though changing it once items are already flowing does not retroactively
// affect an in-progress accumulation cycle.
func (b *Batcher[T]) SetGreedyAccumulate(enabled bool) {
	b.modeMu.Lock()
	defer b.modeMu.Unlock()
	if enabled && b.drainMode.Load() {
		b.cfg.logger.Warnf("batcher %q: SetGreedyAccumulate(true) rejected — drain mode is enabled", b.cfg.name)
		return
	}
	b.greedyAccumulate.Store(enabled)
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
				// Pairs with the inflight.Add issued by the worker before it
				// handed this batch over the rendezvous channel.
				b.inflight.Done()
			}
		}()
	}
}

// SetTickInterval enables fixed-interval ("steady cadence") trigger mode.
//
// When d > 0, the worker fires the current batch every d regardless of how
// many items have accumulated since the previous flush. Empty ticks are
// skipped — no fn call, no span, no metric. The size cap still causes an
// early flush, and Trigger() still works.
//
// SetTickInterval is mutually exclusive with drain mode: enabling tick mode
// while drain mode is on logs a warning and is a no-op. Once enabled, tick
// mode supersedes the constructor's timeout argument — the lazy first-item
// timer is no longer activated.
//
// Must be called before items are added to the batcher. Synchronization with
// the worker goroutine is established via the happens-before relationship on
// the first Put (the channel send that follows configuration), matching the
// contract used by SetMaxConcurrent. Passing d <= 0 is a no-op; tick mode
// cannot be disabled once enabled — call Close to tear the batcher down.
func (b *Batcher[T]) SetTickInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	b.modeMu.Lock()
	defer b.modeMu.Unlock()
	if b.drainMode.Load() {
		b.cfg.logger.Warnf("batcher %q: SetTickInterval rejected — drain mode is enabled", b.cfg.name)
		return
	}
	b.tickInterval = d
	b.ticker.Store(time.NewTicker(d))
	b.cfg.logger.Infof("batcher %q: tick mode enabled, interval=%s", b.cfg.name, d)
}

// flushBatch dispatches the current batch (if non-empty) via fn — following
// the background/pool/workCh dispatch rules the Batcher was configured with
// — and resets b.batch (and returns a fresh nil batchLinks) for the next
// accumulation cycle.
//
// This is the batch-dispatch step formerly inlined at the worker's
// goto-target "saveBatch" label. It is factored into a callable method so a
// single b.ch receive carrying multiple items (PutBatch/PutBatchCtx) can
// flush more than once — at each size-cap boundary — without leaving the
// select statement between flushes (see appendItems).
//
// Returns exit=true in the one case that mirrors the original inline
// behavior of returning from worker() directly: shutdown (b.done closing)
// raced ahead of a free slot in the SetMaxConcurrent worker pool. The batch
// is still dispatched synchronously in that case; exit simply tells the
// caller (worker's select-loop body) to stop processing and let worker()
// return, exactly as the original `return` statement did at that call site.
func (b *Batcher[T]) flushBatch(reason string, batchLinks []trace.Link) (newBatchLinks []trace.Link, exit bool) { //nolint:gocognit,gocyclo // Extracted verbatim from worker()'s former saveBatch label; the branching (background/pool/workCh/shutdown-race) is inherent to the dispatch contract, not incidental
	if len(b.batch) == 0 {
		return batchLinks, false
	}

	batch := b.batch
	links := batchLinks

	if b.background { //nolint:nestif // Necessary complexity for handling pooling and background modes
		// Track this dispatch so the shutdown drain (inflight.Wait in worker)
		// blocks until fn has actually run. Add is always on the worker
		// goroutine, so it never races the Wait that also runs there; Done is
		// paired below (persistent worker, fresh goroutine, or the inline
		// shutdown path).
		b.inflight.Add(1)

		if b.workCh != nil {
			// Hand the batch to a persistent worker. The unbuffered channel
			// blocks here when all N workers are busy, applying backpressure
			// up through b.ch and Put()/PutBatch().
			semStart := time.Now()
			select {
			case b.workCh <- workItem[T]{batch: batch, links: links, reason: reason}:
				if wait := time.Since(semStart); wait > time.Microsecond {
					b.cfg.metricsBound.BackpressureWait(wait)
				}
				// persistent worker calls inflight.Done after dispatch
			case <-b.done:
				// Shutdown while waiting for a worker — process synchronously,
				// then signal the caller to stop (mirrors the original inline
				// `return` from worker() at this exact point).
				b.cfg.logger.Warnf("batcher %q: dispatching batch of %d items inline due to shutdown while waiting for worker slot", b.cfg.name, len(batch))
				b.dispatchAndRecord(batch, links, reason)
				if b.usePool {
					slice := batch[:0]
					b.pool.Put(&slice)
				}
				b.inflight.Done()
				// This batch has been dispatched; clear b.batch so the caller
				// (appendItems) can preserve any not-yet-appended remainder of
				// an oversized PutBatch without re-dispatching these items.
				b.batch = nil
				return nil, true
			}
		} else if b.usePool {
			// Unbounded concurrency path: spawn a fresh goroutine per batch.
			go func(batch []*T, links []trace.Link, reason string) {
				defer b.inflight.Done()
				b.dispatchAndRecord(batch, links, reason)
				slice := batch[:0]
				b.pool.Put(&slice)
			}(batch, links, reason)
		} else {
			go func(batch []*T, links []trace.Link, reason string) {
				defer b.inflight.Done()
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

	// Reset for the next batch. We must not reuse the underlying arrays of
	// `batch` / `links` because background dispatch may still be reading
	// them — allocate fresh slices instead. The pool path takes care of
	// `batch` separately.
	if b.usePool {
		newBatchPtr := b.pool.Get().(*[]*T)
		b.batch = *newBatchPtr
	} else {
		b.batch = make([]*T, 0, b.size)
	}

	return nil, false
}

// appendItems appends items to the current batch, splitting into full
// dispatches of exactly b.size whenever a single PutBatch/PutBatchCtx
// envelope contains more items than currently fit. Each full split is
// flushed immediately via flushBatch, so a batch handed to fn is never
// larger than the configured size — even when a caller enqueues an
// over-sized group in one call. At most one trailing partial batch remains
// in b.batch, accumulating normally toward the next size/timeout/trigger
// flush.
//
// batchLinks is expected to already include this envelope's span link (if
// any) before this call: the link is not duplicated across split boundaries,
// so it travels with whichever batch happens to be open when this call
// begins — either the first full split produced here, or the trailing
// partial batch if no split was needed.
//
// flushed reports whether at least one internal flush happened. Callers that
// manage the lazy timeout timer need this: if a flush occurred, any items
// left in b.batch afterward began a brand-new accumulation cycle *during*
// this call (not before it), so the timer must be (re-)armed for them even
// though b.batch was already non-empty when appendItems was called — the
// "was empty on entry" test alone is not sufficient once a mid-call flush
// can happen. See the worker() call site.
//
// reason is the trigger reason recorded on each internal (mid-split) flush, so
// the caller's cycle semantics are preserved: the normal accumulation and
// greedy paths pass ReasonSize (these splits fire because the batch reached the
// cap), while the drain and trigger paths pass ReasonDrain / ReasonManual so a
// PutBatch group split during those cycles is labeled by the cycle that drove
// it rather than always as "size".
//
// If flushBatch reports exit=true (shutdown raced a full worker pool), this
// stops immediately and propagates it — mirroring the original single-item
// saveBatch behavior of returning from worker() at that point.
func (b *Batcher[T]) appendItems(items []*T, batchLinks []trace.Link, reason string) (newBatchLinks []trace.Link, flushed, exit bool) { //nolint:gocognit // Split-at-cap loop with an early-exit propagation path; each branch is a single guarded step
	for len(items) > 0 {
		room := b.size - len(b.batch)
		if room <= 0 {
			// Defensive: b.batch should never already be at/over size here
			// (the loop below always flushes the instant it reaches size),
			// but guard against stalling if that invariant is ever violated.
			batchLinks, exit = b.flushBatch(reason, batchLinks)
			flushed = true
			if exit {
				// flushBatch cleared b.batch on the shutdown-race path; keep the
				// not-yet-appended remainder so the shutdown drain still delivers
				// it instead of silently dropping it.
				b.batch = append(b.batch, items...)
				return batchLinks, flushed, true
			}
			room = b.size
		}

		n := len(items)
		if n > room {
			n = room
		}

		b.batch = append(b.batch, items[:n]...)
		items = items[n:]

		if len(b.batch) == b.size {
			batchLinks, exit = b.flushBatch(reason, batchLinks)
			flushed = true
			if exit {
				b.batch = append(b.batch, items...)
				return batchLinks, flushed, true
			}
			// Interruption point: once shutdown has begun, stop splitting a
			// large group synchronously here and hand the remainder to the
			// shutdown drain, so a single oversized PutBatch cannot keep the
			// worker from acknowledging Close for len(items)/size flushes.
			select {
			case <-b.done:
				b.batch = append(b.batch, items...)
				return batchLinks, flushed, true
			default:
			}
		}
	}

	return batchLinks, flushed, false
}

// drainOnShutdown is the single terminal path taken whenever the worker stops.
// It is called with b.done already closed, so no further live dispatching can
// happen. It closes b.ch, drains any envelopes still buffered there into
// b.batch, then dispatches everything held — the residual partial batch plus
// the drained remainder — synchronously and in size-capped chunks so a queued
// PutBatch envelope (or any accumulated batch) never hands fn more than the
// configured size on the shutdown path either. Finally it closes the
// persistent worker pool so its goroutines exit.
//
// It must be called exactly once, immediately before worker() returns, on every
// exit path — including the flushBatch/appendItems exit=true (shutdown-race)
// paths, which previously returned directly and leaked the pool goroutines by
// never closing workCh.
func (b *Batcher[T]) drainOnShutdown(batchLinks []trace.Link) { //nolint:gocognit // Terminal drain: close, drain, size-capped dispatch, pool close — each a single guarded step on the one shutdown path
	close(b.ch)
	for env := range b.ch {
		if env.items != nil {
			b.batch = append(b.batch, env.items...)
		} else {
			b.batch = append(b.batch, env.item)
		}
		if env.sc.IsValid() {
			batchLinks = append(batchLinks, trace.Link{SpanContext: env.sc})
		}
	}

	if len(b.batch) > 0 {
		b.cfg.logger.Infof("batcher %q: draining %d items on shutdown", b.cfg.name, len(b.batch))
	}
	// Dispatch synchronously in chunks of at most b.size. Because each dispatch
	// is synchronous, fn has returned before we advance b.batch, so reslicing
	// the backing array is safe and no inflight tracking is needed.
	for len(b.batch) > 0 {
		n := b.size
		if n > len(b.batch) {
			n = len(b.batch)
		}
		b.dispatchAndRecord(b.batch[:n:n], batchLinks, ReasonShutdown)
		b.batch = b.batch[n:]
		batchLinks = nil // the span link travels with the first chunk only
	}

	// We are the sole sender to workCh; closing here lets the persistent
	// dispatch goroutines drain any in-flight batch and exit cleanly. No-op
	// when SetMaxConcurrent was not configured.
	if b.workCh != nil {
		close(b.workCh)
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
//   - Empty batches are not processed (checked inside flushBatch)
//   - Reuses timers to reduce allocations and GC pressure
//   - When usePool=true, manages slice lifecycle through sync.Pool for memory efficiency
//   - Each batch dispatch is wrapped in defer/recover so a panic from the user fn is
//     logged, recorded as a metric and span error, and the worker keeps running
//   - A single b.ch receive can carry more than one item (PutBatch/PutBatchCtx);
//     appendItems splits it across as many flushBatch calls as needed
func (b *Batcher[T]) worker() { //nolint:gocognit,gocyclo // Worker function handles multiple channels and conditions
	var timer *time.Timer
	var timerCh <-chan time.Time // nil channel blocks forever, enabling lazy timer activation
	var batchLinks []trace.Link
	var exit bool

	// Registered first so they run last (LIFO): on any worker return, first
	// wait for outstanding background dispatches to finish (inflight), then
	// signal Close() that shutdown is fully complete (finished). This pairing
	// is what lets Close block until every queued item has been handed to fn.
	defer close(b.finished)
	defer b.inflight.Wait()

	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if t := b.ticker.Load(); t != nil {
			t.Stop()
		}
	}()

	// tickerCh is a worker-local channel derived from b.ticker.Load().C. It starts
	// nil (disabling the ticker select arm) and is resolved on the first b.ch receive
	// via an atomic load — preserving the same "nil until first item" semantics as the
	// previous localTickerCh snapshot approach while dropping the boolean flag.
	var tickerCh <-chan time.Time

workerLoop:
	for {
		select {
		case <-b.done:
			// Shutdown: drain the channel and dispatch every remaining item,
			// size-capped, then close the pool. See drainOnShutdown.
			b.drainOnShutdown(batchLinks)
			return

		case env := <-b.ch:
			if env.sc.IsValid() {
				batchLinks = append(batchLinks, trace.Link{SpanContext: env.sc})
			}

			// Resolve tickerCh from the atomic pointer on first (and subsequent)
			// item receives. Using atomic.Load here is race-safe without a boolean
			// flag: the caller's SetTickInterval write is visible by the time any
			// Put completes (Put → b.ch send establishes happens-before).
			if tickerCh == nil {
				if t := b.ticker.Load(); t != nil {
					tickerCh = t.C
				}
			}

			if b.drainMode.Load() { //nolint:nestif // Drain-then-fire loop with a nested non-blocking select; inherent to the drain-mode contract
				// Drain the already-queued items up to the size cap, then fire
				// immediately. appendItems may itself flush mid-drain if a
				// PutBatch envelope fills to size.
				items := env.items
				if items == nil {
					items = []*T{env.item}
				}
				var dFlushed bool
				batchLinks, dFlushed, exit = b.appendItems(items, batchLinks, ReasonDrain)
				if exit {
					b.drainOnShutdown(batchLinks)
					return
				}

				// Only chase more queued items if the initial envelope did not
				// itself fill and dispatch a batch. This bounds the drain to at
				// most one dispatched batch per outer-loop iteration, so b.done,
				// the trigger and the timer are always serviced promptly — even
				// under sustained load that keeps b.ch non-empty (previously the
				// loop could spin here for seconds without returning to select).
				if !dFlushed {
				drainLoop:
					for len(b.batch) < b.size {
						select {
						case drainEnv := <-b.ch:
							if drainEnv.sc.IsValid() {
								batchLinks = append(batchLinks, trace.Link{SpanContext: drainEnv.sc})
							}
							dItems := drainEnv.items
							if dItems == nil {
								dItems = []*T{drainEnv.item}
							}
							batchLinks, dFlushed, exit = b.appendItems(dItems, batchLinks, ReasonDrain)
							if exit {
								b.drainOnShutdown(batchLinks)
								return
							}
							if dFlushed {
								break drainLoop
							}
						default:
							break drainLoop
						}
					}
				}

				// Fire whatever partial remains (drain mode fires immediately);
				// a no-op if appendItems already dispatched everything.
				batchLinks, exit = b.flushBatch(ReasonDrain, batchLinks)
				if exit {
					b.drainOnShutdown(batchLinks)
					return
				}
				continue workerLoop
			}

			// Lazy timer activation: (re-)start whenever a fresh accumulation
			// cycle begins — either because the batch was empty when this
			// envelope arrived, or because appendItems flushed a full batch
			// partway through an over-sized PutBatch and left a trailing
			// partial batch that just started accumulating *during* this
			// call. Reuse the same Timer across batches to avoid a per-batch
			// allocation; safe under Go 1.23+ semantics where Reset on a
			// stopped/expired timer will not deliver a stale value. Skipped
			// when tick mode is wired — the tickerCh arm below drives all
			// time-based flushes.
			batchWasEmpty := len(b.batch) == 0

			items := env.items
			if items == nil {
				items = []*T{env.item}
			}

			var flushedDuringAppend bool
			batchLinks, flushedDuringAppend, exit = b.appendItems(items, batchLinks, ReasonSize)
			if exit {
				b.drainOnShutdown(batchLinks)
				return
			}

			if b.greedyAccumulate.Load() { //nolint:nestif // Greedy non-blocking drain with a mid-drain flush bound; inherent to the accumulate contract
				// Opportunistically pull additional already-queued items into
				// this same cycle without blocking and without firing early
				// (unlike drain mode's `default` arm, this loop never flushes
				// on an empty channel — it only stops trying). Each pulled
				// envelope still goes through appendItems, so oversized
				// PutBatch groups keep splitting at the cap exactly as above.
			greedyDrain:
				for len(b.batch) < b.size {
					select {
					case greedyEnv := <-b.ch:
						if greedyEnv.sc.IsValid() {
							batchLinks = append(batchLinks, trace.Link{SpanContext: greedyEnv.sc})
						}
						gItems := greedyEnv.items
						if gItems == nil {
							gItems = []*T{greedyEnv.item}
						}
						var gFlushed bool
						batchLinks, gFlushed, exit = b.appendItems(gItems, batchLinks, ReasonSize)
						flushedDuringAppend = flushedDuringAppend || gFlushed
						if exit {
							b.drainOnShutdown(batchLinks)
							return
						}
						if gFlushed {
							// A full batch was dispatched mid-drain; this cycle
							// is complete. Stop and return to the outer select so
							// done/trigger/timer are serviced — greedy must not
							// keep chasing a fresh cycle's items in one pass, which
							// would starve the worker under sustained load.
							break greedyDrain
						}
					default:
						break greedyDrain
					}
				}
			}

			switch {
			case len(b.batch) == 0:
				// Fully flushed (single item, or an exact-multiple PutBatch)
				// — nothing left accumulating, so any timer no longer applies.
				if timer != nil {
					timer.Stop()
					timerCh = nil
				}
			case tickerCh == nil && (batchWasEmpty || flushedDuringAppend):
				// A fresh cycle is accumulating as of right now: either this
				// envelope started it from empty, or a mid-call flush did.
				if timer == nil {
					timer = time.NewTimer(b.timeout)
				} else {
					timer.Reset(b.timeout)
				}
				timerCh = timer.C
			}

		case <-timerCh: // Only fires when timerCh != nil (batch has items).
			timerCh = nil
			batchLinks, exit = b.flushBatch(ReasonTimeout, batchLinks)
			if exit {
				b.drainOnShutdown(batchLinks)
				return
			}

		case <-tickerCh: // nil channel blocks forever when tick mode disabled.
			if len(b.batch) == 0 {
				// Skip empty tick — no fn call, no span, no metric.
				continue
			}
			batchLinks, exit = b.flushBatch(ReasonTimeout, batchLinks)
			if exit {
				b.drainOnShutdown(batchLinks)
				return
			}

		case <-b.triggerCh:
			if timer != nil {
				timer.Stop()
				timerCh = nil
			}
			// Non-blocking drain: if Trigger() raced ahead of a buffered Put, pull
			// any immediately-available items (up to the size cap) so they are
			// included in this flush rather than stranded until the next tick.
			var tFlushed bool
		triggerDrain:
			for len(b.batch) < b.size {
				select {
				case env := <-b.ch:
					if env.sc.IsValid() {
						batchLinks = append(batchLinks, trace.Link{SpanContext: env.sc})
					}
					// Resolve tickerCh if not yet done (same logic as b.ch arm above).
					if tickerCh == nil {
						if t := b.ticker.Load(); t != nil {
							tickerCh = t.C
						}
					}
					items := env.items
					if items == nil {
						items = []*T{env.item}
					}
					batchLinks, tFlushed, exit = b.appendItems(items, batchLinks, ReasonManual)
					if exit {
						b.drainOnShutdown(batchLinks)
						return
					}
					if tFlushed {
						// A full batch was dispatched mid-drain; stop chasing new
						// items so this Trigger cannot spin here under sustained
						// load (mirrors the greedy/drain bound). The trailing
						// partial is flushed below.
						break triggerDrain
					}
				default:
					break triggerDrain
				}
			}
			batchLinks, exit = b.flushBatch(ReasonManual, batchLinks)
			if exit {
				b.drainOnShutdown(batchLinks)
				return
			}
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
