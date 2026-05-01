package batcher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

// --- Mock metrics ---------------------------------------------------------

type mockMetrics struct {
	bound *mockBoundMetrics
}

func (m *mockMetrics) Bind(string) BoundMetrics { return m.bound }

type mockBoundMetrics struct {
	mu                sync.Mutex
	enqueued          int
	enqueueBlocked    int
	batchesByReason   map[string]int
	batchProcessed    int
	totalBatchSize    int
	totalBatchSeconds float64
	backpressureWait  int
	dedupHit          int
	dedupMiss         int
	panicRecovered    int
}

func newMockBoundMetrics() *mockBoundMetrics {
	return &mockBoundMetrics{batchesByReason: map[string]int{}}
}

func (m *mockBoundMetrics) Enqueued() {
	m.mu.Lock()
	m.enqueued++
	m.mu.Unlock()
}

func (m *mockBoundMetrics) EnqueueBlocked(time.Duration) {
	m.mu.Lock()
	m.enqueueBlocked++
	m.mu.Unlock()
}

func (m *mockBoundMetrics) BatchTriggered(reason string) {
	m.mu.Lock()
	m.batchesByReason[reason]++
	m.mu.Unlock()
}

func (m *mockBoundMetrics) BatchProcessed(size int, d time.Duration) {
	m.mu.Lock()
	m.batchProcessed++
	m.totalBatchSize += size
	m.totalBatchSeconds += d.Seconds()
	m.mu.Unlock()
}

func (m *mockBoundMetrics) BackpressureWait(time.Duration) {
	m.mu.Lock()
	m.backpressureWait++
	m.mu.Unlock()
}

func (m *mockBoundMetrics) DedupHit() {
	m.mu.Lock()
	m.dedupHit++
	m.mu.Unlock()
}

func (m *mockBoundMetrics) DedupMiss() {
	m.mu.Lock()
	m.dedupMiss++
	m.mu.Unlock()
}

func (m *mockBoundMetrics) PanicRecovered() {
	m.mu.Lock()
	m.panicRecovered++
	m.mu.Unlock()
}

type metricsSnapshot struct {
	enqueued         int
	enqueueBlocked   int
	batchesByReason  map[string]int
	batchProcessed   int
	totalBatchSize   int
	backpressureWait int
	dedupHit         int
	dedupMiss        int
	panicRecovered   int
}

// snapshot returns a copy of the counters under lock so assertions don't race
// with worker goroutines that may still be reporting. (We can't simply copy
// the struct because it embeds a sync.Mutex.)
func (m *mockBoundMetrics) snapshot() metricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	reasons := make(map[string]int, len(m.batchesByReason))
	for k, v := range m.batchesByReason {
		reasons[k] = v
	}
	return metricsSnapshot{
		enqueued:         m.enqueued,
		enqueueBlocked:   m.enqueueBlocked,
		batchesByReason:  reasons,
		batchProcessed:   m.batchProcessed,
		totalBatchSize:   m.totalBatchSize,
		backpressureWait: m.backpressureWait,
		dedupHit:         m.dedupHit,
		dedupMiss:        m.dedupMiss,
		panicRecovered:   m.panicRecovered,
	}
}

// --- Mock logger ----------------------------------------------------------

type mockLogger struct {
	mu     sync.Mutex
	errors []string
}

func (l *mockLogger) Debugf(string, ...any) {}
func (l *mockLogger) Infof(string, ...any)  {}
func (l *mockLogger) Warnf(string, ...any)  {}
func (l *mockLogger) Errorf(format string, _ ...any) {
	l.mu.Lock()
	l.errors = append(l.errors, format)
	l.mu.Unlock()
}

func (l *mockLogger) errorCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.errors)
}

// --- Mock tracer ----------------------------------------------------------

type recordingTracer struct {
	embedded.Tracer

	mu    sync.Mutex
	spans []*recordingSpan
}

func (t *recordingTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	cfg := trace.NewSpanStartConfig(opts...)
	s := &recordingSpan{
		tracer:     t,
		name:       name,
		attributes: cfg.Attributes(),
		links:      cfg.Links(),
	}
	t.mu.Lock()
	t.spans = append(t.spans, s)
	t.mu.Unlock()
	return trace.ContextWithSpan(ctx, s), s
}

func (t *recordingTracer) snapshot() []*recordingSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*recordingSpan, len(t.spans))
	copy(out, t.spans)
	return out
}

type recordingSpan struct {
	embedded.Span

	tracer     *recordingTracer
	name       string
	attributes []attribute.KeyValue
	links      []trace.Link
	statusCode codes.Code
	statusDesc string
	errored    atomic.Bool
	ended      atomic.Bool
}

func (s *recordingSpan) End(...trace.SpanEndOption)              { s.ended.Store(true) }
func (s *recordingSpan) AddEvent(string, ...trace.EventOption)   {}
func (s *recordingSpan) AddLink(trace.Link)                      {}
func (s *recordingSpan) IsRecording() bool                       { return true }
func (s *recordingSpan) RecordError(error, ...trace.EventOption) { s.errored.Store(true) }
func (s *recordingSpan) SpanContext() trace.SpanContext          { return trace.SpanContext{} }
func (s *recordingSpan) SetStatus(c codes.Code, d string)        { s.statusCode = c; s.statusDesc = d }
func (s *recordingSpan) SetName(string)                          {}
func (s *recordingSpan) SetAttributes(kv ...attribute.KeyValue) {
	s.attributes = append(s.attributes, kv...)
}
func (s *recordingSpan) TracerProvider() trace.TracerProvider { return nil }

// --- Tests ----------------------------------------------------------------

func newTestSetup(t *testing.T, size int, timeout time.Duration, fn func([]*int), background bool) (*Batcher[int], *mockBoundMetrics, *recordingTracer, *mockLogger) {
	t.Helper()
	bm := newMockBoundMetrics()
	mm := &mockMetrics{bound: bm}
	tr := &recordingTracer{}
	lg := &mockLogger{}
	b := New(size, timeout, fn, background,
		WithName("testbatcher"),
		WithMetrics(mm),
		WithTracer(tr),
		WithLogger(lg),
	)
	return b, bm, tr, lg
}

func TestEnqueuedAndBatchProcessedMetric(t *testing.T) {
	processed := make(chan int, 4)
	b, bm, _, _ := newTestSetup(t, 3, time.Hour, func(batch []*int) {
		processed <- len(batch)
	}, false)
	defer b.Close()

	one, two, three := 1, 2, 3
	b.Put(&one)
	b.Put(&two)
	b.Put(&three) // triggers size flush

	got := <-processed
	require.Equal(t, 3, got)

	// Wait for the metric to be recorded after fn returns.
	require.Eventually(t, func() bool {
		s := bm.snapshot()
		return s.batchProcessed == 1
	}, time.Second, 10*time.Millisecond)

	s := bm.snapshot()
	assert.Equal(t, 3, s.enqueued)
	assert.Equal(t, 1, s.batchProcessed)
	assert.Equal(t, 3, s.totalBatchSize)
	assert.Equal(t, 1, s.batchesByReason[ReasonSize])
}

func TestTimeoutTriggerReason(t *testing.T) {
	processed := make(chan int, 4)
	b, bm, _, _ := newTestSetup(t, 100, 25*time.Millisecond, func(batch []*int) {
		processed <- len(batch)
	}, false)
	defer b.Close()

	one := 1
	b.Put(&one)

	require.Equal(t, 1, <-processed)
	require.Eventually(t, func() bool {
		return bm.snapshot().batchesByReason[ReasonTimeout] == 1
	}, time.Second, 10*time.Millisecond)
}

func TestManualTriggerReason(t *testing.T) {
	processed := make(chan int, 4)
	b, bm, _, _ := newTestSetup(t, 100, time.Hour, func(batch []*int) {
		processed <- len(batch)
	}, false)
	defer b.Close()

	one := 1
	b.Put(&one)
	// Give the worker a moment to drain the channel before triggering, so the
	// trigger sees a populated batch. (Without this, select between b.ch and
	// triggerCh races nondeterministically.)
	time.Sleep(20 * time.Millisecond)
	b.Trigger()

	require.Equal(t, 1, <-processed)
	require.Eventually(t, func() bool {
		return bm.snapshot().batchesByReason[ReasonManual] == 1
	}, time.Second, 10*time.Millisecond)
}

func TestDrainModeReason(t *testing.T) {
	processed := make(chan int, 4)
	b, bm, _, _ := newTestSetup(t, 100, time.Hour, func(batch []*int) {
		processed <- len(batch)
	}, false)
	defer b.Close()

	b.SetDrainMode(true)
	one, two := 1, 2
	b.Put(&one)
	b.Put(&two)

	// Drain mode dispatches as soon as items are available; both items end up
	// in one batch (or two — depending on scheduling), but reason must be drain.
	for total := 0; total < 2; {
		total += <-processed
	}
	require.Eventually(t, func() bool {
		s := bm.snapshot()
		return s.batchesByReason[ReasonDrain] >= 1 && s.batchProcessed >= 1
	}, time.Second, 10*time.Millisecond)
}

func TestShutdownDrainReason(t *testing.T) {
	processed := make(chan int, 4)
	b, bm, _, _ := newTestSetup(t, 100, time.Hour, func(batch []*int) {
		processed <- len(batch)
	}, false)

	one, two := 1, 2
	b.Put(&one)
	b.Put(&two)
	b.Close()

	got := 0
	for got < 2 {
		got += <-processed
	}
	require.Eventually(t, func() bool {
		return bm.snapshot().batchesByReason[ReasonShutdown] == 1
	}, time.Second, 10*time.Millisecond)
}

func TestPanicRecovery(t *testing.T) {
	calls := atomic.Int32{}
	b, bm, tr, lg := newTestSetup(t, 1, time.Hour, func(_ []*int) {
		n := calls.Add(1)
		if n == 1 {
			panic("boom")
		}
	}, false)
	defer b.Close()

	one, two := 1, 2
	b.Put(&one)
	require.Eventually(t, func() bool {
		return bm.snapshot().panicRecovered == 1
	}, time.Second, 10*time.Millisecond)

	// Worker must keep going.
	b.Put(&two)
	require.Eventually(t, func() bool {
		return calls.Load() == 2
	}, time.Second, 10*time.Millisecond)

	assert.GreaterOrEqual(t, lg.errorCount(), 1, "expected panic to be logged via logger.Errorf")

	// Span for the panicking batch should have an error status recorded.
	spans := tr.snapshot()
	require.NotEmpty(t, spans)
	first := spans[0]
	assert.True(t, first.errored.Load(), "first span should have RecordError called")
	assert.Equal(t, codes.Error, first.statusCode)
}

func TestPutCtxAttachesSpanLink(t *testing.T) {
	processed := make(chan struct{}, 4)
	b, _, tr, _ := newTestSetup(t, 1, time.Hour, func(_ []*int) {
		processed <- struct{}{}
	}, false)
	defer b.Close()

	// Build a parent span context to attach via PutCtx.
	tid, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	require.NoError(t, err)
	sid, err := trace.SpanIDFromHex("0102030405060708")
	require.NoError(t, err)
	parentCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	}))

	one := 42
	b.PutCtx(parentCtx, &one)
	<-processed

	require.Eventually(t, func() bool {
		spans := tr.snapshot()
		return len(spans) == 1
	}, time.Second, 10*time.Millisecond)

	span := tr.snapshot()[0]
	assert.Equal(t, "testbatcher.flush", span.name)
	require.Len(t, span.links, 1)
	assert.Equal(t, tid, span.links[0].SpanContext.TraceID())
	assert.Equal(t, sid, span.links[0].SpanContext.SpanID())

	// Check expected attributes present.
	var sawName, sawSize, sawReason bool
	for _, kv := range span.attributes {
		switch kv.Key {
		case "batcher.name":
			sawName = kv.Value.AsString() == "testbatcher"
		case "batcher.batch_size":
			sawSize = kv.Value.AsInt64() == 1
		case "batcher.reason":
			sawReason = kv.Value.AsString() == ReasonSize
		}
	}
	assert.True(t, sawName && sawSize && sawReason, "expected batcher.name, batcher.batch_size, batcher.reason attributes; got %v", span.attributes)
}

func TestBackpressureWaitMetric(t *testing.T) {
	release := make(chan struct{})
	b, bm, _, _ := newTestSetup(t, 1, time.Hour, func(_ []*int) {
		<-release // hold the goroutine to force the next batch to wait on the semaphore
	}, true)
	b.SetMaxConcurrent(1)
	defer b.Close()

	one, two := 1, 2
	b.Put(&one) // dispatches, fn blocks on `release`
	// Allow goroutine to start and acquire sem.
	time.Sleep(20 * time.Millisecond)
	b.Put(&two) // should hit semaphore wait

	// Give worker time to attempt sem acquire and block.
	time.Sleep(20 * time.Millisecond)
	close(release) // unblock both fn calls

	require.Eventually(t, func() bool {
		return bm.snapshot().backpressureWait >= 1
	}, time.Second, 10*time.Millisecond)
}

func TestDedupMetrics(t *testing.T) {
	bm := newMockBoundMetrics()
	mm := &mockMetrics{bound: bm}

	processed := make(chan []*int, 4)
	b := NewWithDeduplication[int](10, 50*time.Millisecond, func(batch []*int) {
		cp := append([]*int(nil), batch...)
		processed <- cp
	}, false, WithName("dedup"), WithMetrics(mm))
	defer b.Close()

	a, c := 1, 2
	b.Put(&a)
	b.Put(&a) // duplicate
	b.Put(&c)
	b.Trigger()

	<-processed
	require.Eventually(t, func() bool {
		s := bm.snapshot()
		return s.dedupHit == 1 && s.dedupMiss == 2
	}, time.Second, 10*time.Millisecond)
}

func TestDefaultsAreNoOpAndDontPanic(t *testing.T) {
	// Construct without any options; existing callers must keep working
	// exactly as before, and no observability state should be touched.
	processed := make(chan int, 4)
	b := New(2, 50*time.Millisecond, func(batch []*int) {
		processed <- len(batch)
	}, false)
	defer b.Close()

	one, two := 1, 2
	b.Put(&one)
	b.Put(&two)

	require.Equal(t, 2, <-processed)
}
