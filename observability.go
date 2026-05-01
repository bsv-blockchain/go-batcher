package batcher

import (
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Logger is the minimal logging interface used by the batcher. Any logger that
// exposes printf-style level methods works; teranode's ulogger.Logger satisfies
// it structurally so it can be passed via WithLogger without an adapter.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// Metrics is the provider that mints per-batcher metric views. Bind is called
// once per Batcher construction with the batcher's name; the returned
// BoundMetrics has all label values pre-resolved so the hot path never
// allocates strings or looks up labels.
//
// A single Metrics provider can be shared across many Batcher instances in the
// same service — the per-batcher distinguishing label is resolved by Bind.
type Metrics interface {
	Bind(name string) BoundMetrics
}

// BoundMetrics is the per-batcher hot-path interface. All implementations
// should be allocation-free.
type BoundMetrics interface {
	// Enqueued is called once per Put / PutCtx.
	Enqueued()
	// EnqueueBlocked is called when a Put had to block waiting for channel
	// space; d is the time spent blocked.
	EnqueueBlocked(d time.Duration)
	// BatchTriggered is called once per dispatched batch with the trigger
	// reason — one of the Reason* constants.
	BatchTriggered(reason string)
	// BatchProcessed is called after the user's batch fn has returned (or
	// recovered from a panic) with the batch size and processing duration.
	BatchProcessed(size int, d time.Duration)
	// BackpressureWait is called when the worker waited for a slot in the
	// SetMaxConcurrent semaphore.
	BackpressureWait(d time.Duration)
	// WorkerStarted is called by a SetMaxConcurrent persistent worker when it
	// picks up a batch to process. Implementations should treat this as +1 to
	// a gauge; combined with the BackpressureWait histogram it indicates pool
	// saturation. Not called on the n==0 (unbounded) path.
	WorkerStarted()
	// WorkerFinished is called by a SetMaxConcurrent persistent worker after
	// the dispatch returns (panics are already recovered upstream). Pair with
	// WorkerStarted: implementations should treat this as -1 to the same gauge.
	WorkerFinished()
	// DedupHit / DedupMiss are recorded by BatcherWithDedup on every Put.
	DedupHit()
	DedupMiss()
	// PanicRecovered is called when the user fn panicked and the worker
	// recovered.
	PanicRecovered()
}

// Reason values reported via BatchTriggered and used as the Prometheus
// `reason` label on the batches_total counter.
const (
	ReasonSize     = "size"
	ReasonTimeout  = "timeout"
	ReasonManual   = "manual"
	ReasonDrain    = "drain"
	ReasonShutdown = "shutdown"
)

// Option configures observability and naming on a Batcher. Options are passed
// as a trailing variadic to the existing constructors; omitting them yields
// fully no-op behavior with zero allocations on the hot path.
type Option func(*config)

type config struct {
	logger       Logger
	metrics      Metrics // raw, may be nil; resolved into metricsBound by applyOptions
	metricsBound BoundMetrics
	tracer       trace.Tracer
	name         string
}

func defaultConfig() *config {
	return &config{
		logger:       nopLogger{},
		metricsBound: nopBoundMetrics{},
		tracer:       noop.NewTracerProvider().Tracer("github.com/bsv-blockchain/go-batcher"),
		name:         "batcher",
	}
}

// applyOptions resolves a final config from the variadic options. It binds
// the Metrics provider (if any) to the resolved name so the runtime config
// holds only the pre-bound BoundMetrics view.
func applyOptions(opts []Option) *config {
	c := defaultConfig()
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	if c.metrics != nil {
		c.metricsBound = c.metrics.Bind(c.name)
	}
	return c
}

// WithLogger attaches a logger. Passing nil restores the default no-op logger.
func WithLogger(l Logger) Option {
	return func(c *config) {
		if l == nil {
			c.logger = nopLogger{}
			return
		}
		c.logger = l
	}
}

// WithMetrics attaches a Metrics provider. The provider's Bind(name) is
// invoked once during construction; the returned BoundMetrics is what the
// hot path calls. Passing nil leaves metrics disabled.
func WithMetrics(m Metrics) Option {
	return func(c *config) {
		c.metrics = m
	}
}

// WithTracer attaches an OpenTelemetry Tracer. The batcher starts one span
// per dispatched batch named "<name>.flush" with attributes batcher.name,
// batcher.batch_size, batcher.reason. SpanContexts attached to items via
// PutCtx are recorded as span links. Passing nil restores the default no-op
// tracer.
func WithTracer(t trace.Tracer) Option {
	return func(c *config) {
		if t == nil {
			c.tracer = noop.NewTracerProvider().Tracer("github.com/bsv-blockchain/go-batcher")
			return
		}
		c.tracer = t
	}
}

// WithName sets the batcher's identifier. It is used as the Prometheus
// `batcher` label value and as the prefix for OTel span names. Default is
// "batcher". Set this explicitly when running multiple batchers in the same
// service so their telemetry can be distinguished.
func WithName(name string) Option {
	return func(c *config) {
		if name != "" {
			c.name = name
		}
	}
}

type nopLogger struct{}

func (nopLogger) Debugf(string, ...any) {}
func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

type nopBoundMetrics struct{}

func (nopBoundMetrics) Enqueued()                         {}
func (nopBoundMetrics) EnqueueBlocked(time.Duration)      {}
func (nopBoundMetrics) BatchTriggered(string)             {}
func (nopBoundMetrics) BatchProcessed(int, time.Duration) {}
func (nopBoundMetrics) BackpressureWait(time.Duration)    {}
func (nopBoundMetrics) WorkerStarted()                    {}
func (nopBoundMetrics) WorkerFinished()                   {}
func (nopBoundMetrics) DedupHit()                         {}
func (nopBoundMetrics) DedupMiss()                        {}
func (nopBoundMetrics) PanicRecovered()                   {}
