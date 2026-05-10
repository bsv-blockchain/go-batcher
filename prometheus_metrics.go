package batcher

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const labelBatcher = "batcher"

// metricsBucketsMicroSeconds mirrors teranode's util.MetricsBucketsMicroSeconds
// so histograms align across services.
//
//nolint:gochecknoglobals // Histogram bucket schedule, identical to teranode's util/metrics.go.
var metricsBucketsMicroSeconds = []float64{
	0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001,
	0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
}

// metricsBucketsMilliSeconds mirrors teranode's util.MetricsBucketsMilliSeconds.
//
//nolint:gochecknoglobals // Histogram bucket schedule, identical to teranode's util/metrics.go.
var metricsBucketsMilliSeconds = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 4,
}

type prometheusMetrics struct {
	enqueued         *prometheus.CounterVec
	enqueueBlocked   *prometheus.HistogramVec
	batches          *prometheus.CounterVec
	batchSize        *prometheus.HistogramVec
	batchDuration    *prometheus.HistogramVec
	backpressureWait *prometheus.HistogramVec
	workersInFlight  *prometheus.GaugeVec
	dedup            *prometheus.CounterVec
	panics           *prometheus.CounterVec
}

// NewPrometheusMetrics constructs a Metrics provider that records all batcher
// telemetry as Prometheus series under the given namespace and subsystem.
//
// The returned provider can (and should) be shared across all Batcher
// instances in a service. Each call to Bind(name) attaches the per-batcher
// label set with all metric handles pre-resolved for hot-path use.
//
// Wire it up with:
//
//	m := batcher.NewPrometheusMetrics(reg, "myservice", labelBatcher)
//	store := batcher.New(..., batcher.WithName("store"),  batcher.WithMetrics(m))
//	get   := batcher.New(..., batcher.WithName("get"),    batcher.WithMetrics(m))
//
// Both batchers feed the same series, distinguished by the `batcher` label.
func NewPrometheusMetrics(reg prometheus.Registerer, namespace, subsystem string) Metrics {
	factory := promauto.With(reg)
	return &prometheusMetrics{
		enqueued: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "enqueued_total",
			Help:      "Total items submitted to the batcher.",
		}, []string{labelBatcher}),
		enqueueBlocked: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "enqueue_blocked_seconds",
			Help:      "Seconds a Put spent blocked when the channel was full.",
			Buckets:   metricsBucketsMicroSeconds,
		}, []string{labelBatcher}),
		batches: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batches_total",
			Help:      "Total batches dispatched, by trigger reason.",
		}, []string{labelBatcher, "reason"}),
		batchSize: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_size",
			Help:      "Size of each dispatched batch.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 14),
		}, []string{labelBatcher}),
		batchDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "batch_duration_seconds",
			Help:      "Duration of the user batch-processing function.",
			Buckets:   metricsBucketsMilliSeconds,
		}, []string{labelBatcher}),
		backpressureWait: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "backpressure_wait_seconds",
			Help:      "Time the worker waited for a SetMaxConcurrent worker slot.",
			Buckets:   metricsBucketsMilliSeconds,
		}, []string{labelBatcher}),
		workersInFlight: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "workers_in_flight",
			Help:      "Number of SetMaxConcurrent persistent workers currently executing the batch fn. Combined with backpressure_wait_seconds it indicates pool saturation.",
		}, []string{labelBatcher}),
		dedup: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "dedup_total",
			Help:      "Deduplication results, by outcome.",
		}, []string{labelBatcher, "result"}),
		panics: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "panic_total",
			Help:      "Panics recovered while running the batch function.",
		}, []string{labelBatcher}),
	}
}

// Bind resolves all label values for the given batcher name and returns a
// BoundMetrics with concrete Prometheus metric handles cached.
func (m *prometheusMetrics) Bind(name string) BoundMetrics {
	return &boundPrometheusMetrics{
		enqueued:            m.enqueued.WithLabelValues(name),
		enqueueBlocked:      m.enqueueBlocked.WithLabelValues(name),
		batchSize:           m.batchSize.WithLabelValues(name),
		batchDuration:       m.batchDuration.WithLabelValues(name),
		backpressureWait:    m.backpressureWait.WithLabelValues(name),
		workersInFlight:     m.workersInFlight.WithLabelValues(name),
		panics:              m.panics.WithLabelValues(name),
		batchSizeReason:     m.batches.WithLabelValues(name, ReasonSize),
		batchTimeoutReason:  m.batches.WithLabelValues(name, ReasonTimeout),
		batchManualReason:   m.batches.WithLabelValues(name, ReasonManual),
		batchDrainReason:    m.batches.WithLabelValues(name, ReasonDrain),
		batchShutdownReason: m.batches.WithLabelValues(name, ReasonShutdown),
		dedupHit:            m.dedup.WithLabelValues(name, "hit"),
		dedupMiss:           m.dedup.WithLabelValues(name, "miss"),
	}
}

type boundPrometheusMetrics struct {
	enqueued            prometheus.Counter
	enqueueBlocked      prometheus.Observer
	batchSize           prometheus.Observer
	batchDuration       prometheus.Observer
	backpressureWait    prometheus.Observer
	workersInFlight     prometheus.Gauge
	panics              prometheus.Counter
	batchSizeReason     prometheus.Counter
	batchTimeoutReason  prometheus.Counter
	batchManualReason   prometheus.Counter
	batchDrainReason    prometheus.Counter
	batchShutdownReason prometheus.Counter
	dedupHit            prometheus.Counter
	dedupMiss           prometheus.Counter
}

func (b *boundPrometheusMetrics) Enqueued() { b.enqueued.Inc() }

func (b *boundPrometheusMetrics) EnqueueBlocked(d time.Duration) {
	b.enqueueBlocked.Observe(d.Seconds())
}

func (b *boundPrometheusMetrics) BackpressureWait(d time.Duration) {
	b.backpressureWait.Observe(d.Seconds())
}

func (b *boundPrometheusMetrics) WorkerStarted()  { b.workersInFlight.Inc() }
func (b *boundPrometheusMetrics) WorkerFinished() { b.workersInFlight.Dec() }

func (b *boundPrometheusMetrics) DedupHit()       { b.dedupHit.Inc() }
func (b *boundPrometheusMetrics) DedupMiss()      { b.dedupMiss.Inc() }
func (b *boundPrometheusMetrics) PanicRecovered() { b.panics.Inc() }

func (b *boundPrometheusMetrics) BatchTriggered(reason string) {
	switch reason {
	case ReasonSize:
		b.batchSizeReason.Inc()
	case ReasonTimeout:
		b.batchTimeoutReason.Inc()
	case ReasonManual:
		b.batchManualReason.Inc()
	case ReasonDrain:
		b.batchDrainReason.Inc()
	case ReasonShutdown:
		b.batchShutdownReason.Inc()
	}
}

func (b *boundPrometheusMetrics) BatchProcessed(size int, d time.Duration) {
	b.batchSize.Observe(float64(size))
	b.batchDuration.Observe(d.Seconds())
}
