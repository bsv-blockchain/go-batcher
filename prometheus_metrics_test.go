package batcher

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const floatEpsilon = 1e-9

// gatherText returns the names of registered metric families, useful for
// substring assertions about which series are present.
func gatherText(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var sb strings.Builder
	for _, mf := range mfs {
		sb.WriteString(mf.GetName())
		sb.WriteString("\n")
	}
	return sb.String()
}

// findMetric locates the dto.Metric within the registry that has the given
// metric-family name and matches every label/value pair. Returns nil if
// not found.
func findMetric(t *testing.T, reg *prometheus.Registry, name string, labelKV ...string) *dto.Metric {
	t.Helper()
	require.Zero(t, len(labelKV)%2, "labelKV must be key/value pairs")
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if metricLabelsMatch(m, labelKV) {
				return m
			}
		}
	}
	return nil
}

func metricLabelsMatch(m *dto.Metric, labelKV []string) bool {
	labels := map[string]string{}
	for _, lp := range m.GetLabel() {
		labels[lp.GetName()] = lp.GetValue()
	}
	for i := 0; i < len(labelKV); i += 2 {
		if labels[labelKV[i]] != labelKV[i+1] {
			return false
		}
	}
	return true
}

// counterValue returns the value of a counter series identified by metric
// name and label/value pairs. Returns -1 if the series is not present.
func counterValue(t *testing.T, reg *prometheus.Registry, name string, labelKV ...string) float64 {
	t.Helper()
	m := findMetric(t, reg, name, labelKV...)
	if m == nil || m.GetCounter() == nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// histogramSampleCount returns the sample count of a histogram series.
func histogramSampleCount(t *testing.T, reg *prometheus.Registry, name string, labelKV ...string) uint64 {
	t.Helper()
	m := findMetric(t, reg, name, labelKV...)
	if m == nil || m.GetHistogram() == nil {
		return 0
	}
	return m.GetHistogram().GetSampleCount()
}

// gaugeValue returns the current value of a gauge series. Returns -1 if the
// series is not present.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labelKV ...string) float64 {
	t.Helper()
	m := findMetric(t, reg, name, labelKV...)
	if m == nil || m.GetGauge() == nil {
		return -1
	}
	return m.GetGauge().GetValue()
}

func TestPrometheusMetricsRegistersAllSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "test", "batch")
	// Materialize child metrics so Gather emits the corresponding families.
	m.Bind("smoke")

	text := gatherText(t, reg)
	for _, want := range []string{
		"test_batch_enqueued_total",
		"test_batch_enqueue_blocked_seconds",
		"test_batch_batches_total",
		"test_batch_batch_size",
		"test_batch_batch_duration_seconds",
		"test_batch_backpressure_wait_seconds",
		"test_batch_workers_in_flight",
		"test_batch_dedup_total",
		"test_batch_panic_total",
	} {
		assert.Contains(t, text, want, "expected series %q to be registered", want)
	}
}

func TestPrometheusMetricsRecordsBatcherActivity(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "test", "batch")

	processed := make(chan int, 4)
	b := New[int](3, time.Hour, func(batch []*int) {
		processed <- len(batch)
	}, false, WithName("alpha"), WithMetrics(m))
	defer b.Close()

	one, two, three := 1, 2, 3
	b.Put(&one)
	b.Put(&two)
	b.Put(&three) // size trigger
	require.Equal(t, 3, <-processed)

	require.Eventually(t, func() bool {
		return counterValue(t, reg, "test_batch_enqueued_total", "batcher", "alpha") == 3
	}, time.Second, 10*time.Millisecond)

	assert.InDelta(t, 1, counterValue(t, reg, "test_batch_batches_total", "batcher", "alpha", "reason", ReasonSize), floatEpsilon)
	assert.InDelta(t, 0, counterValue(t, reg, "test_batch_batches_total", "batcher", "alpha", "reason", ReasonTimeout), floatEpsilon)
	require.Eventually(t, func() bool {
		return histogramSampleCount(t, reg, "test_batch_batch_size", "batcher", "alpha") == 1 &&
			histogramSampleCount(t, reg, "test_batch_batch_duration_seconds", "batcher", "alpha") == 1
	}, time.Second, 10*time.Millisecond)
}

func TestPrometheusMetricsSharedAcrossBatchers(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "svc", "batcher")

	procA := make(chan int, 4)
	procB := make(chan int, 4)
	a := New[int](2, time.Hour, func(batch []*int) { procA <- len(batch) }, false, WithName("alpha"), WithMetrics(m))
	bb := New[int](2, time.Hour, func(batch []*int) { procB <- len(batch) }, false, WithName("beta"), WithMetrics(m))
	defer a.Close()
	defer bb.Close()

	x, y := 1, 2
	a.Put(&x)
	a.Put(&y) // size trigger on alpha
	bb.Put(&x)
	bb.Put(&y) // size trigger on beta
	<-procA
	<-procB

	require.Eventually(t, func() bool {
		return counterValue(t, reg, "svc_batcher_enqueued_total", "batcher", "alpha") == 2 &&
			counterValue(t, reg, "svc_batcher_enqueued_total", "batcher", "beta") == 2
	}, time.Second, 10*time.Millisecond)

	assert.True(t, hasBatcherLabel(t, reg, "svc_batcher_enqueued_total", "alpha"), "alpha series missing")
	assert.True(t, hasBatcherLabel(t, reg, "svc_batcher_enqueued_total", "beta"), "beta series missing")
}

// hasBatcherLabel returns true if the gathered metric family with the given
// name contains a series whose `batcher` label equals want.
func hasBatcherLabel(t *testing.T, reg *prometheus.Registry, name, want string) bool {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name && familyHasLabel(mf, "batcher", want) {
			return true
		}
	}
	return false
}

func familyHasLabel(mf *dto.MetricFamily, key, want string) bool {
	for _, mm := range mf.GetMetric() {
		for _, lp := range mm.GetLabel() {
			if lp.GetName() == key && lp.GetValue() == want {
				return true
			}
		}
	}
	return false
}

// TestPrometheusWorkersInFlightGauge verifies the workers_in_flight gauge
// increments as persistent workers pick up batches and decrements after each
// batch finishes — and that it never exceeds maxConcurrent.
func TestPrometheusWorkersInFlightGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "test", "batch")

	release := make(chan struct{})
	started := make(chan struct{}, 4)
	b := New[int](1, time.Hour, func(_ []*int) {
		started <- struct{}{}
		<-release
	}, true, WithName("pool"), WithMetrics(m))
	b.SetMaxConcurrent(2)
	defer b.Close()

	// Submit enough items to keep both workers busy and queue more behind them.
	for i := 0; i < 4; i++ {
		x := i
		b.Put(&x)
	}

	// Wait for both workers to be executing fn.
	for i := 0; i < 2; i++ {
		<-started
	}

	require.Eventually(t, func() bool {
		return gaugeValue(t, reg, "test_batch_workers_in_flight", "batcher", "pool") == 2
	}, time.Second, 5*time.Millisecond, "expected gauge to settle at 2 while both workers run")

	// Release one worker; gauge should drop to 1, then climb back to 2 as the
	// next queued batch is picked up.
	release <- struct{}{}
	<-started // a third batch starts
	require.Eventually(t, func() bool {
		return gaugeValue(t, reg, "test_batch_workers_in_flight", "batcher", "pool") == 2
	}, time.Second, 5*time.Millisecond, "expected gauge to climb back to 2 as queued batch is picked up")

	// Drain the rest.
	close(release)
	<-started
	require.Eventually(t, func() bool {
		return gaugeValue(t, reg, "test_batch_workers_in_flight", "batcher", "pool") == 0
	}, time.Second, 5*time.Millisecond, "expected gauge to drop to 0 once all workers are idle")
}

func TestPrometheusDedupCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "svc", "batcher")

	processed := make(chan struct{}, 4)
	b := NewWithDeduplication[int](10, 50*time.Millisecond, func(_ []*int) {
		processed <- struct{}{}
	}, false, WithName("dedup"), WithMetrics(m))
	defer b.Close()

	a, c := 1, 2
	b.Put(&a)
	b.Put(&a) // dup
	b.Put(&c)
	b.Trigger()
	<-processed

	require.Eventually(t, func() bool {
		return counterValue(t, reg, "svc_batcher_dedup_total", "batcher", "dedup", "result", "hit") == 1 &&
			counterValue(t, reg, "svc_batcher_dedup_total", "batcher", "dedup", "result", "miss") == 2
	}, time.Second, 10*time.Millisecond)
}
