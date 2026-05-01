package batcher

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// counterValue returns the value of a counter series identified by metric
// name and label/value pairs. Returns -1 if the series is not present.
func counterValue(t *testing.T, reg *prometheus.Registry, name string, labelKV ...string) float64 {
	t.Helper()
	require.Zero(t, len(labelKV)%2, "labelKV must be key/value pairs")
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	NEXT:
		for _, m := range mf.Metric {
			labels := map[string]string{}
			for _, lp := range m.Label {
				labels[lp.GetName()] = lp.GetValue()
			}
			for i := 0; i < len(labelKV); i += 2 {
				if labels[labelKV[i]] != labelKV[i+1] {
					continue NEXT
				}
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1
}

// histogramSampleCount returns the sample count of a histogram series.
func histogramSampleCount(t *testing.T, reg *prometheus.Registry, name string, labelKV ...string) uint64 {
	t.Helper()
	require.Zero(t, len(labelKV)%2)
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	NEXT:
		for _, m := range mf.Metric {
			labels := map[string]string{}
			for _, lp := range m.Label {
				labels[lp.GetName()] = lp.GetValue()
			}
			for i := 0; i < len(labelKV); i += 2 {
				if labels[labelKV[i]] != labelKV[i+1] {
					continue NEXT
				}
			}
			if h := m.GetHistogram(); h != nil {
				return h.GetSampleCount()
			}
		}
	}
	return 0
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

	assert.Equal(t, float64(1), counterValue(t, reg, "test_batch_batches_total", "batcher", "alpha", "reason", ReasonSize))
	assert.Equal(t, float64(0), counterValue(t, reg, "test_batch_batches_total", "batcher", "alpha", "reason", ReasonTimeout))
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

	// Both batchers should appear as distinct series under the same metric.
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var sawAlpha, sawBeta bool
	for _, mf := range mfs {
		if mf.GetName() != "svc_batcher_enqueued_total" {
			continue
		}
		for _, mm := range mf.Metric {
			for _, lp := range mm.Label {
				if lp.GetName() == "batcher" {
					switch lp.GetValue() {
					case "alpha":
						sawAlpha = true
					case "beta":
						sawBeta = true
					}
				}
			}
		}
	}
	assert.True(t, sawAlpha, "alpha series missing")
	assert.True(t, sawBeta, "beta series missing")
}

func TestPrometheusDedupCounters(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg, "svc", "batcher")

	processed := make(chan struct{}, 4)
	b := NewWithDeduplication[int](10, 50*time.Millisecond, func(batch []*int) {
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
