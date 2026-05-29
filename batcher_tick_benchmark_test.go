package batcher

import (
	"sync"
	"testing"
	"time"
)

// BenchmarkTickMode_SparseLoadLatency measures the Put→fn-entry latency under
// sparse load (idle periods between Puts). Compares tick mode (steady cadence)
// against lazy-timer mode at the same nominal interval.
//
// Expectation: tick mode shows ~50% of the lazy-timer latency on average, because
// the lazy timer always fires `d` after the first-item-of-batch arrival, while
// the ticker fires on a free-running schedule that the arriving item joins
// partway through (mean wait = d/2).
func BenchmarkTickMode_SparseLoadLatency(bb *testing.B) { //nolint:gocognit // Benchmark covers two sub-cases with latency tracking and statistical reporting
	const interval = 10 * time.Millisecond
	const idleBetweenPuts = 3 * interval

	cases := []struct {
		name  string
		setup func(fn func(batch []*int)) *Batcher[int]
	}{
		{
			name: "lazy_timer",
			setup: func(fn func(batch []*int)) *Batcher[int] {
				return New[int](1000, interval, fn, false)
			},
		},
		{
			name: "tick_mode",
			setup: func(fn func(batch []*int)) *Batcher[int] {
				b := New[int](1000, time.Hour, fn, false)
				b.SetTickInterval(interval)
				return b
			},
		},
	}

	for _, tc := range cases {
		bb.Run(tc.name, func(bb *testing.B) {
			var latencies []time.Duration
			var mu sync.Mutex
			flushed := make(chan struct{}, 1)

			b := tc.setup(func(batch []*int) {
				now := time.Now()
				for _, p := range batch {
					mu.Lock()
					latencies = append(latencies, now.Sub(time.Unix(0, int64(*p))))
					mu.Unlock()
				}
				select {
				case flushed <- struct{}{}:
				default:
				}
			})
			defer b.Close()

			bb.ResetTimer()
			for i := 0; i < bb.N; i++ {
				time.Sleep(idleBetweenPuts)
				now := time.Now().UnixNano()
				v := int(now)
				b.Put(&v)
				<-flushed
			}
			bb.StopTimer()

			mu.Lock()
			defer mu.Unlock()
			if len(latencies) == 0 {
				bb.Fatal("no latencies recorded")
			}
			var total time.Duration
			for _, l := range latencies {
				total += l
			}
			meanNs := float64(total.Nanoseconds()) / float64(len(latencies))
			bb.ReportMetric(meanNs, "ns/put-to-flush")
			bb.ReportMetric(meanNs/float64(interval.Nanoseconds())*100, "%-of-interval")
		})
	}
}
