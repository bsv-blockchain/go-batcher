package batcher

import (
	"testing"
	"time"
)

// BenchmarkPut_NonGreedy and BenchmarkPut_Greedy measure per-item CPU cost
// under sustained load (channel kept saturated by the benchmark loop, which
// approximates the bursty-arrival case greedy accumulate targets: every item
// after the first in a burst costs a non-blocking 2-arm receive instead of a
// 5-arm select).
func BenchmarkPut_NonGreedy(b *testing.B) {
	benchmarkPutThroughput(b, false)
}

func BenchmarkPut_Greedy(b *testing.B) {
	benchmarkPutThroughput(b, true)
}

func benchmarkPutThroughput(b *testing.B, greedy bool) {
	const batchSize = 256

	var opts []Option
	if greedy {
		opts = append(opts, WithGreedyAccumulate(true))
	}

	bt := New[int](batchSize, time.Hour, func(_ []*int) {}, true, opts...)
	defer bt.Close()

	v := 0

	for b.Loop() {
		bt.Put(&v)
	}
}
