//nolint:wsl // Benchmark files have different formatting standards
package batcher

import (
	"sync"
	"testing"
	"time"
)

// BenchmarkBatcherPut measures the performance of Put operation for basic Batcher.
func BenchmarkBatcherPut(b *testing.B) {
	processedBatches := 0
	batchFn := func(_ []*batchStoreItem) {
		processedBatches++
	}

	batcher := New[batchStoreItem](100, 10*time.Second, batchFn, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Put(&batchStoreItem{})
	}

	// Trigger to process any remaining items
	batcher.Trigger()
}

// BenchmarkBatcherPutParallel measures the performance of Put operation under a concurrent load.
func BenchmarkBatcherPutParallel(b *testing.B) {
	processedBatches := 0
	var mu sync.Mutex
	batchFn := func(_ []*batchStoreItem) {
		mu.Lock()
		processedBatches++
		mu.Unlock()
	}

	batcher := New[batchStoreItem](100, 10*time.Second, batchFn, true)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			batcher.Put(&batchStoreItem{})
		}
	})

	// Trigger to process any remaining items
	batcher.Trigger()
}

// BenchmarkBatcherTrigger measures the performance of manual Trigger operation.
func BenchmarkBatcherTrigger(b *testing.B) {
	processedBatches := 0
	batchFn := func(_ []*batchStoreItem) {
		processedBatches++
	}

	batcher := New[batchStoreItem](1000, 10*time.Second, batchFn, false)

	// Pre-fill with some items
	for i := 0; i < 50; i++ {
		batcher.Put(&batchStoreItem{})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Trigger()
	}
}

// BenchmarkBatcherWithBackground compares foreground vs. background processing.
func BenchmarkBatcherWithBackground(b *testing.B) {
	b.Run("Foreground", func(b *testing.B) {
		processedItems := 0
		batchFn := func(batch []*batchStoreItem) {
			processedItems += len(batch)
		}

		batcher := New[batchStoreItem](100, time.Second, batchFn, false)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batcher.Put(&batchStoreItem{})
		}
		batcher.Trigger()
	})

	b.Run("Background", func(b *testing.B) {
		processedItems := 0
		var mu sync.Mutex
		batchFn := func(batch []*batchStoreItem) {
			mu.Lock()
			processedItems += len(batch)
			mu.Unlock()
		}

		batcher := New[batchStoreItem](100, time.Second, batchFn, true)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batcher.Put(&batchStoreItem{})
		}
		batcher.Trigger()
		time.Sleep(10 * time.Millisecond) // Allow background processing
	})
}

// BenchmarkTimePartitionedMapSet measures Set operation performance.
func BenchmarkTimePartitionedMapSet(b *testing.B) {
	m := NewTimePartitionedMap[int, struct{}](time.Second, 60)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Set(i, struct{}{})
	}
}

// BenchmarkTimePartitionedMapGet measures Get operation performance.
func BenchmarkTimePartitionedMapGet(b *testing.B) {
	m := NewTimePartitionedMap[int, struct{}](time.Second, 60)

	// Pre-populate the map
	for i := 0; i < 10000; i++ {
		m.Set(i, struct{}{})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Get(i % 10000)
	}
}

// BenchmarkTimePartitionedMapDelete measures Delete operation performance.
func BenchmarkTimePartitionedMapDelete(b *testing.B) {
	m := NewTimePartitionedMap[int, struct{}](time.Second, 60)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m.Set(i, struct{}{})
		b.StartTimer()
		m.Delete(i)
	}
}

// BenchmarkTimePartitionedMapCount measures Count operation performance.
func BenchmarkTimePartitionedMapCount(b *testing.B) {
	m := NewTimePartitionedMap[int, struct{}](time.Second, 60)

	// Pre-populate with varying amounts
	for i := 0; i < 5000; i++ {
		m.Set(i, struct{}{})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Count()
	}
}

// BenchmarkTimePartitionedMapConcurrent measures concurrent access performance.
func BenchmarkTimePartitionedMapConcurrent(b *testing.B) {
	m := NewTimePartitionedMap[int, struct{}](time.Second, 60)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 4 {
			case 0:
				m.Set(i, struct{}{})
			case 1:
				_, _ = m.Get(i)
			case 2:
				m.Delete(i)
			case 3:
				_ = m.Count()
			}
			i++
		}
	})
}

// BenchmarkBatcherWithDedupPut measures Put operation with deduplication.
func BenchmarkBatcherWithDedupPut(b *testing.B) {
	processedBatches := 0
	batchFn := func(_ []*testItem) {
		processedBatches++
	}

	batcher := NewWithDeduplication[testItem](100, time.Second, batchFn, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Put(&testItem{ID: i})
	}

	// Trigger to process any remaining items
	batcher.Trigger()
}

// BenchmarkBatcherWithDedupDuplicates measures deduplication overhead with duplicates.
func BenchmarkBatcherWithDedupDuplicates(b *testing.B) {
	processedItems := 0
	batchFn := func(batch []*testItem) {
		processedItems += len(batch)
	}

	batcher := NewWithDeduplication[testItem](100, time.Second, batchFn, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create duplicates by using modulo - 90% duplicates
		divisor := b.N / 10
		if divisor == 0 {
			divisor = 1
		}
		batcher.Put(&testItem{ID: i % divisor})
	}

	batcher.Trigger()
}

// BenchmarkSmallBatches measures performance with small batch sizes.
func BenchmarkSmallBatches(b *testing.B) {
	processedBatches := 0
	batchFn := func(_ []*batchStoreItem) {
		processedBatches++
	}

	// Small batch size of 10
	batcher := New[batchStoreItem](10, time.Second, batchFn, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Put(&batchStoreItem{})
	}

	batcher.Trigger()
}

// BenchmarkLargeBatches measures performance with large batch sizes.
func BenchmarkLargeBatches(b *testing.B) {
	processedBatches := 0
	batchFn := func(_ []*batchStoreItem) {
		processedBatches++
	}

	// Large batch size of 10,000
	batcher := New[batchStoreItem](10000, time.Second, batchFn, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batcher.Put(&batchStoreItem{})
	}

	batcher.Trigger()
}

// BenchmarkTimeoutVsSize compares timeout-triggered vs size-triggered batches.
func BenchmarkTimeoutVsSize(b *testing.B) {
	b.Run("SizeTriggered", func(b *testing.B) {
		processedBatches := 0
		batchFn := func(_ []*batchStoreItem) {
			processedBatches++
		}

		// Small batch size to trigger size-based batching
		batcher := New[batchStoreItem](10, 10*time.Second, batchFn, false)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			batcher.Put(&batchStoreItem{})
		}
	})

	b.Run("TimeoutTriggered", func(b *testing.B) {
		processedBatches := 0
		batchFn := func(_ []*batchStoreItem) {
			processedBatches++
		}

		// Large batch size with short timeout
		batcher := New[batchStoreItem](100000, 10*time.Millisecond, batchFn, true)

		b.ResetTimer()
		start := time.Now()
		for i := 0; i < 100; i++ {
			batcher.Put(&batchStoreItem{})
		}

		// Wait for timeout to trigger
		time.Sleep(20 * time.Millisecond)
		b.StopTimer()

		elapsed := time.Since(start)
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(100), "ns/item")
	})
}

// BenchmarkMemoryUsage measures memory allocation patterns.
func BenchmarkMemoryUsage(b *testing.B) {
	b.Run("BasicBatcher", func(b *testing.B) {
		batchFn := func(_ []*batchStoreItem) {}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batcher := New[batchStoreItem](100, time.Second, batchFn, false)
			batcher.Put(&batchStoreItem{})
			batcher.Trigger()
		}
	})

	b.Run("BatcherWithDedup", func(b *testing.B) {
		batchFn := func(_ []*testItem) {}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batcher := NewWithDeduplication[testItem](100, time.Second, batchFn, false)
			batcher.Put(&testItem{ID: i})
			batcher.Trigger()
		}
	})
}
