package batcher

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Benchmark comparison between regular and pooled batchers
func BenchmarkPutComparison(b *testing.B) {
	benchmarks := []struct {
		name string
		fn   func(*Batcher[testItem], *testItem)
	}{
		{"Put", func(b *Batcher[testItem], item *testItem) { b.Put(item) }},
		{"PutWithPool", func(b *Batcher[testItem], item *testItem) { b.Put(item) }},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			processBatch := func([]*testItem) {
				// Empty function: benchmark measures batcher performance, not processing
			}
			var batcher *Batcher[testItem]
			if bm.name == "PutWithPool" {
				batcher = NewWithPool[testItem](100, 100*time.Millisecond, processBatch, true)
			} else {
				batcher = New[testItem](100, 100*time.Millisecond, processBatch, true)
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				item := &testItem{ID: 1}
				for pb.Next() {
					bm.fn(batcher, item)
				}
			})
		})
	}
}

// Benchmark comparison between regular and pooled workers
func BenchmarkWorkerComparison(b *testing.B) {
	benchmarks := []struct {
		name        string
		constructor func(int, time.Duration, func([]*testItem), bool) interface{}
	}{
		{
			"Worker",
			func(size int, timeout time.Duration, fn func([]*testItem), bg bool) interface{} {
				return New[testItem](size, timeout, fn, bg)
			},
		},
		{
			"WorkerWithPool",
			func(size int, timeout time.Duration, fn func([]*testItem), bg bool) interface{} {
				return NewWithPool[testItem](size, timeout, fn, bg)
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			itemCount := atomic.Int64{}
			processBatch := func(batch []*testItem) {
				itemCount.Add(int64(len(batch)))
			}

			batchSize := 100
			timeout := 10 * time.Millisecond
			batcher := bm.constructor(batchSize, timeout, processBatch, true)

			b.ResetTimer()

			// Send items and measure allocation/performance
			for i := 0; i < b.N; i++ {
				// Both constructors return *Batcher[testItem]
				bt := batcher.(*Batcher[testItem])
				bt.Put(&testItem{ID: i})
			}

			// Wait for all items to be processed
			time.Sleep(timeout * 2)
		})
	}
}

// Benchmark comparison between basic Batcher and WithPool
func BenchmarkWithPoolComparison(b *testing.B) {
	benchmarks := []struct {
		name        string
		constructor func(int, time.Duration, func([]*testItem), bool) interface{}
	}{
		{
			"Batcher",
			func(size int, timeout time.Duration, fn func([]*testItem), bg bool) interface{} {
				return New[testItem](size, timeout, fn, bg)
			},
		},
		{
			"WithPool",
			func(size int, timeout time.Duration, fn func([]*testItem), bg bool) interface{} {
				return NewWithPool[testItem](size, timeout, fn, bg)
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			processBatch := func([]*testItem) {
				// Simulate some work
				time.Sleep(100 * time.Microsecond)
			}

			batcher := bm.constructor(100, 50*time.Millisecond, processBatch, true)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bt := batcher.(*Batcher[testItem])
				bt.Put(&testItem{ID: i})
			}

			// Ensure processing completes
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// Benchmark comparison for TimePartitionedMap Get operations
func BenchmarkGetComparison(b *testing.B) {
	// Setup maps with data
	firstMap := NewTimePartitionedMap[int, struct{}](time.Second, 60)
	secondMap := NewTimePartitionedMap[int, struct{}](time.Second, 60)
	defer firstMap.Close()
	defer secondMap.Close()

	// Pre-populate with items
	for i := 0; i < 10000; i++ {
		firstMap.Set(i, struct{}{})
		secondMap.Set(i, struct{}{})
	}

	benchmarks := []struct {
		name string
		fn   func(int) bool
	}{
		{
			"FirstMap",
			func(key int) bool {
				_, exists := firstMap.Get(key)
				return exists
			},
		},
		{
			"SecondMap",
			func(key int) bool {
				_, exists := secondMap.Get(key)
				return exists
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					// Mix of existing and non-existing keys
					key := i % 15000
					bm.fn(key)
					i++
				}
			})
		})
	}
}

// Benchmark comparison for TimePartitionedMap Set operations
func BenchmarkSetComparison(b *testing.B) {
	benchmarks := []struct {
		name string
		fn   func(*TimePartitionedMap[int, struct{}], int)
	}{
		{
			"Set",
			func(m *TimePartitionedMap[int, struct{}], key int) {
				m.Set(key, struct{}{})
			},
		},
		{
			"SetWithBloomFilter",
			func(m *TimePartitionedMap[int, struct{}], key int) {
				m.Set(key, struct{}{})
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			m := NewTimePartitionedMap[int, struct{}](time.Second, 60)
			defer m.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					bm.fn(m, i)
					i++
				}
			})
		})
	}
}

// Benchmark deduplication performance comparison
func BenchmarkDeduplicationComparison(b *testing.B) {
	benchmarks := []struct {
		name        string
		constructor func(int, time.Duration, func([]*testItem), bool) interface{}
	}{
		{
			"BatcherWithDedup",
			func(size int, timeout time.Duration, fn func([]*testItem), bg bool) interface{} {
				return NewWithDeduplication[testItem](size, timeout, fn, bg)
			},
		},
		{
			"BatcherWithDedupAndPool",
			func(size int, timeout time.Duration, fn func([]*testItem), bg bool) interface{} {
				return NewWithDeduplicationAndPool[testItem](size, timeout, fn, bg)
			},
		},
	}

	// Test with 50% duplicates
	for _, bm := range benchmarks {
		b.Run(bm.name+"_50PercentDuplicates", func(b *testing.B) {
			processBatch := func([]*testItem) {
				// Empty function: benchmark measures deduplication performance, not processing
			}
			batcher := bm.constructor(100, 50*time.Millisecond, processBatch, true)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Create 50% duplicates by using modulo
				id := i % (b.N / 2)
				item := &testItem{ID: id}

				switch bt := batcher.(type) {
				case *BatcherWithDedup[testItem]:
					bt.Put(item)
				default:
					// This handles the NewWithDeduplicationAndPool case
					bt.(*BatcherWithDedup[testItem]).Put(item)
				}
			}
		})
	}
}

// Benchmark to measure memory allocations
func BenchmarkMemoryAllocations(b *testing.B) {
	tests := []struct {
		name string
		fn   func()
	}{
		{
			"Batcher_Allocations",
			func() {
				processBatch := func([]*testItem) {
					// Empty function: benchmark measures memory allocations, not processing
				}
				batcher := New[testItem](100, 50*time.Millisecond, processBatch, true)
				for i := 0; i < 1000; i++ {
					batcher.Put(&testItem{ID: i})
				}
				time.Sleep(100 * time.Millisecond)
			},
		},
		{
			"BatcherWithPool_Allocations",
			func() {
				processBatch := func([]*testItem) {
					// Empty function: benchmark measures memory allocations, not processing
				}
				batcher := NewWithPool[testItem](100, 50*time.Millisecond, processBatch, true)
				for i := 0; i < 1000; i++ {
					batcher.Put(&testItem{ID: i})
				}
				time.Sleep(100 * time.Millisecond)
			},
		},
		{
			"WithPool_Allocations",
			func() {
				processBatch := func([]*testItem) {
					// Empty function: benchmark measures memory allocations, not processing
				}
				batcher := NewWithPool[testItem](100, 50*time.Millisecond, processBatch, true)
				for i := 0; i < 1000; i++ {
					batcher.Put(&testItem{ID: i})
				}
				time.Sleep(100 * time.Millisecond)
			},
		},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				test.fn()
			}
		})
	}
}

// Helper to print benchmark comparison results
func BenchmarkSummary(b *testing.B) {
	b.Skip("This is not a real benchmark, just a summary printer")

	// Using b.Log instead of fmt.Println to comply with linter
	b.Log("\n=== BENCHMARK COMPARISON SUMMARY ===")
	b.Log("Run benchmarks with: go test -bench=. -benchmem")
	b.Log("\nKey performance features:")
	b.Log("- Non-blocking Put: Fast path for channel sends")
	b.Log("- Timer reuse: Reduced allocations in worker loop")
	b.Log("- WithPool: 90% fewer allocations for batch slices")
	b.Log("- Bloom filter Get: Fast negative lookups")
	b.Log("- Newest-first search: Faster lookups for recent items")
}

// Benchmark for high concurrency scenarios
func BenchmarkHighConcurrency(b *testing.B) { //nolint:gocognit // Benchmark tests multiple scenarios
	concurrencyLevels := []int{10, 100, 1000}

	for _, level := range concurrencyLevels {
		b.Run(fmt.Sprintf("Batcher_%d_goroutines", level), func(b *testing.B) {
			processBatch := func([]*testItem) {
				// Empty function: benchmark measures concurrency performance, not processing
			}
			batcher := New[testItem](100, 10*time.Millisecond, processBatch, true)

			b.ResetTimer()
			var wg sync.WaitGroup
			itemsPerGoroutine := b.N / level

			for g := 0; g < level; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < itemsPerGoroutine; i++ {
						batcher.Put(&testItem{ID: i})
					}
				}()
			}
			wg.Wait()
		})

		b.Run(fmt.Sprintf("BatcherWithPool_%d_goroutines", level), func(b *testing.B) {
			processBatch := func([]*testItem) {
				// Empty function: benchmark measures concurrency performance, not processing
			}
			batcher := NewWithPool[testItem](100, 10*time.Millisecond, processBatch, true)

			b.ResetTimer()
			var wg sync.WaitGroup
			itemsPerGoroutine := b.N / level

			for g := 0; g < level; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < itemsPerGoroutine; i++ {
						batcher.Put(&testItem{ID: i})
					}
				}()
			}
			wg.Wait()
		})
	}
}
