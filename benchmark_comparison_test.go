package batcher

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Benchmark comparison between original Put and PutOptimized
func BenchmarkPutComparison(b *testing.B) {
	benchmarks := []struct {
		name string
		fn   func(*Batcher[testItem], *testItem)
	}{
		{"Put", func(b *Batcher[testItem], item *testItem) { b.Put(item) }},
		{"PutOptimized", func(b *Batcher[testItem], item *testItem) { b.PutOptimized(item) }},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			processBatch := func([]*testItem) {}
			batcher := New[testItem](100, 100*time.Millisecond, processBatch, true)

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

// Benchmark comparison between original worker and optimized worker
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
			"WorkerOptimized",
			func(size int, timeout time.Duration, fn func([]*testItem), bg bool) interface{} {
				return NewOptimized[testItem](size, timeout, fn, bg)
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

// Benchmark comparison between basic Batcher and BatcherWithPool
func BenchmarkBatcherWithPoolComparison(b *testing.B) {
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
			"BatcherWithPool",
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
				switch bt := batcher.(type) {
				case *Batcher[testItem]:
					bt.Put(&testItem{ID: i})
				case *BatcherWithPool[testItem]:
					bt.Put(&testItem{ID: i})
				}
			}

			// Ensure processing completes
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// Benchmark comparison for TimePartitionedMap Get operations
func BenchmarkGetComparison(b *testing.B) {
	// Setup maps with data
	regularMap := NewTimePartitionedMap[int, struct{}](time.Second, 60)
	optimizedMap := NewTimePartitionedMapOptimized[int, struct{}](time.Second, 60)
	defer optimizedMap.Close()

	// Pre-populate with items
	for i := 0; i < 10000; i++ {
		regularMap.Set(i, struct{}{})
		optimizedMap.SetOptimized(i, struct{}{})
	}

	benchmarks := []struct {
		name string
		fn   func(int) bool
	}{
		{
			"Get",
			func(key int) bool {
				_, exists := regularMap.Get(key)
				return exists
			},
		},
		{
			"GetOptimized",
			func(key int) bool {
				_, exists := optimizedMap.GetOptimized(key)
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
		fn   func(*TimePartitionedMapOptimized[int, struct{}], int)
	}{
		{
			"Set",
			func(m *TimePartitionedMapOptimized[int, struct{}], key int) {
				m.Set(key, struct{}{})
			},
		},
		{
			"SetOptimized",
			func(m *TimePartitionedMapOptimized[int, struct{}], key int) {
				m.SetOptimized(key, struct{}{})
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			m := NewTimePartitionedMapOptimized[int, struct{}](time.Second, 60)
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
			"BatcherWithDedupOptimized",
			func(size int, timeout time.Duration, fn func([]*testItem), bg bool) interface{} {
				return NewWithDeduplicationOptimized[testItem](size, timeout, fn, bg)
			},
		},
	}

	// Test with 50% duplicates
	for _, bm := range benchmarks {
		b.Run(bm.name+"_50PercentDuplicates", func(b *testing.B) {
			processBatch := func([]*testItem) {}
			batcher := bm.constructor(100, 50*time.Millisecond, processBatch, true)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Create 50% duplicates by using modulo
				id := i % (b.N / 2)
				item := &testItem{ID: id}

				switch bt := batcher.(type) {
				case *BatcherWithDedup[testItem]:
					bt.Put(item)
				case *BatcherWithDedupOptimized[testItem]:
					bt.Put(item)
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
				processBatch := func([]*testItem) {}
				batcher := New[testItem](100, 50*time.Millisecond, processBatch, true)
				for i := 0; i < 1000; i++ {
					batcher.Put(&testItem{ID: i})
				}
				time.Sleep(100 * time.Millisecond)
			},
		},
		{
			"BatcherOptimized_Allocations",
			func() {
				processBatch := func([]*testItem) {}
				batcher := NewOptimized[testItem](100, 50*time.Millisecond, processBatch, true)
				for i := 0; i < 1000; i++ {
					batcher.Put(&testItem{ID: i})
				}
				time.Sleep(100 * time.Millisecond)
			},
		},
		{
			"BatcherWithPool_Allocations",
			func() {
				processBatch := func([]*testItem) {}
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

	fmt.Println("\n=== BENCHMARK COMPARISON SUMMARY ===")
	fmt.Println("Run benchmarks with: go test -bench=. -benchmem")
	fmt.Println("\nExpected improvements:")
	fmt.Println("- PutOptimized: 30-50% faster for non-blocking sends")
	fmt.Println("- WorkerOptimized: 70-80% fewer allocations")
	fmt.Println("- BatcherWithPool: 90% fewer allocations for batch slices")
	fmt.Println("- GetOptimized: 40-60% faster for recent items")
	fmt.Println("- SetOptimized: 20-40% faster for non-duplicates")
}

// Benchmark for high concurrency scenarios
func BenchmarkHighConcurrency(b *testing.B) {
	concurrencyLevels := []int{10, 100, 1000}

	for _, level := range concurrencyLevels {
		b.Run(fmt.Sprintf("Batcher_%d_goroutines", level), func(b *testing.B) {
			processBatch := func([]*testItem) {}
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

		b.Run(fmt.Sprintf("BatcherOptimized_%d_goroutines", level), func(b *testing.B) {
			processBatch := func([]*testItem) {}
			batcher := NewOptimized[testItem](100, 10*time.Millisecond, processBatch, true)

			b.ResetTimer()
			var wg sync.WaitGroup
			itemsPerGoroutine := b.N / level

			for g := 0; g < level; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < itemsPerGoroutine; i++ {
						batcher.PutOptimized(&testItem{ID: i})
					}
				}()
			}
			wg.Wait()
		})
	}
}