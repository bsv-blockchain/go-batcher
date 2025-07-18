# Performance Optimization Plan for go-batcher

## Overview

This document outlines a comprehensive plan to optimize the go-batcher library's performance while maintaining 100% backward compatibility. All existing code remains untouched - we only add new optimized versions for comparison.

## Core Principle: Preserve ALL Existing Code

- **No modifications** to existing files or functions
- **Only add** new optimized versions alongside the originals
- **Create benchmarks** to compare old vs new implementations
- **Maintain** full API compatibility

## Optimization Targets and Strategies

### 1. Optimized Put Method

**Current Implementation**: `(*Batcher[T]) Put(item *T, _ ...int)` in `batcher.go`
- Simple blocking channel send: `b.ch <- item`

**Optimized Version**: `(*Batcher[T]) PutOptimized(item *T, _ ...int)` in `batcher_optimized.go`
- **Strategy**: Non-blocking channel send with select/default pattern
- **Expected Benefit**: Reduced blocking when channel has capacity
- **Implementation**:
  ```go
  select {
  case b.ch <- item:
      // Fast path - non-blocking
  default:
      // Fallback to blocking send
      b.ch <- item
  }
  ```

### 2. Optimized Worker Loop

**Current Implementation**: `(*Batcher[T]) worker()` in `batcher.go`
- Creates new timer for each batch cycle: `expire := time.After(b.timeout)`

**Optimized Version**: `(*Batcher[T]) workerOptimized()` in `batcher_optimized.go`
- **New Constructor**: `NewOptimized[T]()` that uses `workerOptimized`
- **Strategy**: Reuse timer with `Reset()` instead of creating new timers
- **Expected Benefit**: Fewer allocations, reduced GC pressure
- **Implementation**:
  ```go
  timer := time.NewTimer(b.timeout)
  defer timer.Stop()
  // ... in loop:
  timer.Reset(b.timeout)
  ```

### 3. Optimized TimePartitionedMap Get

**Current Implementation**: `(*TimePartitionedMap[K,V]) Get(key K)` in `batcher_deduplication.go`
- Linear search through all buckets with map iteration (random order)

**Optimized Version**: `(*TimePartitionedMap[K,V]) GetOptimized(key K)` in `dedup_optimized.go`
- **Strategy**: Search from newest to oldest bucket (most likely to find recent items)
- **Expected Benefit**: Faster lookups for recently added items (common case)
- **Implementation**:
  ```go
  // Start from newest bucket and work backwards
  for bucketID := m.newestBucket.Load(); bucketID >= m.oldestBucket.Load(); bucketID-- {
      if bucket, exists := m.buckets.Get(bucketID); exists {
          if value, found := bucket.Get(key); found {
              return value, true
          }
      }
  }
  ```

### 4. Optimized TimePartitionedMap Set with Bloom Filter

**Current Implementation**: `(*TimePartitionedMap[K,V]) Set(key K, value V)` in `batcher_deduplication.go`
- Global deduplication check calls Get() which searches all buckets

**Optimized Version**: `(*TimePartitionedMap[K,V]) SetOptimized(key K, value V)` in `dedup_optimized.go`
- **Strategy**: Use bloom filter for fast negative checks before full search
- **Expected Benefit**: Faster duplicate detection for non-duplicate items
- **Implementation**:
  ```go
  // Check bloom filter first (fast path for non-duplicates)
  if !m.bloomFilter.Test(key) {
      m.bloomFilter.Add(key)
      // Proceed with insertion
  } else {
      // Bloom filter says maybe - do full check
      if _, exists := m.GetOptimized(key); exists {
          return false
      }
  }
  ```

### 5. Batch Slice Pooling

**New Type**: `BatcherWithPool[T]` in `batcher_optimized.go`
- **New Constructor**: `NewWithPool[T]()`
- **Strategy**: Use `sync.Pool` for batch slice reuse
- **Expected Benefit**: Reduced memory allocations
- **Implementation**:
  ```go
  var pool = sync.Pool{
      New: func() interface{} {
          slice := make([]*T, 0, b.size)
          return &slice
      },
  }
  ```

## New Files Structure

### 1. `batcher_optimized.go`
Contains:
- `PutOptimized()` method
- `workerOptimized()` method
- `NewOptimized[T]()` constructor
- `BatcherWithPool[T]` type
- `NewWithPool[T]()` constructor

### 2. `dedup_optimized.go`
Contains:
- `GetOptimized()` method
- `SetOptimized()` method
- Bloom filter implementation
- `NewTimePartitionedMapOptimized()` constructor

### 3. `batcher_optimized_test.go`
Contains:
- Unit tests for all optimized functions
- Fuzz tests for edge cases
- Parallel tests for concurrency safety
- Property-based tests for behavioral equivalence

### 4. `benchmark_comparison_test.go`
Contains side-by-side benchmarks:
- `BenchmarkPut` vs `BenchmarkPutOptimized`
- `BenchmarkWorker` vs `BenchmarkWorkerOptimized`
- `BenchmarkGet` vs `BenchmarkGetOptimized`
- `BenchmarkSet` vs `BenchmarkSetOptimized`
- `BenchmarkBatcherWithPool` vs `BenchmarkBatcher`

## Benchmark Methodology

### Metrics to Measure
- **ns/op**: Nanoseconds per operation
- **allocs/op**: Allocations per operation
- **B/op**: Bytes allocated per operation
- **Throughput**: Operations per second

### Benchmark Scenarios
1. **Light Load**: Small batches, infrequent items
2. **Heavy Load**: Large batches, continuous stream
3. **Bursty Load**: Alternating high/low traffic
4. **High Concurrency**: Multiple goroutines sending items
5. **Deduplication Heavy**: 50%+ duplicate items

### Expected Results Format
```
BenchmarkPut-8                    10000000   145 ns/op    0 B/op   0 allocs/op
BenchmarkPutOptimized-8           20000000    72 ns/op    0 B/op   0 allocs/op
Improvement: 50.3% faster

BenchmarkWorker-8                  1000000  1050 ns/op   48 B/op   1 allocs/op  
BenchmarkWorkerOptimized-8         5000000   210 ns/op    0 B/op   0 allocs/op
Improvement: 80.0% faster, 100% fewer allocations
```

## Testing Strategy

### Correctness Tests
1. **Equivalence Testing**: Ensure optimized versions produce identical results
2. **Edge Cases**: Empty batches, single items, maximum sizes
3. **Concurrency**: Race detector enabled tests
4. **Stress Tests**: Extended runs with millions of items

### Performance Validation
1. **Regression Tests**: Ensure no performance degradation in any scenario
2. **Memory Profiling**: Verify reduced allocations
3. **CPU Profiling**: Identify any new hotspots
4. **Latency Distribution**: P50, P95, P99 percentiles

## Implementation Timeline

1. **Phase 1**: Create optimized batcher methods (Put, worker)
2. **Phase 2**: Create optimized deduplication methods (Get, Set)
3. **Phase 3**: Implement bloom filter and pooling
4. **Phase 4**: Write comprehensive tests
5. **Phase 5**: Run benchmarks and analyze results
6. **Phase 6**: Document findings and recommendations

## Success Criteria

- All optimized functions pass correctness tests
- At least 25% performance improvement in target scenarios
- No regression in any benchmark
- Memory allocations reduced by at least 50% where applicable
- Documentation clearly shows performance gains

## Next Steps

1. Review and approve this plan
2. Begin implementation of optimized functions
3. Create comprehensive test suite
4. Run benchmarks and analyze results
5. Make recommendations for potential library updates