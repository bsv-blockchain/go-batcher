package batcher

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
	"time"

	txmap "github.com/bsv-blockchain/go-tx-map"
)

// BloomFilter is a simple bloom filter implementation for fast negative lookups.
// This implementation uses a bit array and multiple hash functions to provide
// probabilistic set membership testing with no false negatives.
type BloomFilter struct {
	bits      []uint64
	size      uint64
	hashFuncs uint
	itemCount atomic.Uint64
	mu        sync.RWMutex
}

// NewBloomFilter creates a new bloom filter with the specified size and hash functions.
func NewBloomFilter(size uint64, hashFuncs uint) *BloomFilter {
	// Ensure size is a multiple of 64 for uint64 alignment
	alignedSize := (size + 63) / 64
	return &BloomFilter{
		bits:      make([]uint64, alignedSize),
		size:      alignedSize * 64,
		hashFuncs: hashFuncs,
	}
}

// Add adds a key to the bloom filter.
func (bf *BloomFilter) Add(key interface{}) {
	hashes := bf.hash(key)
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for _, h := range hashes {
		wordIndex := h / 64
		bitIndex := h % 64
		bf.bits[wordIndex] |= 1 << bitIndex
	}
	bf.itemCount.Add(1)
}

// Test checks if a key might be in the bloom filter.
// Returns false if definitely not present, true if maybe present.
func (bf *BloomFilter) Test(key interface{}) bool {
	hashes := bf.hash(key)
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	for _, h := range hashes {
		wordIndex := h / 64
		bitIndex := h % 64
		if bf.bits[wordIndex]&(1<<bitIndex) == 0 {
			return false
		}
	}
	return true
}

// Reset clears the bloom filter.
func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.itemCount.Store(0)
}

// hash generates multiple hash values for the given key.
func (bf *BloomFilter) hash(key interface{}) []uint64 { //nolint:gocyclo // Type switch for performance
	h := fnv.New64a()
	// Convert key to bytes for hashing - optimized paths for common types
	switch k := key.(type) {
	case string:
		_, _ = h.Write([]byte(k))
	case int:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(k)) //nolint:gosec // Safe conversion for hashing
		_, _ = h.Write(buf[:])
	case int8:
		_, _ = h.Write([]byte{byte(k)})
	case int16:
		var buf [2]byte
		binary.BigEndian.PutUint16(buf[:], uint16(k)) //nolint:gosec // Safe conversion for hashing
		_, _ = h.Write(buf[:])
	case int32:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(k)) //nolint:gosec // Safe conversion for hashing
		_, _ = h.Write(buf[:])
	case int64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(k)) //nolint:gosec // Safe conversion for hashing
		_, _ = h.Write(buf[:])
	case uint:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(k))
		_, _ = h.Write(buf[:])
	case uint8:
		_, _ = h.Write([]byte{k})
	case uint16:
		var buf [2]byte
		binary.BigEndian.PutUint16(buf[:], k)
		_, _ = h.Write(buf[:])
	case uint32:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], k)
		_, _ = h.Write(buf[:])
	case uint64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], k)
		_, _ = h.Write(buf[:])
	case float32:
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], math.Float32bits(k))
		_, _ = h.Write(buf[:])
	case float64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(k))
		_, _ = h.Write(buf[:])
	case bool:
		if k {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
	default:
		// For other types (structs, arrays, etc), use fmt.Fprintf for generic conversion
		_, _ = fmt.Fprintf(h, "%v", key)
	}
	hash1 := h.Sum64()
	// Generate additional hashes using double hashing
	hashes := make([]uint64, bf.hashFuncs)
	hash2 := hash1 >> 32
	for i := uint(0); i < bf.hashFuncs; i++ {
		hashes[i] = (hash1 + uint64(i)*hash2) % bf.size
	}
	return hashes
}

// TimePartitionedMapOptimized extends TimePartitionedMap with optimized operations.
type TimePartitionedMapOptimized[K comparable, V any] struct {
	*TimePartitionedMap[K, V]

	bloomFilter      *BloomFilter
	bloomResetTicker *time.Ticker
}

// NewTimePartitionedMapOptimized creates an optimized time-partitioned map with bloom filter.
func NewTimePartitionedMapOptimized[K comparable, V any](bucketSize time.Duration, maxBuckets int) *TimePartitionedMapOptimized[K, V] {
	base := NewTimePartitionedMap[K, V](bucketSize, maxBuckets)
	// Create bloom filter sized for expected items
	// Assuming average of 10k items per bucket with 0.1% false positive rate
	bloomSize := uint64(maxBuckets) * 10000 * 10 //nolint:gosec // Controlled input with reasonable bounds
	m := &TimePartitionedMapOptimized[K, V]{
		TimePartitionedMap: base,
		bloomFilter:        NewBloomFilter(bloomSize, 3), // 3 hash functions
	}
	// Reset bloom filter periodically to handle expired items
	m.bloomResetTicker = time.NewTicker(bucketSize * time.Duration(maxBuckets))
	go func() {
		for range m.bloomResetTicker.C {
			m.bloomFilter.Reset()
			// Re-add all current items to bloom filter
			m.rebuildBloomFilter()
		}
	}()
	return m
}

// rebuildBloomFilter reconstructs the bloom filter from current items.
func (m *TimePartitionedMapOptimized[K, V]) rebuildBloomFilter() {
	m.bucketsMu.Lock()
	defer m.bucketsMu.Unlock()
	for _, bucket := range m.buckets.Range() {
		for key := range bucket.Range() {
			m.bloomFilter.Add(key)
		}
	}
}

// GetOptimized searches for a key starting from the newest bucket.
// This is more efficient for recently added items which are the most common case.
func (m *TimePartitionedMapOptimized[K, V]) GetOptimized(key K) (V, bool) {
	// First check bloom filter for fast negative
	if !m.bloomFilter.Test(key) {
		return m.zero, false
	}
	// Search from newest to oldest bucket
	newestID := m.newestBucket.Load()
	oldestID := m.oldestBucket.Load()
	// If no buckets exist
	if newestID == 0 || oldestID == 0 {
		return m.zero, false
	}
	// Search backwards from newest bucket
	for bucketID := newestID; bucketID >= oldestID; bucketID-- {
		if bucket, exists := m.buckets.Get(bucketID); exists {
			if value, found := bucket.Get(key); found {
				return value, true
			}
		}
	}
	return m.zero, false
}

// SetOptimized adds a key-value pair with bloom filter optimization.
// The bloom filter provides fast negative lookups for non-duplicate items.
func (m *TimePartitionedMapOptimized[K, V]) SetOptimized(key K, value V) bool {
	// Check bloom filter first (fast path for non-duplicates)
	if !m.bloomFilter.Test(key) {
		// Definitely not a duplicate, proceed with insertion
		m.bloomFilter.Add(key)
	} else {
		// Bloom filter says maybe - do full check
		if _, exists := m.GetOptimized(key); exists {
			return false
		}
		// Not actually a duplicate, add to bloom filter
		m.bloomFilter.Add(key)
	}
	// Proceed with insertion (same as original Set logic)
	var (
		bucket *txmap.SyncedMap[K, V]
		exists bool
	)
	bucketID := m.currentBucketID.Load()
	m.bucketsMu.Lock()
	if bucket, exists = m.buckets.Get(bucketID); !exists {
		bucket = txmap.NewSyncedMap[K, V]()
		m.buckets.Set(bucketID, bucket)
		if m.newestBucket.Load() < bucketID {
			m.newestBucket.Store(bucketID)
		}
		if m.oldestBucket.Load() == 0 || m.oldestBucket.Load() > bucketID {
			m.oldestBucket.Store(bucketID)
		}
	}
	bucket.Set(key, value)
	m.itemCount.Add(1)
	m.bucketsMu.Unlock()
	return true
}

// Close stops the bloom filter reset ticker.
func (m *TimePartitionedMapOptimized[K, V]) Close() {
	if m.bloomResetTicker != nil {
		m.bloomResetTicker.Stop()
	}
}

// WithDedupOptimized extends BatcherWithDedup with optimized deduplication.
type WithDedupOptimized[T comparable] struct {
	Batcher[T]

	deduplicationWindow time.Duration
	deduplicationMap    *TimePartitionedMapOptimized[T, struct{}]
}

// NewWithDeduplicationOptimized creates a new Batcher with optimized deduplication.
func NewWithDeduplicationOptimized[T comparable](size int, timeout time.Duration, fn func(batch []*T), background bool) *WithDedupOptimized[T] {
	deduplicationWindow := time.Minute // 1-minute deduplication window
	b := &WithDedupOptimized[T]{
		Batcher: Batcher[T]{
			fn:         fn,
			size:       size,
			timeout:    timeout,
			batch:      make([]*T, 0, size),
			ch:         make(chan *T, size*64),
			triggerCh:  make(chan struct{}),
			background: background,
		},
		deduplicationWindow: deduplicationWindow,
		deduplicationMap:    NewTimePartitionedMapOptimized[T, struct{}](time.Second, int(deduplicationWindow.Seconds())+1),
	}
	go b.workerOptimized()
	return b
}

// Put adds an item to the batch with optimized deduplication.
func (b *WithDedupOptimized[T]) Put(item *T) {
	if item == nil {
		return
	}
	// SetOptimized returns TRUE if the item was added, FALSE if it was a duplicate
	if b.deduplicationMap.SetOptimized(*item, struct{}{}) {
		// Add the item to the batch
		b.ch <- item
	}
}
